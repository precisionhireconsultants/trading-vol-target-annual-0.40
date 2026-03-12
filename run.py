#!/usr/bin/env python3
"""
QQQ MA250 Trading Strategy - CLI Entrypoint

Usage:
    python run.py --qqq path/to/QQQ.csv
    python run.py --qqq data/qqq_us_d.csv --out output/for_graphs/consolidated.csv
    python run.py --backtest-mode intraday --out output/intraday.csv
    python run.py --help
"""
import argparse
import sys
import numpy as np
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from data_loader import load_qqq_csv, normalize_data, validate_data_integrity
from indicators import add_ma250, add_ma50, add_annualized_volatility
from regime import add_base_regime, add_confirmed_regime, add_final_trading_regime, add_target_weight
from fold_selection import select_sample_fold, add_fold_metadata_columns
from portfolio import (
    init_portfolio, add_exec_target_weight, compute_target_shares,
    execute_trade, compute_trade_fields, compute_holdings,
    compute_eod_valuation, compute_actual_weight, PortfolioState,
    should_rebalance
)
from export import build_final_schema, export_to_csv, DEFAULT_OUTPUT_PATH
from engine import clamp_weight_for_leverage
from config import DEFAULT_CONFIG
from metrics import compute_baseline_metrics


# ---------------------------------------------------------------------------
# Intraday pipeline helper
# ---------------------------------------------------------------------------

def _load_intraday_pipeline(
    alpaca_dir: str,
    signal_offset: int,
    exec_offset: int,
    load_tqqq: bool = True,
    load_sqqq: bool = False,
):
    """Load Alpaca 1-min parquet, resample, and aggregate to daily rows.

    Returns (qqq_daily, tqqq_daily_or_None, sqqq_daily_or_None).
    Each daily df has: Date, Open, High, Low, Close, Volume,
                       Signal_Price, Exec_Price, can_trade.
    """
    from intraday_loader import (
        load_intraday_parquet,
        resample_to_full_grid,
        aggregate_intraday_to_daily,
    )

    def _load_one(symbol):
        raw = load_intraday_parquet(alpaca_dir, symbol)
        gridded = resample_to_full_grid(raw)
        daily = aggregate_intraday_to_daily(gridded, signal_offset, exec_offset)
        daily = daily[daily["symbol"] == symbol].drop(columns=["symbol"]).copy()
        daily = daily.sort_values("Date").reset_index(drop=True)
        return daily

    qqq_daily = _load_one("QQQ")
    print(f"Loaded {len(qqq_daily)} daily rows from QQQ 1-min parquet")

    tqqq_daily = None
    if load_tqqq:
        try:
            tqqq_daily = _load_one("TQQQ")
            print(f"Loaded {len(tqqq_daily)} daily rows from TQQQ 1-min parquet")
        except FileNotFoundError:
            tqqq_daily = None

    sqqq_daily = None
    if load_sqqq:
        try:
            sqqq_daily = _load_one("SQQQ")
            print(f"Loaded {len(sqqq_daily)} daily rows from SQQQ 1-min parquet")
        except FileNotFoundError:
            sqqq_daily = None

    return qqq_daily, tqqq_daily, sqqq_daily


def run_backtest(
    qqq_path: str,
    output_path: str,
    initial_capital: float,
    fold_start: str = None,
    fold_years: int = 5,
    use_ma_confirmation: bool = False,
    execution_mode: str = "NEXT_OPEN",
    use_vol_targeting: bool = False,
    vol_target: float = None,
    min_weight_change: float = 0.0,
    tqqq_path: str = None,
    sqqq_path: str = None,
    debug_columns: bool = False,
    backtest_mode: str = "daily",
    signal_offset_bars: int = None,
    exec_offset_bars: int = None,
) -> dict:
    """
    Run the full backtest pipeline.
    
    Args:
        qqq_path: Path to QQQ CSV file (used in daily mode)
        output_path: Output CSV path
        initial_capital: Starting capital
        fold_start: Optional fold start date (YYYY-MM-DD)
        fold_years: Number of years for fold (default 5)
        use_ma_confirmation: Enable MA50 confirmation (require MA50 > MA250 for bull)
        execution_mode: "NEXT_OPEN" or "SAME_DAY_CLOSE" (daily mode only)
        use_vol_targeting: Enable volatility targeting for position sizing
        vol_target: Target annual vol for vol targeting (None = use config VOL_TARGET_ANNUAL)
        min_weight_change: Minimum weight change to trigger rebalance (0 = disabled)
        tqqq_path: Optional path to TQQQ CSV for leveraged long execution (daily mode)
        sqqq_path: Optional path to SQQQ CSV for leveraged short execution (daily mode)
        debug_columns: Include debug columns in output
        backtest_mode: "daily" or "intraday"
        signal_offset_bars: Intraday signal offset (None = use config)
        exec_offset_bars: Intraday exec offset (None = use config)
        
    Returns:
        Summary dict with results
    """
    from datetime import datetime

    is_intraday = (backtest_mode == "intraday")

    if signal_offset_bars is None:
        signal_offset_bars = DEFAULT_CONFIG.SIGNAL_OFFSET_BARS
    if exec_offset_bars is None:
        exec_offset_bars = DEFAULT_CONFIG.EXEC_OFFSET_BARS

    # ------------------------------------------------------------------
    # Load data — branch on backtest mode
    # ------------------------------------------------------------------
    exec_symbol = "QQQ"
    tqqq_df = None
    sqqq_df = None
    use_sqqq = False

    if is_intraday:
        alpaca_dir = DEFAULT_CONFIG.ALPACA_INTRADAY_DIR
        print(f"Loading intraday data from: {alpaca_dir}")
        print(f"  Signal offset: {signal_offset_bars}  Exec offset: {exec_offset_bars}")

        df, tqqq_df, sqqq_df = _load_intraday_pipeline(
            alpaca_dir,
            signal_offset_bars,
            exec_offset_bars,
            load_tqqq=True,
            load_sqqq=(sqqq_path is not None),
        )

        if tqqq_df is not None:
            exec_symbol = "TQQQ"
        if sqqq_df is not None:
            use_sqqq = True

        execution_mode = "INTRADAY"
    else:
        print(f"Loading data from: {qqq_path}")

        df = load_qqq_csv(qqq_path)
        df = normalize_data(df)
        df = validate_data_integrity(df)
        print(f"Loaded {len(df)} rows of QQQ data")

        if tqqq_path:
            tqqq_df = load_qqq_csv(tqqq_path)
            tqqq_df = normalize_data(tqqq_df)
            tqqq_df = validate_data_integrity(tqqq_df)
            print(f"Loaded {len(tqqq_df)} rows of TQQQ data")
            exec_symbol = "TQQQ"

        if sqqq_path:
            sqqq_df = load_qqq_csv(sqqq_path, price_scale=100_000)
            sqqq_df = normalize_data(sqqq_df)
            sqqq_df = validate_data_integrity(sqqq_df)
            print(f"Loaded {len(sqqq_df)} rows of SQQQ data")
            use_sqqq = True

    # ------------------------------------------------------------------
    # Indicators (no-lookahead: shift(1) for intraday OR daily NEXT_OPEN)
    # ------------------------------------------------------------------
    use_shifted = is_intraday or (not is_intraday and execution_mode == "NEXT_OPEN")
    df = add_ma250(df, intraday_mode=use_shifted)
    df = add_ma50(df, intraday_mode=use_shifted)
    df = add_annualized_volatility(df, intraday_mode=use_shifted)

    # ------------------------------------------------------------------
    # Regime signals
    # ------------------------------------------------------------------
    if is_intraday:
        price_col = 'Signal_Price'
    elif execution_mode == "NEXT_OPEN":
        df['Prev_Close'] = df['Close'].shift(1)
        price_col = 'Prev_Close'
    else:
        price_col = 'Close'
    df = add_base_regime(df, price_col=price_col)
    df = add_confirmed_regime(df, use_ma_confirmation=use_ma_confirmation)
    df = add_final_trading_regime(df, use_sqqq_in_bear=use_sqqq)
    df = add_target_weight(df, use_vol_targeting=use_vol_targeting, vol_target=vol_target)
    
    min_weight_change = float(min_weight_change) if min_weight_change is not None else DEFAULT_CONFIG.MIN_WEIGHT_CHANGE
    if use_ma_confirmation:
        print("  MA50 confirmation: ENABLED")
    if use_vol_targeting:
        print("  Volatility targeting: ENABLED")
    if use_sqqq:
        print("  SQQQ in bear: ENABLED")
    
    # When using TQQQ/SQQQ, default fold start to their first available date (they started 2010-02-11)
    if fold_start is None and (tqqq_df is not None or sqqq_df is not None):
        exec_first_dates = []
        if tqqq_df is not None:
            exec_first_dates.append(tqqq_df["Date"].min())
        if sqqq_df is not None:
            exec_first_dates.append(sqqq_df["Date"].min())
        latest_first = max(exec_first_dates)
        fold_start = str(latest_first)[:10]  # YYYY-MM-DD
        print(f"  Fold start defaulted to TQQQ/SQQQ data start: {fold_start}")
    
    # Parse fold start date if provided
    start_date = None
    if fold_start:
        start_date = datetime.strptime(fold_start, "%Y-%m-%d")
    
    # Select sample fold
    fold_df, metadata = select_sample_fold(df, years=fold_years, start_date=start_date)
    fold_df = add_fold_metadata_columns(fold_df, metadata)
    print(f"Selected fold with {len(fold_df)} rows")
    print(f"  Date range: {metadata['Test_Start'].date()} to {metadata['Test_End'].date()}")
    
    # Add execution timing with optional throttling.
    # When daily NEXT_OPEN, indicators already use Close.shift(1) so the
    # signal on row D reflects only data through D-1.  Pass SAME_DAY_CLOSE
    # to avoid a second shift of Target_Weight (which would cause 2-day lag).
    weight_exec_mode = execution_mode
    if not is_intraday and execution_mode == "NEXT_OPEN":
        weight_exec_mode = "SAME_DAY_CLOSE"
    fold_df = add_exec_target_weight(
        fold_df,
        execution_mode=weight_exec_mode,
        min_weight_change=min_weight_change
    )
    print(f"  Execution mode: {execution_mode}")
    if min_weight_change > 0:
        print(f"  Trade throttling: {min_weight_change:.1%} minimum change")
    
    # ------------------------------------------------------------------
    # Execution instrument columns
    # ------------------------------------------------------------------
    if is_intraday:
        # In intraday mode, Exec_Open / Exec_Close are set from the intraday
        # aggregation columns already present on df (and thus fold_df).
        # For TQQQ/SQQQ we merge their daily Exec_Price by Date.
        if tqqq_df is not None:
            tqqq_prices = tqqq_df[['Date', 'Open', 'Close', 'Exec_Price']].copy()
            tqqq_prices = tqqq_prices.rename(columns={
                'Open': 'TQQQ_Open', 'Close': 'TQQQ_Close',
                'Exec_Price': 'TQQQ_Exec_Price',
            })
            fold_df = fold_df.merge(tqqq_prices, on='Date', how='left')

        if sqqq_df is not None:
            sqqq_prices = sqqq_df[['Date', 'Open', 'Close', 'Exec_Price']].copy()
            sqqq_prices = sqqq_prices.rename(columns={
                'Open': 'SQQQ_Open', 'Close': 'SQQQ_Close',
                'Exec_Price': 'SQQQ_Exec_Price',
            })
            fold_df = fold_df.merge(sqqq_prices, on='Date', how='left')

        def _intraday_exec_symbol(row):
            if row['Exec_Target_Weight'] > 0:
                return 'TQQQ' if tqqq_df is not None else 'QQQ'
            elif row['Exec_Target_Weight'] < 0 and use_sqqq:
                return 'SQQQ'
            return 'CASH'

        fold_df['Exec_Symbol'] = fold_df.apply(_intraday_exec_symbol, axis=1)
        fold_df['Exec_Open'] = fold_df['Open']
        fold_df['Exec_Close'] = fold_df['Close']

        if tqqq_df is not None:
            mask_tqqq = fold_df['Exec_Symbol'] == 'TQQQ'
            fold_df.loc[mask_tqqq, 'Exec_Open'] = fold_df.loc[mask_tqqq, 'TQQQ_Open']
            fold_df.loc[mask_tqqq, 'Exec_Close'] = fold_df.loc[mask_tqqq, 'TQQQ_Close']
        if sqqq_df is not None:
            mask_sqqq = fold_df['Exec_Symbol'] == 'SQQQ'
            fold_df.loc[mask_sqqq, 'Exec_Open'] = fold_df.loc[mask_sqqq, 'SQQQ_Open']
            fold_df.loc[mask_sqqq, 'Exec_Close'] = fold_df.loc[mask_sqqq, 'SQQQ_Close']

        if tqqq_df is not None:
            bull_exec = "TQQQ"
        else:
            bull_exec = "QQQ"
        if use_sqqq:
            print(f"  Execution: {bull_exec} (bull) / SQQQ (bear)")
        else:
            print(f"  Execution symbol: {bull_exec}")

    elif use_sqqq and sqqq_df is not None:
        # Merge both TQQQ and SQQQ prices
        if tqqq_df is not None:
            tqqq_prices = tqqq_df[['Date', 'Open', 'Close']].copy()
            tqqq_prices = tqqq_prices.rename(columns={'Open': 'TQQQ_Open', 'Close': 'TQQQ_Close'})
            fold_df = fold_df.merge(tqqq_prices, on='Date', how='left')
        
        sqqq_prices = sqqq_df[['Date', 'Open', 'Close']].copy()
        sqqq_prices = sqqq_prices.rename(columns={'Open': 'SQQQ_Open', 'Close': 'SQQQ_Close'})
        fold_df = fold_df.merge(sqqq_prices, on='Date', how='left')
        
        def get_exec_symbol(row):
            if row['Exec_Target_Weight'] > 0:
                return 'TQQQ' if tqqq_df is not None else 'QQQ'
            elif row['Exec_Target_Weight'] < 0:
                return 'SQQQ'
            else:
                return 'CASH'
        
        def get_exec_open(row):
            if row['Exec_Target_Weight'] > 0:
                return row.get('TQQQ_Open', row['Open']) if tqqq_df is not None else row['Open']
            elif row['Exec_Target_Weight'] < 0:
                return row.get('SQQQ_Open', row['Open'])
            else:
                return row['Open']
        
        def get_exec_close(row):
            if row['Exec_Target_Weight'] > 0:
                return row.get('TQQQ_Close', row['Close']) if tqqq_df is not None else row['Close']
            elif row['Exec_Target_Weight'] < 0:
                return row.get('SQQQ_Close', row['Close'])
            else:
                return row['Close']
        
        fold_df['Exec_Symbol'] = fold_df.apply(get_exec_symbol, axis=1)
        fold_df['Exec_Open'] = fold_df.apply(get_exec_open, axis=1)
        fold_df['Exec_Close'] = fold_df.apply(get_exec_close, axis=1)
        bull_exec = "TQQQ" if tqqq_df is not None else "QQQ"
        print(f"  Execution: {bull_exec} (bull) / SQQQ (bear)")
    elif tqqq_df is not None:
        tqqq_prices = tqqq_df[['Date', 'Open', 'Close']].copy()
        tqqq_prices = tqqq_prices.rename(columns={'Open': 'Exec_Open', 'Close': 'Exec_Close'})
        fold_df = fold_df.merge(tqqq_prices, on='Date', how='left')
        fold_df['Exec_Symbol'] = 'TQQQ'
        print(f"  Execution symbol: TQQQ")
    else:
        fold_df['Exec_Open'] = fold_df['Open']
        fold_df['Exec_Close'] = fold_df['Close']
        fold_df['Exec_Symbol'] = 'QQQ'
        print(f"  Execution symbol: QQQ")
    
    # ------------------------------------------------------------------
    # Portfolio simulation
    # ------------------------------------------------------------------
    state = init_portfolio(initial_capital)
    result_data = []
    prev_exec_weight = 0.0
    trade_count = 0
    
    use_close_for_fill = (execution_mode == "SAME_DAY_CLOSE")
    
    max_exposure = DEFAULT_CONFIG.MAX_EFFECTIVE_EXPOSURE
    rebalance_band = DEFAULT_CONFIG.REBALANCE_BAND_PCT
    
    prev_actual_weight = 0.0
    held_symbol = "CASH"

    def resolve_symbol_price(row, symbol: str, use_close_price: bool) -> float:
        """Resolve per-symbol open/close price for the current row."""
        if symbol == "CASH":
            return np.nan

        base_col = "Close" if use_close_price else "Open"
        exec_col = "Exec_Close" if use_close_price else "Exec_Open"

        if row['Exec_Symbol'] == symbol:
            return float(row[exec_col])

        if symbol == "QQQ":
            return float(row[base_col])

        symbol_col = f"{symbol}_{base_col}"
        if symbol_col in row.index:
            val = row[symbol_col]
            if not np.isnan(val):
                return float(val)

        raise ValueError(f"Missing {symbol} {base_col} price for date {row['Date']}")

    def get_rebalance_reason(prev_weight: float, curr_weight: float, trade_made: bool) -> str:
        if not trade_made:
            return "NO_TRADE"
        prev_in_market = prev_weight > 0.0
        curr_in_market = curr_weight > 0.0
        if prev_in_market != curr_in_market:
            return "REGIME_SWITCH"
        return "REBALANCE"
    
    # Simulate each day
    for idx, row in fold_df.iterrows():

        # ---------------------------------------------------------------
        # Intraday can_trade guard: skip execution on short/bad days
        # ---------------------------------------------------------------
        if is_intraday and not row.get('can_trade', True):
            cash_open = state.cash
            shares_open = state.shares

            if state.shares > 0 and held_symbol != "CASH":
                held_close_price = resolve_symbol_price(row, held_symbol, use_close_price=True)
                eod_value = compute_eod_valuation(state.shares, state.cash, held_close_price)
                actual_weight = compute_actual_weight(state.shares, held_close_price, eod_value)
            else:
                eod_value = state.cash
                actual_weight = 0.0

            row_data = {
                'Portfolio_Value_Open': eod_value,
                'Target_Shares': state.shares,
                'Trade_Flag': 0,
                'Trade_Made_Type': '',
                'Trade_Count': 0,
                'Net_Shares_Change': 0,
                'Total_Notional_Abs': 0.0,
                'Fill_Price_VWAP': np.nan,
                'Rebalance_Reason_Code': 'NO_TRADE',
                'Total_Stocks_Owned': state.shares,
                'Cash': state.cash,
                'Remaining_Portfolio_Amount': eod_value,
                'Actual_Weight': actual_weight,
            }
            if debug_columns:
                row_data.update({
                    'Cash_Open': cash_open,
                    'Shares_Open': shares_open,
                    'Decision_Price': np.nan,
                    'Fill_Price_Source': np.nan,
                    'Fill_Price_Effective': np.nan,
                    'Weight_Raw': np.nan,
                    'Weight_Clamped': np.nan,
                })
            result_data.append(row_data)
            prev_actual_weight = actual_weight
            continue

        # ---------------------------------------------------------------
        # Normal execution logic (daily + intraday when can_trade=True)
        # ---------------------------------------------------------------
        target_symbol = row['Exec_Symbol']
        exec_open = row['Exec_Open']
        exec_close = row['Exec_Close']
        exec_exposure = row['Exec_Target_Weight']
        leverage = DEFAULT_CONFIG.TQQQ_LEVERAGE if target_symbol in ('TQQQ', 'SQQQ') else 1.0
        exec_weight_raw = exec_exposure / leverage
        clamped_weight, clamp_reason = clamp_weight_for_leverage(
            exec_weight_raw, leverage, max_exposure
        )
        exec_weight = clamped_weight
        
        cash_open = state.cash
        shares_open = state.shares

        # ---- fill / decision price selection ---
        if is_intraday:
            # Intraday: fill at Exec_Price, decision at Signal_Price
            if target_symbol == 'TQQQ' and 'TQQQ_Exec_Price' in row.index:
                fill_price = float(row['TQQQ_Exec_Price'])
            elif target_symbol == 'SQQQ' and 'SQQQ_Exec_Price' in row.index:
                fill_price = float(row['SQQQ_Exec_Price'])
            else:
                fill_price = float(row['Exec_Price'])
            decision_price = float(row['Signal_Price'])
        elif use_close_for_fill:
            fill_price = exec_close
            decision_price = fill_price
        else:
            fill_price = exec_open
            decision_price = fill_price

        # Mark current holdings
        if state.shares > 0 and held_symbol != "CASH":
            held_fill_price = resolve_symbol_price(row, held_symbol, use_close_for_fill) if not is_intraday else resolve_symbol_price(row, held_symbol, use_close_price=True)
            portfolio_value_at_exec = state.cash + abs(state.shares) * held_fill_price
        else:
            held_fill_price = np.nan
            portfolio_value_at_exec = state.cash
        
        symbol_switch = (state.shares > 0 and held_symbol != "CASH" and held_symbol != target_symbol)
        rebalance_needed = should_rebalance(exec_weight, prev_actual_weight, rebalance_band) or symbol_switch

        if rebalance_needed:
            target_shares = compute_target_shares(portfolio_value_at_exec, abs(exec_weight), fill_price)
        else:
            target_shares = state.shares

        if rebalance_needed and symbol_switch:
            liquidate_result = execute_trade(state, target_shares=0, open_price=held_fill_price)
            flat_state = PortfolioState(cash=liquidate_result.new_cash, shares=0)
            entry_result = execute_trade(flat_state, target_shares, fill_price)
            trade_result = entry_result
            state = PortfolioState(cash=entry_result.new_cash, shares=entry_result.new_shares)

            trade_made = (liquidate_result.shares_diff != 0) or (entry_result.shares_diff != 0)
            trade_flag = 1 if trade_made else 0
            trade_count_row = 1 if trade_made else 0
            trade_type = "BUY" if target_shares > 0 else "SELL"
            net_shares_change = target_shares - shares_open
            total_notional_abs = liquidate_result.notional + entry_result.notional
            fill_price_vwap = fill_price if trade_made else np.nan
            rebalance_reason = get_rebalance_reason(prev_exec_weight, exec_weight, trade_made)
        else:
            trade_result = execute_trade(state, target_shares, fill_price)
            state = PortfolioState(cash=trade_result.new_cash, shares=trade_result.new_shares)
            trade_fields = compute_trade_fields(trade_result, prev_exec_weight, exec_weight)

            trade_flag = trade_fields.Trade_Flag
            trade_count_row = trade_fields.Trade_Count
            trade_type = trade_fields.Trade_Made_Type
            net_shares_change = trade_fields.Net_Shares_Change
            total_notional_abs = trade_fields.Total_Notional_Abs
            fill_price_vwap = trade_fields.Fill_Price_VWAP
            rebalance_reason = trade_fields.Rebalance_Reason_Code

        if trade_flag:
            trade_count += 1

        if state.shares <= 0:
            held_symbol = "CASH"
        elif trade_flag:
            held_symbol = target_symbol
        
        holdings = compute_holdings(trade_result)

        if state.shares > 0 and held_symbol != "CASH":
            held_close_price = resolve_symbol_price(row, held_symbol, use_close_price=True)
            eod_value = compute_eod_valuation(state.shares, state.cash, held_close_price)
            actual_weight = compute_actual_weight(state.shares, held_close_price, eod_value)
        else:
            eod_value = state.cash
            actual_weight = 0.0
        
        row_data = {
            'Portfolio_Value_Open': portfolio_value_at_exec,
            'Target_Shares': target_shares,
            'Trade_Flag': trade_flag,
            'Trade_Made_Type': trade_type,
            'Trade_Count': trade_count_row,
            'Net_Shares_Change': net_shares_change,
            'Total_Notional_Abs': total_notional_abs,
            'Fill_Price_VWAP': fill_price_vwap,
            'Rebalance_Reason_Code': rebalance_reason,
            'Total_Stocks_Owned': holdings.Total_Stocks_Owned,
            'Cash': holdings.Cash,
            'Remaining_Portfolio_Amount': eod_value,
            'Actual_Weight': actual_weight
        }
        
        if debug_columns:
            row_data.update({
                'Cash_Open': cash_open,
                'Shares_Open': shares_open,
                'Decision_Price': decision_price,
                'Fill_Price_Source': fill_price,
                'Fill_Price_Effective': fill_price_vwap if trade_flag else np.nan,
                'Weight_Raw': exec_exposure,
                'Weight_Clamped': clamped_weight
            })
        
        result_data.append(row_data)
        prev_exec_weight = exec_weight
        prev_actual_weight = actual_weight
    
    # Add computed columns to DataFrame
    for i, data in enumerate(result_data):
        for key, value in data.items():
            fold_df.loc[fold_df.index[i], key] = value
    
    final_df = build_final_schema(fold_df, include_debug=debug_columns)
    output_file = export_to_csv(final_df, output_path)
    
    if debug_columns:
        print("  Debug columns: ENABLED")
    
    final_value = result_data[-1]['Remaining_Portfolio_Amount']
    total_return = (final_value - initial_capital) / initial_capital * 100
    
    days_simulated = len(fold_df)
    years = days_simulated / 252.0 if days_simulated else 0.0
    cagr_pct = ((final_value / initial_capital) ** (1.0 / years) - 1.0) * 100 if years > 0 and initial_capital > 0 else 0.0
    baseline = compute_baseline_metrics(final_df)
    
    signal_symbol = "QQQ"
    if use_sqqq:
        execution_symbol = "TQQQ (bull) / SQQQ (bear)" if tqqq_df is not None else "QQQ (bull) / SQQQ (bear)"
    elif tqqq_df is not None:
        execution_symbol = "TQQQ"
    else:
        execution_symbol = "QQQ"
    
    summary = {
        'initial_capital': initial_capital,
        'final_value': final_value,
        'total_return_pct': total_return,
        'trade_count': trade_count,
        'days_simulated': len(fold_df),
        'output_file': str(output_file),
        'signal_symbol': signal_symbol,
        'execution_symbol': execution_symbol,
        'cagr_pct': cagr_pct,
        'max_drawdown_pct': baseline['max_drawdown_pct'],
        'worst_20d_return': baseline['worst_20d_return'],
        'exposure_pct': baseline['exposure_pct'],
    }
    
    return summary


def main():
    """Main CLI entrypoint."""
    parser = argparse.ArgumentParser(
        description='QQQ MA250 Trading Strategy Backtest',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    python run.py --qqq data/qqq_us_d.csv
    python run.py --qqq data/qqq_us_d.csv --out results.csv
    python run.py --qqq data/qqq_us_d.csv --initial-capital 50000
    python run.py --backtest-mode intraday --out output/intraday.csv
        """
    )
    
    parser.add_argument(
        '--qqq',
        type=str,
        default=None,
        help='Path to QQQ CSV file (required for daily mode)'
    )

    parser.add_argument(
        '--backtest-mode',
        type=str,
        choices=['daily', 'intraday'],
        default=DEFAULT_CONFIG.BACKTEST_MODE,
        help=f'Pipeline mode (default: {DEFAULT_CONFIG.BACKTEST_MODE})'
    )

    parser.add_argument(
        '--signal-offset-bars',
        type=int,
        default=DEFAULT_CONFIG.SIGNAL_OFFSET_BARS,
        help=f'Intraday signal offset from last bar (default: {DEFAULT_CONFIG.SIGNAL_OFFSET_BARS})'
    )

    parser.add_argument(
        '--exec-offset-bars',
        type=int,
        default=DEFAULT_CONFIG.EXEC_OFFSET_BARS,
        help=f'Intraday exec offset from last bar (default: {DEFAULT_CONFIG.EXEC_OFFSET_BARS})'
    )
    
    parser.add_argument(
        '--out',
        type=str,
        default=DEFAULT_OUTPUT_PATH,
        help=f'Output CSV path (default: {DEFAULT_OUTPUT_PATH})'
    )
    
    parser.add_argument(
        '--initial-capital',
        type=float,
        default=DEFAULT_CONFIG.INITIAL_CAPITAL,
        help=f'Initial capital (default: {DEFAULT_CONFIG.INITIAL_CAPITAL:,.0f})'
    )
    
    parser.add_argument(
        '--fold-start',
        type=str,
        default=None,
        help='Fold start date YYYY-MM-DD (default: first valid MA250 date)'
    )
    
    parser.add_argument(
        '--fold-years',
        type=int,
        default=DEFAULT_CONFIG.FOLD_YEARS,
        help=f'Number of years for fold (default: {DEFAULT_CONFIG.FOLD_YEARS})'
    )
    
    parser.add_argument(
        '--ma-confirmation',
        action='store_true',
        default=DEFAULT_CONFIG.USE_MA_CONFIRMATION,
        help='Enable MA50 confirmation (require MA50 > MA250 for bull)'
    )
    
    parser.add_argument(
        '--execution-mode',
        type=str,
        choices=['NEXT_OPEN', 'SAME_DAY_CLOSE'],
        default=DEFAULT_CONFIG.EXECUTION_MODE,
        help=f'Execution timing for daily mode (default: {DEFAULT_CONFIG.EXECUTION_MODE})'
    )
    
    parser.add_argument(
        '--vol-targeting',
        action='store_true',
        default=DEFAULT_CONFIG.USE_VOL_TARGETING,
        help='Enable volatility targeting for position sizing'
    )
    
    parser.add_argument(
        '--min-weight-change',
        type=float,
        default=DEFAULT_CONFIG.MIN_WEIGHT_CHANGE,
        help=f'Minimum weight change to trigger rebalance (default: {DEFAULT_CONFIG.MIN_WEIGHT_CHANGE})'
    )
    
    parser.add_argument(
        '--tqqq',
        type=str,
        default=None,
        help='Path to TQQQ CSV file for leveraged long execution (optional, daily mode)'
    )
    
    parser.add_argument(
        '--sqqq',
        type=str,
        default=None,
        help='Path to SQQQ CSV file for leveraged short execution (optional)'
    )
    
    parser.add_argument(
        '--debug-columns',
        action='store_true',
        default=DEFAULT_CONFIG.DEBUG_COLUMNS,
        help='Include debug columns (Cash_Open, Shares_Open, Decision_Price, etc.)'
    )
    
    args = parser.parse_args()

    # In daily mode, when --qqq not provided, use DATA_ROOT and daily file names from config
    if args.backtest_mode == "daily" and not args.qqq:
        root = Path(DEFAULT_CONFIG.DATA_ROOT)
        args.qqq = str(root / DEFAULT_CONFIG.DAILY_QQQ_FILE)
        tqqq_p = root / DEFAULT_CONFIG.DAILY_TQQQ_FILE
        # sqqq_p = root / DEFAULT_CONFIG.DAILY_SQQQ_FILE
        args.tqqq = str(tqqq_p) if tqqq_p.exists() else None
        # args.sqqq = str(sqqq_p) if sqqq_p.exists() else None
        args.sqqq = None  # SQQQ loading disabled (uncomment above to re-enable)

    # Run backtest
    print("=" * 60)
    print("QQQ MA250 Trading Strategy Backtest")
    print(f"  Mode: {args.backtest_mode}")
    print("=" * 60)
    
    summary = run_backtest(
        qqq_path=args.qqq or "",
        output_path=args.out,
        initial_capital=args.initial_capital,
        fold_start=args.fold_start,
        fold_years=args.fold_years,
        use_ma_confirmation=args.ma_confirmation,
        execution_mode=args.execution_mode,
        use_vol_targeting=args.vol_targeting,
        vol_target=None,
        min_weight_change=args.min_weight_change,
        tqqq_path=args.tqqq,
        sqqq_path=args.sqqq,
        debug_columns=args.debug_columns,
        backtest_mode=args.backtest_mode,
        signal_offset_bars=args.signal_offset_bars,
        exec_offset_bars=args.exec_offset_bars,
    )
    
    # Print results
    print("\n" + "=" * 60)
    print("RESULTS")
    print("=" * 60)
    print(f"Signal (regime):    {summary['signal_symbol']}")
    print(f"Execution (trading): {summary['execution_symbol']}")
    print(f"Initial Capital:   ${summary['initial_capital']:,.2f}")
    print(f"Final Value:        ${summary['final_value']:,.2f}")
    print(f"Total Return:       {summary['total_return_pct']:+.2f}%")
    print(f"CAGR:               {summary['cagr_pct']:+.2f}%")
    print(f"Max Drawdown:       {summary['max_drawdown_pct']:.2%}")
    print(f"Worst 20-day:       {summary['worst_20d_return']:.2%}")
    print(f"% Time in Market:   {summary['exposure_pct']:.1%}")
    print(f"Trade Count:        {summary['trade_count']}")
    print(f"Days Simulated:     {summary['days_simulated']}")
    print(f"Output File:        {summary['output_file']}")
    print("=" * 60)


if __name__ == '__main__':
    main()
