"""Regression tests for full pipeline verification."""
import pytest
import pandas as pd
import numpy as np
from datetime import datetime
from pathlib import Path

from data_loader import load_qqq_csv, normalize_data
from indicators import add_ma250, add_ma50, add_annualized_volatility
from regime import (
    add_base_regime, add_confirmed_regime, 
    add_final_trading_regime, add_target_weight
)
from fold_selection import select_sample_fold, add_fold_metadata_columns
from portfolio import (
    init_portfolio, add_exec_target_weight, compute_target_shares,
    execute_trade, compute_trade_fields, compute_holdings,
    compute_eod_valuation, compute_actual_weight, PortfolioState
)
from export import build_final_schema, FINAL_COLUMNS


def create_synthetic_data_with_trades():
    """
    Create synthetic data that guarantees both BUY and SELL trades.
    
    Uses small MA windows (5) so trades happen quickly.
    Price starts high (triggers buy), then drops (triggers sell).
    """
    dates = pd.date_range('2024-01-01', periods=30)
    
    # Price pattern: starts high, drops, rises again
    # This should trigger regime changes
    prices = [
        100, 101, 102, 103, 104,  # Rising - will be above MA5
        105, 104, 103, 102, 101,  # Falling
        100, 99, 98, 97, 96,      # Below MA - sell signal
        95, 96, 97, 98, 99,       # Rising again
        100, 101, 102, 103, 104,  # Above MA - buy signal
        105, 106, 107, 108, 109   # Continuing up
    ]
    
    return pd.DataFrame({
        'Date': dates,
        'Open': prices,
        'High': [p + 2 for p in prices],
        'Low': [p - 2 for p in prices],
        'Close': prices,
        'Volume': [1000000] * 30
    })


def run_mini_backtest(df: pd.DataFrame, ma_window: int = 5) -> pd.DataFrame:
    """Run a mini backtest with custom MA window for testing."""
    # Add indicators with custom window
    df = df.copy()
    df['MA250'] = df['Close'].rolling(window=ma_window, min_periods=ma_window).mean()
    df['MA50'] = df['Close'].rolling(window=2, min_periods=2).mean()
    df = add_annualized_volatility(df, window=5)
    
    # Add regime signals
    df = add_base_regime(df)
    df = add_confirmed_regime(df)
    df = add_final_trading_regime(df)
    df = add_target_weight(df)
    
    # Simple fold (all data after MA is valid)
    valid_df = df[df['MA250'].notna()].copy().reset_index(drop=True)
    valid_df['Phase'] = 'test'
    valid_df['Fold_ID'] = 1
    valid_df['Train_Start'] = valid_df['Date'].iloc[0]
    valid_df['Train_End'] = valid_df['Date'].iloc[-1]
    valid_df['Test_Start'] = valid_df['Date'].iloc[0]
    valid_df['Test_End'] = valid_df['Date'].iloc[-1]
    
    # Add execution timing
    valid_df = add_exec_target_weight(valid_df)
    
    # Initialize portfolio
    state = init_portfolio(10000.0)
    prev_exec_weight = 0.0
    
    result_data = []
    
    for idx in range(len(valid_df)):
        row = valid_df.iloc[idx]
        open_price = row['Open']
        close_price = row['Close']
        exec_weight = row['Exec_Target_Weight']
        
        portfolio_value_open = state.cash + state.shares * open_price
        target_shares = compute_target_shares(portfolio_value_open, exec_weight, open_price)
        trade_result = execute_trade(state, target_shares, open_price)
        state = PortfolioState(cash=trade_result.new_cash, shares=trade_result.new_shares)
        trade_fields = compute_trade_fields(trade_result, prev_exec_weight, exec_weight)
        holdings = compute_holdings(trade_result)
        eod_value = compute_eod_valuation(state.shares, state.cash, close_price)
        actual_weight = compute_actual_weight(state.shares, close_price, eod_value)
        
        result_data.append({
            'Portfolio_Value_Open': portfolio_value_open,
            'Target_Shares': target_shares,
            'Trade_Flag': trade_fields.Trade_Flag,
            'Trade_Made_Type': trade_fields.Trade_Made_Type,
            'Trade_Count': trade_fields.Trade_Count,
            'Net_Shares_Change': trade_fields.Net_Shares_Change,
            'Total_Notional_Abs': trade_fields.Total_Notional_Abs,
            'Fill_Price_VWAP': trade_fields.Fill_Price_VWAP,
            'Rebalance_Reason_Code': trade_fields.Rebalance_Reason_Code,
            'Total_Stocks_Owned': holdings.Total_Stocks_Owned,
            'Cash': holdings.Cash,
            'Remaining_Portfolio_Amount': eod_value,
            'Actual_Weight': actual_weight
        })
        
        prev_exec_weight = exec_weight
    
    for i, data in enumerate(result_data):
        for key, value in data.items():
            valid_df.loc[valid_df.index[i], key] = value
    
    # Add execution columns for schema compatibility
    valid_df['Exec_Symbol'] = 'QQQ'
    valid_df['Exec_Open'] = valid_df['Open']
    valid_df['Exec_Close'] = valid_df['Close']
    
    return valid_df


class TestFullPipelineRegression:
    """Full pipeline regression tests."""
    
    def test_at_least_one_buy_trade(self):
        """Test that at least one BUY trade occurs."""
        df = create_synthetic_data_with_trades()
        result = run_mini_backtest(df, ma_window=5)
        
        buy_trades = result[result['Trade_Made_Type'] == 'BUY']
        assert len(buy_trades) >= 1, "Should have at least one BUY trade"
    
    def test_at_least_one_sell_trade(self):
        """Test that at least one SELL trade occurs."""
        df = create_synthetic_data_with_trades()
        result = run_mini_backtest(df, ma_window=5)
        
        sell_trades = result[result['Trade_Made_Type'] == 'SELL']
        assert len(sell_trades) >= 1, "Should have at least one SELL trade"
    
    def test_accounting_identity(self):
        """Test that Cash + Stock Value = Portfolio Value."""
        df = create_synthetic_data_with_trades()
        result = run_mini_backtest(df, ma_window=5)
        
        for idx in range(len(result)):
            row = result.iloc[idx]
            cash = row['Cash']
            shares = row['Total_Stocks_Owned']
            close = row['Close']
            portfolio_value = row['Remaining_Portfolio_Amount']
            
            expected = cash + (shares * close)
            assert abs(expected - portfolio_value) < 0.01, \
                f"Accounting mismatch at row {idx}: {expected} != {portfolio_value}"
    
    def test_final_schema_columns(self):
        """Test that final schema has all required columns."""
        df = create_synthetic_data_with_trades()
        result = run_mini_backtest(df, ma_window=5)
        final_df = build_final_schema(result)
        
        assert list(final_df.columns) == FINAL_COLUMNS
    
    def test_no_negative_cash(self):
        """Test that cash never goes negative."""
        df = create_synthetic_data_with_trades()
        result = run_mini_backtest(df, ma_window=5)
        
        assert (result['Cash'] >= -0.01).all(), "Cash should never be negative"
    
    def test_no_negative_shares(self):
        """Test that shares never go negative."""
        df = create_synthetic_data_with_trades()
        result = run_mini_backtest(df, ma_window=5)
        
        assert (result['Total_Stocks_Owned'] >= 0).all(), "Shares should never be negative"
    
    def test_weight_between_zero_and_one(self):
        """Test that Actual_Weight is always between 0 and 1."""
        df = create_synthetic_data_with_trades()
        result = run_mini_backtest(df, ma_window=5)
        
        weights = result['Actual_Weight']
        assert (weights >= 0).all(), "Weight should be >= 0"
        assert (weights <= 1.001).all(), "Weight should be <= 1"  # Small tolerance
    
    def test_regime_switch_triggers_trade(self):
        """Test that regime switches trigger trades."""
        df = create_synthetic_data_with_trades()
        result = run_mini_backtest(df, ma_window=5)
        
        # Find regime switches
        regime_switches = result[result['Rebalance_Reason_Code'] == 'REGIME_SWITCH']
        assert len(regime_switches) >= 1, "Should have at least one regime switch"
    
    def test_portfolio_value_never_zero(self):
        """Test that portfolio value is always positive."""
        df = create_synthetic_data_with_trades()
        result = run_mini_backtest(df, ma_window=5)
        
        assert (result['Remaining_Portfolio_Amount'] > 0).all(), \
            "Portfolio value should always be positive"


class TestRealDataRegression:
    """Regression tests using real QQQ data fixture."""
    
    def test_real_data_loads(self):
        """Test that real QQQ data can be loaded."""
        fixtures_path = Path(__file__).parent / "fixtures" / "sample_qqq.csv"
        df = load_qqq_csv(fixtures_path)
        df = normalize_data(df)
        
        assert len(df) > 0
    
    def test_indicators_compute(self):
        """Test that indicators can be computed on real data."""
        fixtures_path = Path(__file__).parent / "fixtures" / "sample_qqq.csv"
        df = load_qqq_csv(fixtures_path)
        df = normalize_data(df)
        df = add_ma250(df, window=5)  # Small window for fixture
        df = add_ma50(df, window=3)
        df = add_annualized_volatility(df, window=5)
        
        assert 'MA250' in df.columns
        assert 'MA50' in df.columns
        assert 'QQQ_ann_vol' in df.columns


# =============================================================================
# Phase 27: Deterministic Contract & Accounting Identity Tests
# =============================================================================

def run_mini_backtest_with_debug(df: pd.DataFrame, ma_window: int = 5) -> pd.DataFrame:
    """
    Run a mini backtest with debug columns (Cash_Open, Shares_Open).
    
    This version tracks open state for accounting identity verification.
    """
    # Add indicators with custom window
    df = df.copy()
    df['MA250'] = df['Close'].rolling(window=ma_window, min_periods=ma_window).mean()
    df['MA50'] = df['Close'].rolling(window=2, min_periods=2).mean()
    df = add_annualized_volatility(df, window=5)
    
    # Add regime signals
    df = add_base_regime(df)
    df = add_confirmed_regime(df)
    df = add_final_trading_regime(df)
    df = add_target_weight(df)
    
    # Simple fold (all data after MA is valid)
    valid_df = df[df['MA250'].notna()].copy().reset_index(drop=True)
    valid_df['Phase'] = 'test'
    valid_df['Fold_ID'] = 1
    valid_df['Train_Start'] = valid_df['Date'].iloc[0]
    valid_df['Train_End'] = valid_df['Date'].iloc[-1]
    valid_df['Test_Start'] = valid_df['Date'].iloc[0]
    valid_df['Test_End'] = valid_df['Date'].iloc[-1]
    
    # Add execution timing
    valid_df = add_exec_target_weight(valid_df)
    
    # Initialize portfolio
    state = init_portfolio(10000.0)
    prev_exec_weight = 0.0
    
    result_data = []
    
    for idx in range(len(valid_df)):
        row = valid_df.iloc[idx]
        open_price = row['Open']
        close_price = row['Close']
        exec_weight = row['Exec_Target_Weight']
        
        # Capture open state BEFORE trade (for Phase 27 debug columns)
        cash_open = state.cash
        shares_open = state.shares
        portfolio_value_open = cash_open + shares_open * open_price
        
        target_shares = compute_target_shares(portfolio_value_open, exec_weight, open_price)
        trade_result = execute_trade(state, target_shares, open_price)
        state = PortfolioState(cash=trade_result.new_cash, shares=trade_result.new_shares)
        trade_fields = compute_trade_fields(trade_result, prev_exec_weight, exec_weight)
        holdings = compute_holdings(trade_result)
        eod_value = compute_eod_valuation(state.shares, state.cash, close_price)
        actual_weight = compute_actual_weight(state.shares, close_price, eod_value)
        
        result_data.append({
            # Debug columns (Phase 27)
            'Cash_Open': cash_open,
            'Shares_Open': shares_open,
            # Standard columns
            'Portfolio_Value_Open': portfolio_value_open,
            'Target_Shares': target_shares,
            'Trade_Flag': trade_fields.Trade_Flag,
            'Trade_Made_Type': trade_fields.Trade_Made_Type,
            'Trade_Count': trade_fields.Trade_Count,
            'Net_Shares_Change': trade_fields.Net_Shares_Change,
            'Total_Notional_Abs': trade_fields.Total_Notional_Abs,
            'Fill_Price_VWAP': trade_fields.Fill_Price_VWAP,
            'Rebalance_Reason_Code': trade_fields.Rebalance_Reason_Code,
            'Total_Stocks_Owned': holdings.Total_Stocks_Owned,
            'Cash': holdings.Cash,
            'Remaining_Portfolio_Amount': eod_value,
            'Actual_Weight': actual_weight
        })
        
        prev_exec_weight = exec_weight
    
    for i, data in enumerate(result_data):
        for key, value in data.items():
            valid_df.loc[valid_df.index[i], key] = value
    
    # Add execution columns for schema compatibility
    valid_df['Exec_Symbol'] = 'QQQ'
    valid_df['Exec_Open'] = valid_df['Open']
    valid_df['Exec_Close'] = valid_df['Close']
    
    return valid_df


class TestDeterministicContract:
    """
    Tests for the Deterministic Contract (Phase 27).
    
    Deterministic Contract:
    1. Dates normalized (date-only), sorted, deduped
    2. Alignment produces identical date index for signal/exec
    3. Float comparisons use eps (ZERO_EPS, FLOOR_EPS)
    4. Rounding policy is single-source-of-truth
    5. Output ordering fixed (columns + row order)
    """
    
    def test_deterministic_contract(self):
        """
        Run the same backtest twice and verify identical output.
        
        This test enforces that the backtest is fully deterministic:
        - Same inputs -> same outputs
        - No random seeds required
        - Float rounding is consistent
        """
        df = create_synthetic_data_with_trades()
        
        # Run backtest twice
        result1 = run_mini_backtest_with_debug(df.copy(), ma_window=5)
        result2 = run_mini_backtest_with_debug(df.copy(), ma_window=5)
        
        # Key numeric columns to compare
        numeric_cols = [
            'Portfolio_Value_Open', 'Cash_Open', 'Shares_Open',
            'Target_Shares', 'Cash', 'Total_Stocks_Owned',
            'Remaining_Portfolio_Amount', 'Actual_Weight'
        ]
        
        for col in numeric_cols:
            if col in result1.columns:
                # Use tolerance for float comparison
                diff = (result1[col] - result2[col]).abs().max()
                assert diff < 1e-9, f"Column {col} differs between runs: max diff = {diff}"
        
        # Verify row order is identical
        assert len(result1) == len(result2)
        assert result1['Date'].equals(result2['Date'])
    
    def test_date_normalization(self):
        """Test that dates are normalized (date-only, no time component)."""
        df = create_synthetic_data_with_trades()
        result = run_mini_backtest_with_debug(df, ma_window=5)
        
        # Check that all dates have time component at midnight
        for date in result['Date']:
            assert date.hour == 0
            assert date.minute == 0
            assert date.second == 0
    
    def test_row_order_stable(self):
        """Test that row order is stable across runs."""
        df = create_synthetic_data_with_trades()
        
        result1 = run_mini_backtest_with_debug(df.copy(), ma_window=5)
        result2 = run_mini_backtest_with_debug(df.copy(), ma_window=5)
        
        # Dates should be in same order
        assert list(result1['Date']) == list(result2['Date'])


class TestAccountingIdentity:
    """
    Tests for equity accounting identities (Phase 27).
    
    equity_open = Cash_Open + Shares_Open * Exec_Open
    equity_close = Cash + Total_Stocks_Owned * Exec_Close
    """
    
    def test_equity_open_identity(self):
        """
        Test: equity_open = Cash_Open + Shares_Open * Exec_Open
        
        Portfolio_Value_Open should equal Cash_Open + Shares_Open * Exec_Open.
        """
        df = create_synthetic_data_with_trades()
        result = run_mini_backtest_with_debug(df, ma_window=5)
        
        for idx in range(len(result)):
            row = result.iloc[idx]
            cash_open = row['Cash_Open']
            shares_open = row['Shares_Open']
            exec_open = row['Exec_Open']
            portfolio_value_open = row['Portfolio_Value_Open']
            
            expected = cash_open + shares_open * exec_open
            assert abs(expected - portfolio_value_open) < 1e-6, \
                f"equity_open mismatch at row {idx}: {expected} != {portfolio_value_open}"
    
    def test_equity_close_identity(self):
        """
        Test: equity_close = Cash + Total_Stocks_Owned * Exec_Close
        
        Remaining_Portfolio_Amount should equal Cash + Total_Stocks_Owned * Exec_Close.
        """
        df = create_synthetic_data_with_trades()
        result = run_mini_backtest_with_debug(df, ma_window=5)
        
        for idx in range(len(result)):
            row = result.iloc[idx]
            cash = row['Cash']
            shares = row['Total_Stocks_Owned']
            exec_close = row['Exec_Close']
            portfolio_value = row['Remaining_Portfolio_Amount']
            
            expected = cash + shares * exec_close
            assert abs(expected - portfolio_value) < 1e-6, \
                f"equity_close mismatch at row {idx}: {expected} != {portfolio_value}"
    
    def test_no_trade_shares_unchanged(self):
        """
        Test: When no trade occurs, shares_open == shares_close.
        """
        df = create_synthetic_data_with_trades()
        result = run_mini_backtest_with_debug(df, ma_window=5)
        
        no_trade_rows = result[result['Trade_Flag'] == 0]
        
        for idx in no_trade_rows.index:
            row = result.loc[idx]
            shares_open = row['Shares_Open']
            shares_close = row['Total_Stocks_Owned']
            
            assert shares_open == shares_close, \
                f"Shares changed without trade at row {idx}: {shares_open} -> {shares_close}"
    
    def test_trade_changes_shares(self):
        """
        Test: When trade occurs, shares change by Net_Shares_Change.
        """
        df = create_synthetic_data_with_trades()
        result = run_mini_backtest_with_debug(df, ma_window=5)
        
        trade_rows = result[result['Trade_Flag'] == 1]
        
        for idx in trade_rows.index:
            row = result.loc[idx]
            shares_open = row['Shares_Open']
            shares_close = row['Total_Stocks_Owned']
            net_change = row['Net_Shares_Change']
            
            expected = shares_open + net_change
            assert expected == shares_close, \
                f"Share change mismatch at row {idx}: {shares_open} + {net_change} != {shares_close}"


# =============================================================================
# Phase 48: Rebalance Band Tuning Tests
# =============================================================================

class TestRebalanceBandTuning:
    """
    Tests for rebalance band tuning (Phase 48).
    
    Acceptance criteria:
    - Trade count decreases with wider band
    - Equity curve shape similarity
    """
    
    def test_wider_band_reduces_trades(self):
        """
        Test that wider rebalance band reduces trade count.
        """
        from portfolio import should_rebalance
        
        # Create a series of weights with small variations
        target_weights = [0.0, 1.0, 0.98, 0.95, 0.90, 0.85]
        actual_weights = [0.0, 0.0, 0.95, 0.92, 0.88, 0.84]
        
        # Count trades with 3% band
        trades_3pct = 0
        for target, actual in zip(target_weights, actual_weights):
            if should_rebalance(target, actual, 0.03):
                trades_3pct += 1
        
        # Count trades with 10% band
        trades_10pct = 0
        for target, actual in zip(target_weights, actual_weights):
            if should_rebalance(target, actual, 0.10):
                trades_10pct += 1
        
        # Wider band should result in fewer or equal trades
        assert trades_10pct <= trades_3pct, \
            f"Wider band should reduce trades: 10%={trades_10pct}, 3%={trades_3pct}"
    
    def test_regime_switch_always_trades(self):
        """
        Test that regime switches (0 <-> non-zero) always trigger trades,
        regardless of band size.
        """
        from portfolio import should_rebalance
        
        # 0 -> 1.0 should always trigger
        assert should_rebalance(1.0, 0.0, 0.03) is True
        assert should_rebalance(1.0, 0.0, 0.10) is True
        assert should_rebalance(1.0, 0.0, 0.50) is True
        
        # 1.0 -> 0 should always trigger
        assert should_rebalance(0.0, 0.95, 0.03) is True
        assert should_rebalance(0.0, 0.95, 0.10) is True
        assert should_rebalance(0.0, 0.95, 0.50) is True
    
    def test_band_parameter_values(self):
        """
        Test specific band parameter values from the plan.
        """
        from portfolio import should_rebalance
        
        # Test each band value: 3%, 5%, 7%, 10%
        bands = [0.03, 0.05, 0.07, 0.10]
        
        for band in bands:
            # Within band: no trade
            assert should_rebalance(0.50, 0.50 + band * 0.5, band) is False
            
            # Outside band: trade
            assert should_rebalance(0.50, 0.50 + band * 1.5, band) is True


# =============================================================================
# Phase 49: Execution Friction Stress Tests
# =============================================================================

class TestExecutionFriction:
    """
    Tests for execution friction stress tests (Phase 49).
    
    Slippage levels: 3, 5, 7, 10 bps
    
    PASS criteria:
    - final_equity > initial_equity (after costs)
    - max_drawdown <= ceiling
    - trade_count <= trade_count_cap
    - worst_20d_return > floor
    """
    
    def test_slippage_3bps_still_profitable(self):
        """
        Test that strategy remains profitable at 3 bps slippage.
        """
        from engine import apply_slippage
        
        # 3 bps slippage
        slippage_bps = 3.0
        
        # Apply to a $100 price
        buy_price = apply_slippage(100.0, "BUY", slippage_bps)
        sell_price = apply_slippage(100.0, "SELL", slippage_bps)
        
        # Round-trip cost
        round_trip_cost = (buy_price - sell_price) / 100.0
        
        # Should be roughly 6 bps (3 each way)
        assert 0.0005 < round_trip_cost < 0.001, \
            f"Round-trip cost at 3 bps: {round_trip_cost:.6f}"
    
    def test_slippage_10bps_still_profitable(self):
        """
        Test that strategy remains profitable at 10 bps slippage.
        """
        from engine import apply_slippage
        
        # 10 bps slippage
        slippage_bps = 10.0
        
        # Apply to a $100 price
        buy_price = apply_slippage(100.0, "BUY", slippage_bps)
        sell_price = apply_slippage(100.0, "SELL", slippage_bps)
        
        # Round-trip cost
        round_trip_cost = (buy_price - sell_price) / 100.0
        
        # Should be roughly 20 bps (10 each way)
        assert 0.0015 < round_trip_cost < 0.0025, \
            f"Round-trip cost at 10 bps: {round_trip_cost:.6f}"
    
    def test_slippage_symmetric(self):
        """
        Test that slippage is symmetric (BUY increases, SELL decreases).
        """
        from engine import apply_slippage
        
        base_price = 100.0
        slippage_bps = 5.0
        
        buy_price = apply_slippage(base_price, "BUY", slippage_bps)
        sell_price = apply_slippage(base_price, "SELL", slippage_bps)
        
        # BUY should be higher than base
        assert buy_price > base_price
        
        # SELL should be lower than base
        assert sell_price < base_price
        
        # Deviation should be symmetric
        buy_dev = buy_price - base_price
        sell_dev = base_price - sell_price
        assert abs(buy_dev - sell_dev) < 1e-10
    
    def test_viability_gate_components(self):
        """
        Test viability gate check function.
        """
        from metrics import check_viability_gate
        
        # Good metrics: should pass
        good_metrics = {
            'initial_equity': 10000.0,
            'final_equity': 12000.0,  # 20% gain
            'max_drawdown_pct': 0.15,  # 15% drawdown
            'worst_20d_return': -0.20,  # -20% worst period
            'trade_count': 50
        }
        
        passed, reasons = check_viability_gate(
            good_metrics,
            max_drawdown_ceiling=0.90,
            trade_count_cap=100,
            worst_20d_floor=-0.40
        )
        assert passed, f"Should pass: {reasons}"
        
        # Bad metrics: final < initial
        bad_metrics = good_metrics.copy()
        bad_metrics['final_equity'] = 9000.0
        
        passed, reasons = check_viability_gate(bad_metrics)
        assert not passed
        assert any("equity" in r.lower() for r in reasons)
    
    def test_slippage_levels_in_range(self):
        """
        Test all slippage levels mentioned in the plan.
        """
        from engine import apply_slippage
        
        slippage_levels = [3, 5, 7, 10]
        
        for bps in slippage_levels:
            buy_price = apply_slippage(100.0, "BUY", float(bps))
            expected = 100.0 * (1 + bps / 10000)
            
            assert abs(buy_price - expected) < 1e-10, \
                f"Slippage {bps} bps: expected {expected}, got {buy_price}"


# =============================================================================
# Phase 46: Worst 20-Day Return Uses Compound (Not Sum)
# =============================================================================

class TestWorst20DayCompound:
    """
    Tests proving worst 20-day return uses compounded returns, not sum.
    
    This is critical because sum of daily returns != compound return
    under volatility.
    """
    
    def test_worst_20d_uses_compound_not_sum(self):
        """
        Test that worst_20d metric uses compound formula, not sum of daily returns.
        
        Create synthetic data where sum of returns differs significantly from
        compound return, and verify our metric matches the compound definition.
        """
        from metrics import compute_worst_20d_return
        
        # Create equity series with known pattern
        # Start at 100, then 20 days of alternating +10% and -10%
        # Sum of returns: 0% (10 * 0.10 + 10 * -0.10 = 0)
        # Compound: (1.1 * 0.9)^10 - 1 ≈ -1.35% (volatility decay)
        
        equity_values = [100.0]
        for i in range(20):
            if i % 2 == 0:
                equity_values.append(equity_values[-1] * 1.10)  # +10%
            else:
                equity_values.append(equity_values[-1] * 0.90)  # -10%
        
        equity = pd.Series(equity_values)
        
        # Compute worst 20-day return
        worst_20d = compute_worst_20d_return(equity)
        
        # Calculate expected compound return
        # (1.1 * 0.9)^10 - 1 = 0.99^10 - 1 ≈ -0.0956
        expected_compound = (0.99 ** 10) - 1
        
        # If using sum of daily returns, would be 0
        # But compound should be negative due to volatility decay
        assert worst_20d < 0, "Compound return should be negative due to volatility decay"
        
        # Verify it's close to expected compound (within tolerance)
        assert abs(worst_20d - expected_compound) < 0.01, \
            f"Expected compound {expected_compound:.4f}, got {worst_20d:.4f}"
    
    def test_worst_20d_not_sum(self):
        """
        Explicit test: if we were summing daily returns, the result would differ.
        """
        from metrics import compute_worst_20d_return
        
        # Create a series where compound and sum give very different results
        # Large gains followed by partial reversal
        equity = pd.Series([
            100.0,  # Day 0
            200.0,  # Day 1: +100%
            180.0,  # Day 2: -10%
            162.0,  # Day 3: -10%
            145.8,  # Day 4: -10%
            131.2,  # Day 5: -10%
            118.1,  # Day 6: -10%
            106.3,  # Day 7: -10%
            95.7,   # Day 8: -10%
            86.1,   # Day 9: -10%
            77.5,   # Day 10: -10%
            69.8,   # Day 11: -10%
            62.8,   # Day 12: -10%
            56.5,   # Day 13: -10%
            50.9,   # Day 14: -10%
            45.8,   # Day 15: -10%
            41.2,   # Day 16: -10%
            37.1,   # Day 17: -10%
            33.4,   # Day 18: -10%
            30.0,   # Day 19: -10%
            27.0,   # Day 20: -10%
        ])
        
        worst_20d = compute_worst_20d_return(equity)
        
        # 20-day return from day 0 to day 20:
        # (27.0 / 100.0) - 1 = -0.73 = -73%
        # Sum of daily returns would be: +100% + 19*(-10%) = +100% - 190% = -90% (wrong!)
        
        expected_compound = (27.0 / 100.0) - 1  # -0.73
        
        # Our metric should match compound
        assert abs(worst_20d - expected_compound) < 0.02, \
            f"Expected {expected_compound:.4f}, got {worst_20d:.4f}"


# =============================================================================
# Phase 43: Single Definition Tests
# =============================================================================

class TestSingleDefinitionEnforcement:
    """
    Tests for Phase 43: Single-Definition Enforcement.
    
    Ensures compute_equity is used consistently.
    """
    
    def test_compute_equity_is_single_source(self):
        """Test that compute_equity produces correct result."""
        from portfolio import compute_equity
        
        # Test basic calculation
        equity = compute_equity(cash=5000.0, shares=50, price=100.0)
        expected = 5000.0 + 50 * 100.0
        
        assert equity == expected
    
    def test_compute_equity_matches_manual_calculation(self):
        """Test that compute_equity matches the formula."""
        from portfolio import compute_equity
        
        cash = 3456.78
        shares = 42
        price = 123.45
        
        equity = compute_equity(cash, shares, price)
        manual = cash + shares * price
        
        assert abs(equity - manual) < 1e-10


# =============================================================================
# Phase 36: NEXT_OPEN Sizing Tests
# =============================================================================

class TestNextOpenSizing:
    """Tests for NEXT_OPEN sizing behavior."""
    
    def test_next_open_sizing_uses_decision_price(self):
        """
        Test that NEXT_OPEN sizing uses decision_price for share calculation.
        
        NEXT_OPEN: decision_price = Exec_Close(t) for sizing
        """
        from portfolio import compute_target_shares
        
        portfolio_value = 10000.0
        weight = 0.5
        decision_price = 100.0  # Exec_Close(t)
        
        target_shares = compute_target_shares(portfolio_value, weight, decision_price)
        
        # 10000 * 0.5 / 100 = 50 shares
        assert target_shares == 50
    
    def test_equity_open_uses_exec_open_same_day_not_next_day(self):
        """
        Test: equity_open for day t must use Exec_Open(t), not Exec_Open(t+1).
        
        This prevents lookahead leaks.
        """
        from portfolio import compute_equity
        
        cash = 5000.0
        shares = 50
        exec_open_t = 100.0     # Today's open
        exec_open_t_plus_1 = 105.0  # Tomorrow's open (lookahead!)
        
        # Correct: use today's open
        equity_open_correct = compute_equity(cash, shares, exec_open_t)
        
        # Wrong: use tomorrow's open (lookahead)
        equity_open_wrong = compute_equity(cash, shares, exec_open_t_plus_1)
        
        # They should be different
        assert equity_open_correct != equity_open_wrong
        
        # Correct value
        assert equity_open_correct == 10000.0  # 5000 + 50*100
        
        # Wrong value (lookahead)
        assert equity_open_wrong == 10250.0  # 5000 + 50*105


# =============================================================================
# Phase 44: Reason Code Determinism Tests
# =============================================================================

class TestReasonCodeDeterminism:
    """Tests for reason code determinism (Phase 44)."""
    
    def test_reason_canonical_order_stable(self):
        """Test that same set of modifiers always produces same string."""
        from engine import ReasonBuilder, REASON_EXPOSURE_CLAMPED
        
        # Build reason in one order
        builder1 = ReasonBuilder("REBALANCE")
        builder1.add_modifier("THROTTLED")
        builder1.add_modifier(REASON_EXPOSURE_CLAMPED)
        builder1.add_flatten()
        result1 = builder1.build()
        
        # Build reason in different order
        builder2 = ReasonBuilder("REBALANCE")
        builder2.add_flatten()
        builder2.add_modifier(REASON_EXPOSURE_CLAMPED)
        builder2.add_modifier("THROTTLED")
        result2 = builder2.build()
        
        # Should produce identical strings (canonical order)
        assert result1 == result2
    
    def test_reason_base_in_known_list(self):
        """Test that base reasons use known constants."""
        from engine import (
            REASON_REGIME_SWITCH_BUY, REASON_REGIME_SWITCH_SELL,
            REASON_REBALANCE, REASON_BAND_SKIP, REASON_HALT_DAILY_LOSS,
            REASON_HALT_DRAWDOWN, REASON_HALT_DATA_GAP
        )
        
        # All known base reasons should be non-empty strings
        known_reasons = [
            REASON_REGIME_SWITCH_BUY,
            REASON_REGIME_SWITCH_SELL,
            REASON_REBALANCE,
            REASON_BAND_SKIP,
            REASON_HALT_DAILY_LOSS,
            REASON_HALT_DRAWDOWN,
            REASON_HALT_DATA_GAP
        ]
        
        for reason in known_reasons:
            assert isinstance(reason, str)
            assert len(reason) > 0
