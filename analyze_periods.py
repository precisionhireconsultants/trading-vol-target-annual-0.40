#!/usr/bin/env python3
"""
Analyze MA250 strategy across different time periods.

Tests profitability across various market conditions:
- Dot-com bust (2000-2003)
- Recovery (2003-2007)
- Financial crisis (2007-2009)
- Bull market (2009-2020)
- COVID crash and recovery (2020-2021)
- Recent period (2022-2026)
"""
import sys
from pathlib import Path
from datetime import datetime
import pandas as pd

sys.path.insert(0, str(Path(__file__).parent / "src"))

from data_loader import load_qqq_csv, normalize_data
from indicators import add_ma250, add_ma50, add_annualized_volatility
from regime import add_base_regime, add_confirmed_regime, add_final_trading_regime, add_target_weight
from portfolio import (
    init_portfolio, add_exec_target_weight, compute_target_shares,
    execute_trade, compute_trade_fields, compute_holdings,
    compute_eod_valuation, compute_actual_weight, PortfolioState
)


def run_period_backtest(df: pd.DataFrame, start_date: str, end_date: str, initial_capital: float = 10000.0) -> dict:
    """Run backtest for a specific date range."""
    # Filter data
    df = df.copy()
    df = df[(df['Date'] >= start_date) & (df['Date'] <= end_date)].reset_index(drop=True)
    
    if len(df) < 250:
        return None  # Not enough data for MA250
    
    # Add indicators
    df = add_ma250(df)
    df = add_ma50(df)
    df = add_annualized_volatility(df)
    
    # Add regime signals
    df = add_base_regime(df)
    df = add_confirmed_regime(df)
    df = add_final_trading_regime(df)
    df = add_target_weight(df)
    
    # Get valid data (where MA250 exists)
    valid_df = df[df['MA250'].notna()].copy().reset_index(drop=True)
    
    if len(valid_df) < 10:
        return None
    
    valid_df = add_exec_target_weight(valid_df)
    
    # Initialize portfolio
    state = init_portfolio(initial_capital)
    prev_exec_weight = 0.0
    trade_count = 0
    
    # Track buy & hold for comparison
    first_price = valid_df.iloc[0]['Open']
    shares_buy_hold = int(initial_capital / first_price)
    buy_hold_cost = shares_buy_hold * first_price
    
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
        
        if trade_fields.Trade_Flag:
            trade_count += 1
        
        prev_exec_weight = exec_weight
    
    # Final calculations
    final_close = valid_df.iloc[-1]['Close']
    final_value = state.cash + state.shares * final_close
    strategy_return = (final_value - initial_capital) / initial_capital * 100
    
    buy_hold_value = shares_buy_hold * final_close + (initial_capital - buy_hold_cost)
    buy_hold_return = (buy_hold_value - initial_capital) / initial_capital * 100
    
    return {
        'start_date': valid_df.iloc[0]['Date'].strftime('%Y-%m-%d'),
        'end_date': valid_df.iloc[-1]['Date'].strftime('%Y-%m-%d'),
        'days': len(valid_df),
        'trades': trade_count,
        'initial': initial_capital,
        'final_strategy': final_value,
        'strategy_return': strategy_return,
        'final_buy_hold': buy_hold_value,
        'buy_hold_return': buy_hold_return,
        'outperformance': strategy_return - buy_hold_return
    }


def main():
    print("=" * 80)
    print("QQQ MA250 STRATEGY - MULTI-PERIOD ANALYSIS")
    print("=" * 80)
    
    # Load data
    df = load_qqq_csv('data/qqq_us_d.csv')
    df = normalize_data(df)
    print(f"\nLoaded {len(df)} rows of data")
    print(f"Date range: {df['Date'].min().date()} to {df['Date'].max().date()}")
    
    # Define test periods
    periods = [
        ("Dot-com Bust", "2000-01-01", "2003-12-31"),
        ("Recovery", "2003-01-01", "2007-12-31"),
        ("Financial Crisis", "2007-01-01", "2010-12-31"),
        ("Post-Crisis Bull", "2010-01-01", "2015-12-31"),
        ("Pre-COVID Bull", "2015-01-01", "2020-02-19"),
        ("COVID Period", "2020-01-01", "2021-12-31"),
        ("2022 Bear", "2022-01-01", "2022-12-31"),
        ("2023 Recovery", "2023-01-01", "2023-12-31"),
        ("2024-2026", "2024-01-01", "2026-12-31"),
        ("Full History", "1999-01-01", "2026-12-31"),
    ]
    
    results = []
    
    print("\n" + "-" * 80)
    print(f"{'Period':<20} {'Days':>6} {'Trades':>7} {'Strategy':>12} {'Buy&Hold':>12} {'Outperf':>10}")
    print("-" * 80)
    
    for name, start, end in periods:
        result = run_period_backtest(df, start, end)
        if result:
            results.append((name, result))
            print(f"{name:<20} {result['days']:>6} {result['trades']:>7} "
                  f"{result['strategy_return']:>+11.2f}% {result['buy_hold_return']:>+11.2f}% "
                  f"{result['outperformance']:>+9.2f}%")
        else:
            print(f"{name:<20} {'Insufficient data':>50}")
    
    # Summary statistics
    print("\n" + "=" * 80)
    print("SUMMARY ANALYSIS")
    print("=" * 80)
    
    wins = sum(1 for _, r in results if r['outperformance'] > 0)
    losses = sum(1 for _, r in results if r['outperformance'] <= 0)
    
    profitable_periods = sum(1 for _, r in results if r['strategy_return'] > 0)
    total_periods = len(results)
    
    avg_strategy = sum(r['strategy_return'] for _, r in results) / len(results) if results else 0
    avg_buyhold = sum(r['buy_hold_return'] for _, r in results) / len(results) if results else 0
    
    print(f"\nPeriods Analyzed: {total_periods}")
    print(f"Profitable Periods (Strategy > 0): {profitable_periods}/{total_periods}")
    print(f"Outperformed Buy&Hold: {wins}/{total_periods}")
    print(f"\nAverage Strategy Return: {avg_strategy:+.2f}%")
    print(f"Average Buy&Hold Return: {avg_buyhold:+.2f}%")
    print(f"Average Outperformance: {avg_strategy - avg_buyhold:+.2f}%")
    
    # Key insight
    print("\n" + "=" * 80)
    print("KEY INSIGHTS")
    print("=" * 80)
    
    full_result = next((r for n, r in results if n == "Full History"), None)
    if full_result:
        print(f"\nFull History ({full_result['start_date']} to {full_result['end_date']}):")
        print(f"  Strategy: ${full_result['initial']:,.0f} -> ${full_result['final_strategy']:,.2f} ({full_result['strategy_return']:+.2f}%)")
        print(f"  Buy&Hold: ${full_result['initial']:,.0f} -> ${full_result['final_buy_hold']:,.2f} ({full_result['buy_hold_return']:+.2f}%)")
    
    # When does strategy work best?
    print("\nStrategy tends to outperform during:")
    for name, r in results:
        if r['outperformance'] > 5:
            print(f"  - {name}: +{r['outperformance']:.1f}% vs Buy&Hold")
    
    print("\nStrategy underperforms during:")
    for name, r in results:
        if r['outperformance'] < -5:
            print(f"  - {name}: {r['outperformance']:.1f}% vs Buy&Hold")
    
    # Conclusion
    print("\n" + "=" * 80)
    print("CONCLUSION")
    print("=" * 80)
    if avg_strategy - avg_buyhold > 0:
        print("\nThe MA250 strategy OUTPERFORMS buy-and-hold on average across tested periods.")
    else:
        print("\nThe MA250 strategy UNDERPERFORMS buy-and-hold on average across tested periods.")
    
    print("\nThe strategy provides value during BEAR MARKETS by avoiding large drawdowns,")
    print("but may lag during strong BULL MARKETS due to late entry signals.")
    print("=" * 80)


if __name__ == '__main__':
    main()
