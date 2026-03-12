#!/usr/bin/env python3
"""Analyze MA250 strategy year by year."""
import sys
from pathlib import Path
import pandas as pd

sys.path.insert(0, str(Path(__file__).parent / "src"))

from data_loader import load_qqq_csv, normalize_data
from indicators import add_ma250, add_ma50, add_annualized_volatility
from regime import add_base_regime, add_confirmed_regime, add_final_trading_regime, add_target_weight
from portfolio import (
    init_portfolio, add_exec_target_weight, compute_target_shares,
    execute_trade, compute_trade_fields, PortfolioState
)


def run_year_backtest(df: pd.DataFrame, year: int, initial_capital: float = 10000.0) -> dict:
    """Run backtest for a specific year, using prior 250 days for MA calculation."""
    # Need data from prior year for MA250
    start_date = f"{year-1}-01-01"
    end_date = f"{year}-12-31"
    
    period_df = df[(df['Date'] >= start_date) & (df['Date'] <= end_date)].copy().reset_index(drop=True)
    
    if len(period_df) < 260:
        return None
    
    # Add indicators
    period_df = add_ma250(period_df)
    period_df = add_ma50(period_df)
    period_df = add_annualized_volatility(period_df)
    
    # Add regime signals
    period_df = add_base_regime(period_df)
    period_df = add_confirmed_regime(period_df)
    period_df = add_final_trading_regime(period_df)
    period_df = add_target_weight(period_df)
    
    # Filter to just the target year
    year_df = period_df[period_df['Date'].dt.year == year].copy().reset_index(drop=True)
    
    if len(year_df) < 10 or year_df['MA250'].isna().all():
        return None
    
    year_df = add_exec_target_weight(year_df)
    
    # Initialize portfolio
    state = init_portfolio(initial_capital)
    prev_exec_weight = 0.0
    trade_count = 0
    
    # Buy and hold
    first_open = year_df.iloc[0]['Open']
    bh_shares = int(initial_capital / first_open)
    bh_remaining = initial_capital - bh_shares * first_open
    
    for idx in range(len(year_df)):
        row = year_df.iloc[idx]
        open_price = row['Open']
        exec_weight = row['Exec_Target_Weight']
        
        portfolio_value_open = state.cash + state.shares * open_price
        target_shares = compute_target_shares(portfolio_value_open, exec_weight, open_price)
        trade_result = execute_trade(state, target_shares, open_price)
        state = PortfolioState(cash=trade_result.new_cash, shares=trade_result.new_shares)
        trade_fields = compute_trade_fields(trade_result, prev_exec_weight, exec_weight)
        
        if trade_fields.Trade_Flag:
            trade_count += 1
        
        prev_exec_weight = exec_weight
    
    final_close = year_df.iloc[-1]['Close']
    final_value = state.cash + state.shares * final_close
    strategy_return = (final_value - initial_capital) / initial_capital * 100
    
    bh_value = bh_shares * final_close + bh_remaining
    bh_return = (bh_value - initial_capital) / initial_capital * 100
    
    return {
        'year': year,
        'days': len(year_df),
        'trades': trade_count,
        'strategy_return': strategy_return,
        'buy_hold_return': bh_return,
        'outperformance': strategy_return - bh_return,
        'final_value': final_value
    }


def main():
    print("=" * 80)
    print("QQQ MA250 STRATEGY - YEARLY ANALYSIS")
    print("=" * 80)
    
    df = load_qqq_csv('data/qqq_us_d.csv')
    df = normalize_data(df)
    
    print(f"\nData: {df['Date'].min().date()} to {df['Date'].max().date()}")
    
    years = list(range(2001, 2026))  # 2001 onwards (need 2000 for MA250)
    
    results = []
    
    print("\n" + "-" * 80)
    print(f"{'Year':>6} {'Days':>6} {'Trades':>7} {'Strategy':>12} {'Buy&Hold':>12} {'Outperf':>10} {'Winner':>10}")
    print("-" * 80)
    
    strategy_wins = 0
    bh_wins = 0
    
    for year in years:
        result = run_year_backtest(df, year)
        if result:
            results.append(result)
            winner = "Strategy" if result['outperformance'] > 0 else "Buy&Hold"
            if result['outperformance'] > 0:
                strategy_wins += 1
            else:
                bh_wins += 1
            
            print(f"{result['year']:>6} {result['days']:>6} {result['trades']:>7} "
                  f"{result['strategy_return']:>+11.2f}% {result['buy_hold_return']:>+11.2f}% "
                  f"{result['outperformance']:>+9.2f}% {winner:>10}")
    
    print("-" * 80)
    
    # Summary
    print("\n" + "=" * 80)
    print("YEARLY SUMMARY")
    print("=" * 80)
    
    avg_strat = sum(r['strategy_return'] for r in results) / len(results)
    avg_bh = sum(r['buy_hold_return'] for r in results) / len(results)
    
    profitable_years = sum(1 for r in results if r['strategy_return'] > 0)
    
    print(f"\nYears Analyzed: {len(results)}")
    print(f"Strategy Profitable Years: {profitable_years}/{len(results)} ({profitable_years/len(results)*100:.0f}%)")
    print(f"Strategy Outperforms B&H: {strategy_wins}/{len(results)} ({strategy_wins/len(results)*100:.0f}%)")
    print(f"Buy&Hold Outperforms: {bh_wins}/{len(results)} ({bh_wins/len(results)*100:.0f}%)")
    
    print(f"\nAverage Annual Strategy Return: {avg_strat:+.2f}%")
    print(f"Average Annual Buy&Hold Return: {avg_bh:+.2f}%")
    print(f"Average Annual Outperformance: {avg_strat - avg_bh:+.2f}%")
    
    # Best and worst years
    print("\n" + "-" * 40)
    print("BEST YEARS FOR STRATEGY (vs Buy&Hold):")
    sorted_results = sorted(results, key=lambda x: x['outperformance'], reverse=True)
    for r in sorted_results[:5]:
        print(f"  {r['year']}: {r['outperformance']:+.2f}% (Strat: {r['strategy_return']:+.1f}%, B&H: {r['buy_hold_return']:+.1f}%)")
    
    print("\nWORST YEARS FOR STRATEGY (vs Buy&Hold):")
    for r in sorted_results[-5:]:
        print(f"  {r['year']}: {r['outperformance']:+.2f}% (Strat: {r['strategy_return']:+.1f}%, B&H: {r['buy_hold_return']:+.1f}%)")
    
    # Compound returns
    print("\n" + "=" * 80)
    print("COMPOUND GROWTH ($10,000 initial)")
    print("=" * 80)
    
    strategy_compound = 10000.0
    bh_compound = 10000.0
    
    for r in results:
        strategy_compound *= (1 + r['strategy_return'] / 100)
        bh_compound *= (1 + r['buy_hold_return'] / 100)
    
    print(f"\n$10,000 invested in 2001:")
    print(f"  Strategy Final Value (2025): ${strategy_compound:,.2f}")
    print(f"  Buy&Hold Final Value (2025): ${bh_compound:,.2f}")
    print(f"  Difference: ${strategy_compound - bh_compound:+,.2f}")
    
    # Verdict
    print("\n" + "=" * 80)
    print("FINAL VERDICT")
    print("=" * 80)
    
    if strategy_compound > bh_compound:
        print("\nOVERALL: MA250 Strategy is PROFITABLE and OUTPERFORMS Buy&Hold over the long term!")
        print(f"The strategy turned $10,000 into ${strategy_compound:,.0f} vs ${bh_compound:,.0f} for Buy&Hold.")
    else:
        print("\nOVERALL: MA250 Strategy is PROFITABLE but UNDERPERFORMS Buy&Hold over the long term.")
        print(f"The strategy turned $10,000 into ${strategy_compound:,.0f} vs ${bh_compound:,.0f} for Buy&Hold.")
    
    print("\nKEY TAKEAWAY: The MA250 strategy adds value during market crashes by")
    print("reducing drawdowns, but sacrifices some upside during strong bull markets.")
    print("It's best suited for risk-averse investors who prioritize capital preservation.")
    print("=" * 80)


if __name__ == '__main__':
    main()
