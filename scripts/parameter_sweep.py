#!/usr/bin/env python
"""Risk parameter sweep script (Phase 47).

One-at-a-time testing of risk parameters with Malik strategy invariants enforced.
This is NOT strategy discovery. This is parameter sanity tuning for stability.

Usage:
    python scripts/parameter_sweep.py --qqq data/qqq_us_d.csv

Parameters swept:
- MAX_DAILY_LOSS: 1%, 2%, 3%
- MAX_DRAWDOWN: 15%, 25%, 30%
- COOLDOWN_DAYS: 1, 3, 5
- MAX_EFFECTIVE_EXPOSURE: 0.6, 0.8, 1.0
"""
import sys
import os
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import argparse
import pandas as pd
from typing import Dict, Any, List

from config import TradingConfig
from metrics import compute_baseline_metrics, format_metrics_report, check_viability_gate
from invariants import assert_malik_invariants, compute_trade_count_cap


# Parameter ranges to sweep
SWEEP_PARAMS = {
    'MAX_DAILY_LOSS_PCT': [0.01, 0.02, 0.03],
    'MAX_DRAWDOWN_PCT': [0.15, 0.25, 0.30],
    'HALT_COOLDOWN_DAYS': [1, 3, 5],
    'MAX_EFFECTIVE_EXPOSURE': [0.6, 0.8, 1.0],
}


def run_backtest_with_config(
    qqq_path: str,
    config_overrides: Dict[str, Any]
) -> pd.DataFrame:
    """
    Run backtest with specific configuration.
    
    This is a stub - in production, this would run the full backtest
    with the given configuration overrides.
    
    Args:
        qqq_path: Path to QQQ data
        config_overrides: Dict of config parameters to override
        
    Returns:
        DataFrame with backtest results
    """
    # Import here to avoid circular imports
    from data_loader import load_qqq_csv, normalize_data
    from indicators import add_ma250, add_ma50, add_annualized_volatility
    from regime import add_base_regime, add_confirmed_regime, add_final_trading_regime, add_target_weight
    from portfolio import add_exec_target_weight
    
    # Load data
    df = load_qqq_csv(qqq_path)
    df = normalize_data(df)
    
    # Add indicators
    df = add_ma250(df)
    df = add_ma50(df)
    df = add_annualized_volatility(df)
    
    # Add regime
    df = add_base_regime(df)
    df = add_confirmed_regime(df)
    df = add_final_trading_regime(df)
    df = add_target_weight(df)
    df = add_exec_target_weight(df)
    
    # Filter to valid data (after MA250 warmup)
    df = df[df['MA250'].notna()].copy().reset_index(drop=True)
    
    # Simulate portfolio (simplified for sweep)
    initial_capital = 10000.0
    cash = initial_capital
    shares = 0
    
    results = []
    for idx in range(len(df)):
        row = df.iloc[idx]
        open_price = row['Open']
        close_price = row['Close']
        target_weight = row['Exec_Target_Weight']
        
        # Portfolio value at open
        portfolio_open = cash + shares * open_price
        
        # Target shares
        target_shares = int(portfolio_open * target_weight / open_price) if open_price > 0 else 0
        
        # Trade
        shares_diff = target_shares - shares
        trade_flag = 1 if shares_diff != 0 else 0
        if shares_diff > 0:
            cost = shares_diff * open_price
            cash -= cost
            shares = target_shares
        elif shares_diff < 0:
            proceeds = abs(shares_diff) * open_price
            cash += proceeds
            shares = target_shares
        
        # End of day value
        portfolio_close = cash + shares * close_price
        actual_weight = (shares * close_price) / portfolio_close if portfolio_close > 0 else 0
        
        results.append({
            'Date': row['Date'],
            'Exec_Open': open_price,
            'Exec_Close': close_price,
            'Cash': cash,
            'Total_Stocks_Owned': shares,
            'Portfolio_Value_Open': portfolio_open,
            'Remaining_Portfolio_Amount': portfolio_close,
            'Actual_Weight': actual_weight,
            'Trade_Flag': trade_flag
        })
    
    return pd.DataFrame(results)


def evaluate_config(
    qqq_path: str,
    config_overrides: Dict[str, Any],
    baseline_metrics: Dict[str, Any] = None
) -> Dict[str, Any]:
    """
    Evaluate a configuration and return metrics.
    
    Args:
        qqq_path: Path to QQQ data
        config_overrides: Config overrides
        baseline_metrics: Baseline metrics for comparison
        
    Returns:
        Dict with metrics and evaluation result
    """
    try:
        # Run backtest
        df = run_backtest_with_config(qqq_path, config_overrides)
        
        # Check invariants
        assert_malik_invariants(df)
        
        # Compute metrics
        metrics = compute_baseline_metrics(df)
        
        # Check viability
        trade_cap = compute_trade_count_cap(len(df))
        passed, reasons = check_viability_gate(metrics, trade_count_cap=trade_cap)
        
        return {
            'config': config_overrides,
            'metrics': metrics,
            'passed': passed,
            'reasons': reasons,
            'error': None
        }
    except Exception as e:
        return {
            'config': config_overrides,
            'metrics': None,
            'passed': False,
            'reasons': [str(e)],
            'error': str(e)
        }


def run_sweep(qqq_path: str) -> List[Dict[str, Any]]:
    """
    Run full parameter sweep.
    
    Args:
        qqq_path: Path to QQQ data
        
    Returns:
        List of evaluation results
    """
    print("=" * 60)
    print("RISK PARAMETER SWEEP")
    print("=" * 60)
    
    # First run baseline
    print("\nRunning baseline configuration...")
    baseline = evaluate_config(qqq_path, {})
    
    if baseline['error']:
        print(f"Baseline failed: {baseline['error']}")
        return [baseline]
    
    print(format_metrics_report(baseline['metrics']))
    
    results = [baseline]
    
    # Sweep each parameter
    for param_name, values in SWEEP_PARAMS.items():
        print(f"\n{'=' * 60}")
        print(f"Sweeping {param_name}")
        print("=" * 60)
        
        for value in values:
            config = {param_name: value}
            print(f"\n{param_name} = {value}")
            
            result = evaluate_config(qqq_path, config, baseline['metrics'])
            results.append(result)
            
            if result['error']:
                print(f"  ERROR: {result['error']}")
            else:
                m = result['metrics']
                status = "PASS" if result['passed'] else "FAIL"
                print(f"  Status: {status}")
                print(f"  Max DD: {m['max_drawdown_pct']:.2%}")
                print(f"  Worst 20d: {m['worst_20d_return']:.2%}")
                print(f"  Trade Count: {m['trade_count']}")
                print(f"  Final Equity: ${m['final_equity']:,.2f}")
                
                if not result['passed']:
                    for r in result['reasons']:
                        print(f"  Reason: {r}")
    
    # Summary
    print("\n" + "=" * 60)
    print("SWEEP SUMMARY")
    print("=" * 60)
    
    passed = sum(1 for r in results if r['passed'])
    total = len(results)
    print(f"Passed: {passed}/{total}")
    
    return results


def main():
    parser = argparse.ArgumentParser(description="Risk parameter sweep")
    parser.add_argument("--qqq", required=True, help="Path to QQQ CSV")
    parser.add_argument("--out", default="reports/sweep_results.csv", help="Output path")
    args = parser.parse_args()
    
    results = run_sweep(args.qqq)
    
    # Save results
    output_path = Path(args.out)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Convert to DataFrame for saving
    records = []
    for r in results:
        record = {'config': str(r['config']), 'passed': r['passed']}
        if r['metrics']:
            record.update(r['metrics'])
        records.append(record)
    
    pd.DataFrame(records).to_csv(output_path, index=False)
    print(f"\nResults saved to {output_path}")


if __name__ == "__main__":
    main()
