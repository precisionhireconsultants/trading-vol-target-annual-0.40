#!/usr/bin/env python
"""Run backtests across multiple time folds and display results.

Supports both daily (CSV) and intraday (Alpaca parquet) modes via
``--backtest-mode``.
"""
import argparse
import sys
import subprocess
import traceback
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import pandas as pd
from metrics import compute_baseline_metrics, check_viability_gate
from invariants import compute_trade_count_cap
from config import DEFAULT_CONFIG

# TQQQ and SQQQ both started 2010-02-11; all folds must start on or after this date
TQQQ_SQQQ_START = "2010-02-11"

# Alpaca IEX minute data starts ~2016
ALPACA_INTRADAY_START = "2016-01-04"

# Define folds to test (start_date, years, name) — all starts from TQQQ/SQQQ availability
FOLDS_DAILY = [
    (TQQQ_SQQQ_START, 3, "2010-2012"),
    (TQQQ_SQQQ_START, 5, "2010-2014"),
    (TQQQ_SQQQ_START, 7, "2010-2016"),
    (TQQQ_SQQQ_START, 10, "2010-2019"),
    (TQQQ_SQQQ_START, 14, "2010-2023 (full)"),
    ("2012-01-01", 3, "2012-2014"),
    ("2014-01-01", 3, "2014-2016"),
    ("2016-01-01", 3, "2016-2018"),
    ("2018-01-01", 3, "2018-2020"),
    ("2020-01-01", 3, "2020-2022"),
    ("2022-01-01", 3, "2022-2024"),
    ("2012-01-01", 5, "2012-2016"),
    ("2015-01-01", 5, "2015-2019"),
    ("2018-01-01", 5, "2018-2022"),
    ("2019-01-01", 5, "2019-2023"),
]

FOLDS_INTRADAY = [
    (ALPACA_INTRADAY_START, 3, "2016-2018"),
    (ALPACA_INTRADAY_START, 5, "2016-2020"),
    (ALPACA_INTRADAY_START, 8, "2016-2023"),
    ("2018-01-01", 3, "2018-2020"),
    ("2020-01-01", 3, "2020-2022"),
    ("2022-01-01", 3, "2022-2024"),
    ("2018-01-01", 5, "2018-2022"),
    ("2019-01-01", 5, "2019-2023"),
]


def run_fold(start_date: str, years: int, name: str, backtest_mode: str = "daily") -> dict:
    """Run a single fold and return metrics."""
    output_dir = Path(__file__).parent.parent / "output"
    output_dir.mkdir(parents=True, exist_ok=True)
    output_file = output_dir / f"fold_{name.replace('-', '_').replace(' ', '_')}.csv"

    if backtest_mode == "intraday":
        cmd = [
            sys.executable, "run.py",
            "--backtest-mode", "intraday",
            "--fold-start", start_date,
            "--fold-years", str(years),
            "--out", str(output_file),
        ]
    else:
        cmd = [
            sys.executable, "run.py",
            "--qqq", "data/qqq_us_d.csv",
            "--tqqq", "data/tqqq_us_d.csv",
            # "--sqqq", "data/sqqq_us_d.csv",
            "--fold-start", start_date,
            "--fold-years", str(years),
            "--out", str(output_file),
        ]
    
    project_root = Path(__file__).parent.parent
    result = subprocess.run(cmd, capture_output=True, text=True, cwd=str(project_root))
    
    if result.returncode != 0:
        return {
            "name": name,
            "status": "ERROR",
            "error": result.stderr or result.stdout
        }
    
    # Load results and compute metrics
    try:
        df = pd.read_csv(output_file)
        metrics = compute_baseline_metrics(df)
        trade_cap = compute_trade_count_cap(len(df))
        days = len(df)
        trading_days_per_year = DEFAULT_CONFIG.TRADING_DAYS_PER_YEAR
        years = days / trading_days_per_year if trading_days_per_year > 0 else 0.0
        if years > 0 and metrics["total_return"] > -1.0:
            cagr = (1.0 + metrics["total_return"]) ** (1.0 / years) - 1.0
        else:
            cagr = 0.0
        passed, reasons = check_viability_gate(metrics, trade_count_cap=trade_cap)
        
        return {
            "name": name,
            "status": "PASS" if passed else "FAIL",
            "rows": days,
            "initial": metrics["initial_equity"],
            "final": metrics["final_equity"],
            "return_pct": metrics["total_return"] * 100,  # Convert to %
            "cagr_pct": cagr * 100,  # Annualized return %
            "max_dd": metrics["max_drawdown_pct"] * 100,  # Convert to %
            "worst_20d": metrics["worst_20d_return"] * 100,  # Convert to %
            "trades": metrics["trade_count"],
            "exposure_pct": metrics["exposure_pct"] * 100,  # Convert to %
            "fail_reasons": reasons if not passed else []
        }
    except Exception as e:
        return {
            "name": name,
            "status": "ERROR",
            "error": traceback.format_exc()
        }

def main():
    ap = argparse.ArgumentParser(description="Run backtests across multiple folds")
    ap.add_argument(
        "--backtest-mode",
        choices=["daily", "intraday"],
        default=DEFAULT_CONFIG.BACKTEST_MODE,
        help="Pipeline mode (default: daily)",
    )
    cli_args = ap.parse_args()
    backtest_mode = cli_args.backtest_mode

    folds = FOLDS_INTRADAY if backtest_mode == "intraday" else FOLDS_DAILY

    print("=" * 100)
    title = "MULTI-FOLD BACKTEST RESULTS"
    if backtest_mode == "intraday":
        title += " (INTRADAY / Alpaca 1-min)"
    else:
        title += " (TQQQ + SQQQ, Risk Controls)"
    print(title)
    print("=" * 100)

    results = []
    for start_date, years, name in folds:
        print(f"\nRunning fold: {name}...", end=" ", flush=True)
        result = run_fold(start_date, years, name, backtest_mode=backtest_mode)
        results.append(result)
        print(result["status"])
        if result["status"] == "ERROR":
            print(result["error"], file=sys.stderr)
    
    # Print summary table
    print("\n" + "=" * 100)
    print(f"{'Fold':<12} {'Status':<6} {'Days':>6} {'Return':>10} {'CAGR':>10} {'Max DD':>10} {'Worst 20d':>10} {'Trades':>7} {'Exposure':>8}")
    print("-" * 100)
    
    for r in results:
        if r["status"] == "ERROR":
            print(f"{r['name']:<12} {'ERROR':<6} {r.get('error', 'Unknown')[:70]}")
        else:
            print(f"{r['name']:<12} {r['status']:<6} {r['rows']:>6} {r['return_pct']:>+9.2f}% {r['cagr_pct']:>+9.2f}% {r['max_dd']:>9.2f}% {r['worst_20d']:>+9.2f}% {r['trades']:>7} {r['exposure_pct']:>7.1f}%")
    
    print("-" * 100)
    
    # Summary statistics
    passed = [r for r in results if r["status"] == "PASS"]
    failed = [r for r in results if r["status"] == "FAIL"]
    errors = [r for r in results if r["status"] == "ERROR"]
    
    print(f"\nSUMMARY: {len(passed)} PASSED, {len(failed)} FAILED, {len(errors)} ERRORS")
    
    if passed:
        avg_return = sum(r["return_pct"] for r in passed) / len(passed)
        avg_cagr = sum(r["cagr_pct"] for r in passed) / len(passed)
        avg_dd = sum(r["max_dd"] for r in passed) / len(passed)
        print(f"Average Return (PASS): {avg_return:+.2f}%")
        print(f"Average CAGR (PASS):   {avg_cagr:+.2f}%")
        print(f"Average Max DD (PASS): {avg_dd:.2f}%")
    
    # Show failure reasons
    if failed:
        print("\nFAILURE REASONS:")
        for r in failed:
            print(f"  {r['name']}: {', '.join(r['fail_reasons'])}")
    
    print("=" * 100)

if __name__ == "__main__":
    main()
