#!/usr/bin/env python
"""Show baseline metrics for a backtest result."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import pandas as pd
from metrics import compute_baseline_metrics, format_metrics_report, check_viability_gate
from invariants import assert_malik_invariants, compute_trade_count_cap

# Load the backtest results
csv_path = sys.argv[1] if len(sys.argv) > 1 else "output/full_backtest.csv"
df = pd.read_csv(csv_path)

print("=" * 60)
print("BACKTEST ANALYSIS")
print("=" * 60)
print(f"File: {csv_path}")
print(f"Rows: {len(df)}")

# Check invariants
print("\n" + "-" * 60)
try:
    assert_malik_invariants(df)
    print("INVARIANT CHECK: PASSED")
except Exception as e:
    print(f"INVARIANT CHECK: FAILED - {e}")

# Compute metrics
metrics = compute_baseline_metrics(df)
print("\n" + format_metrics_report(metrics))

# Check viability
trade_cap = compute_trade_count_cap(len(df))
passed, reasons = check_viability_gate(metrics, trade_count_cap=trade_cap)

print("-" * 60)
if passed:
    print("VIABILITY GATE: PASS")
else:
    print("VIABILITY GATE: FAIL")
    for r in reasons:
        print(f"  - {r}")

print("=" * 60)
