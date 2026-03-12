#!/usr/bin/env python3
"""Sweep VOL_TARGET_ANNUAL from 0.05 to 1.0 in 0.05 steps and report total return %."""
import sys
import io
from pathlib import Path

# Add project root and src for imports
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "src"))

from run import run_backtest

def main():
    qqq_path = str(ROOT / "data" / "qqq_us_d.csv")
    tqqq_path = str(ROOT / "data" / "tqqq_us_d.csv")
    out_dir = ROOT / "output"
    out_dir.mkdir(parents=True, exist_ok=True)

    results = []
    for v in [round(0.05 + i * 0.05, 2) for i in range(20)]:  # 0.05, 0.10, ..., 1.00
        output_path = str(out_dir / f"sweep_vol_{v:.2f}.csv")
        old_stdout = sys.stdout
        sys.stdout = io.StringIO()
        try:
            summary = run_backtest(
                qqq_path,
                output_path,
                initial_capital=10_000.0,
                fold_start="2010-02-11",
                fold_years=15,
                use_vol_targeting=True,
                vol_target=v,
                min_weight_change=0.05,
                tqqq_path=tqqq_path,
            )
        finally:
            sys.stdout = old_stdout
        results.append((v, summary["total_return_pct"]))

    print("\nVOL_TARGET_ANNUAL | Total return %")
    print("-" * 35)
    for vol, ret in results:
        print(f"       {vol:.2f}       |  {ret:+.2f}%")
    print("-" * 35)
    return results

if __name__ == "__main__":
    main()
