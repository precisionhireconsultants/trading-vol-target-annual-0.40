# Phase 23 Report: CLI Entrypoint

## Date: 2026-01-30

## Phase Objective
Create run.py with command-line interface.

## Functionalities Implemented

1. **`run.py` CLI Script**
   - `--qqq`: Path to QQQ CSV file (required)
   - `--out`: Output path (default: output/for_graphs/consolidated.csv)
   - `--initial-capital`: Starting capital (default: 10000)

2. **`run_backtest()` Function**
   - Full backtest pipeline
   - Returns summary dict

## CLI Usage

```bash
python run.py --help
python run.py --qqq data/qqq_us_d.csv
python run.py --qqq data/qqq_us_d.csv --out results.csv
python run.py --qqq data/qqq_us_d.csv --initial-capital 50000
```

## Actual Backtest Run

```
============================================================
QQQ MA250 Trading Strategy Backtest
============================================================
Loading data from: data/qqq_us_d.csv
Loaded 6760 rows of data
Selected fold with 1256 rows
  Date range: 2000-03-06 to 2005-03-04

============================================================
RESULTS
============================================================
Initial Capital:    $10,000.00
Final Value:        $8,123.91
Total Return:       -18.76%
Trade Count:        29
Days Simulated:     1256
Output File:        output\for_graphs\consolidated.csv
============================================================
```

## Test Results

```
pytest -q
135 passed in 1.33s
```

**Total Tests: 135 passed**

## Notes

The negative return (-18.76%) reflects the dot-com bust period (2000-2005) in the test fold. The MA250 strategy correctly:
- Entered positions when price was above MA250
- Exited when price fell below MA250
- Made 29 trades over 1256 trading days

---
**Phase 23 Status: COMPLETE**
