# Command Reference — QQQ MA250 Trading Strategy

This document describes the three main ways to run the backtester with **TQQQ** and **SQQQ**, which folds each command runs, and what parameters you can change.

---

## 1. Full history run (single fold, 15 years)

```bash
python run.py --qqq data/qqq_us_d.csv --tqqq data/tqqq_us_d.csv --sqqq data/sqqq_us_d.csv --fold-start 2010-02-11 --fold-years 15
```

**What it does:** Runs one backtest from the first day TQQQ/SQQQ trade (2010-02-11) for **15 years**. Use this when you want a single “full history” run to end of data (adjust `--fold-years` if your data ends earlier or later).

**Folds run:** **One fold** — 2010-02-11 through ~2025-02-11 (or last date in your data if sooner).

**Parameters you can add/override:**

| Parameter | Example | Description |
|-----------|---------|-------------|
| `--out` | `--out results/full_run.csv` | Output CSV path (default: `output/for_graphs/consolidated.csv`) |
| `--initial-capital` | `--initial-capital 50000` | Starting capital (default: 10000) |
| `--ma-confirmation` | `--ma-confirmation` | Require MA50 > MA250 for bull regime |
| `--execution-mode` | `--execution-mode SAME_DAY_CLOSE` | `NEXT_OPEN` (default) or `SAME_DAY_CLOSE` |
| `--vol-targeting` | `--vol-targeting` | Size position by target volatility |
| `--min-weight-change` | `--min-weight-change 0.05` | Min weight change to rebalance (e.g. 5%) |
| `--debug-columns` | `--debug-columns` | Add Cash_Open, Shares_Open, Decision_Price, etc. |

**Example with options:**

```bash
python run.py --qqq data/qqq_us_d.csv --tqqq data/tqqq_us_d.csv --sqqq data/sqqq_us_d.csv --fold-start 2010-02-11 --fold-years 15 --out output/full_15y.csv --initial-capital 50000 --ma-confirmation
```

---

## 2. Default single fold (5 years from TQQQ/SQQQ start)

```bash
python run.py --qqq data/qqq_us_d.csv --tqqq data/tqqq_us_d.csv --sqqq data/sqqq_us_d.csv
```

**What it does:** Runs one backtest with **no** `--fold-start` or `--fold-years`. The script defaults the fold start to the first date in the TQQQ/SQQQ data (**2010-02-11**) and uses **5 years** for the fold length.

**Folds run:** **One fold** — 2010-02-11 through ~2015-02-11 (5 years).

**Parameters you can add/override:** Same as in section 1. You can also override the fold:

| Parameter | Example | Description |
|-----------|---------|-------------|
| `--fold-start` | `--fold-start 2015-01-01` | Start date of backtest (YYYY-MM-DD) |
| `--fold-years` | `--fold-years 10` | Length of backtest in years (default: 5) |

**Example — same as above but 7-year window:**

```bash
python run.py --qqq data/qqq_us_d.csv --tqqq data/tqqq_us_d.csv --sqqq data/sqqq_us_d.csv --fold-years 7
```

---

## 3. Multiple folds (16 fixed windows)

```bash
python scripts/run_multiple_folds.py
```

**What it does:** Runs **16 separate backtests**, each with its own date range. Each fold uses the same strategy (QQQ signal, TQQQ in bull, SQQQ in bear) and writes one CSV under `output/`. The script then prints a summary table (return, max drawdown, worst 20-day, trade count, exposure) and PASS/FAIL per fold.

**Folds run:** All start on or after 2010-02-11 (TQQQ/SQQQ availability). Exact list:

| Fold name | Start date | Years | Date range (approx.) |
|-----------|------------|-------|------------------------|
| 2010-2012 | 2010-02-11 | 3 | 2010–2012 |
| 2010-2014 | 2010-02-11 | 5 | 2010–2014 |
| 2010-2016 | 2010-02-11 | 7 | 2010–2016 |
| 2010-2019 | 2010-02-11 | 10 | 2010–2019 |
| 2010-2023 (full) | 2010-02-11 | 14 | 2010–2023 |
| 2012-2014 | 2012-01-01 | 3 | 2012–2014 |
| 2014-2016 | 2014-01-01 | 3 | 2014–2016 |
| 2016-2018 | 2016-01-01 | 3 | 2016–2018 |
| 2018-2020 | 2018-01-01 | 3 | 2018–2020 |
| 2020-2022 | 2020-01-01 | 3 | 2020–2022 |
| 2022-2024 | 2022-01-01 | 3 | 2022–2024 |
| 2012-2016 | 2012-01-01 | 5 | 2012–2016 |
| 2015-2019 | 2015-01-01 | 5 | 2015–2019 |
| 2018-2022 | 2018-01-01 | 5 | 2018–2022 |
| 2019-2023 | 2019-01-01 | 5 | 2019–2023 |

**Parameters:** This script has **no command-line arguments**. Folds and data paths are fixed in `scripts/run_multiple_folds.py` (it always uses `data/qqq_us_d.csv`, `data/tqqq_us_d.csv`, `data/sqqq_us_d.csv`). To change folds or paths, edit the `FOLDS` list and the `run.py` invocation inside that script.

---

## Quick comparison

| Command | Folds | Window(s) | Output |
|---------|--------|-----------|--------|
| **1** Full history (15y) | 1 | 2010-02-11 → 15 years | One CSV (default or `--out`) |
| **2** Default run | 1 | 2010-02-11 → 5 years | One CSV (default or `--out`) |
| **3** Multiple folds | 16 | See table above | 16 CSVs in `output/` + summary table |

---

## All `run.py` parameters (for commands 1 and 2)

| Option | Required | Default | Description |
|--------|----------|---------|-------------|
| `--qqq` | Yes | — | Path to QQQ CSV (signal source). |
| `--tqqq` | No | — | Path to TQQQ CSV; if set, execution uses TQQQ in bull. |
| `--sqqq` | No | — | Path to SQQQ CSV; if set, execution uses SQQQ in bear. |
| `--out` | No | `output/for_graphs/consolidated.csv` | Output CSV path. |
| `--initial-capital` | No | 10000 | Starting capital. |
| `--fold-start` | No | First valid date* | Fold start date (YYYY-MM-DD). *With TQQQ/SQQQ: first date in exec data (e.g. 2010-02-11). |
| `--fold-years` | No | 5 | Length of backtest window in years. |
| `--ma-confirmation` | No | off | Require MA50 > MA250 for bull (stricter regime). |
| `--execution-mode` | No | NEXT_OPEN | `NEXT_OPEN` or `SAME_DAY_CLOSE`. |
| `--vol-targeting` | No | off | Size position by target volatility. |
| `--min-weight-change` | No | 0 | Min weight change to rebalance (e.g. 0.05 = 5%); 0 = disabled. |
| `--debug-columns` | No | off | Add Cash_Open, Shares_Open, Decision_Price, etc. to output. |

**Help:** Run `python run.py --help` to see all options in the terminal.
