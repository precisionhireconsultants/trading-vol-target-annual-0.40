# QQQ MA250 Trading Strategy

A production-ready trading system that trades QQQ/TQQQ based on a 250-day Moving Average strategy with risk controls.

## Ways to run

**Quick start (single backtest, QQQ only):**
```bash
python run.py --qqq data/qqq_us_d.csv
```

**Single backtest with options:**
```bash
# With TQQQ execution (signal from QQQ, trade TQQQ)
python run.py --qqq data/qqq_us_d.csv --tqqq data/tqqq_us_d.csv

# Trade both TQQQ (bull) and SQQQ (bear) — long TQQQ above MA250, long SQQQ below
python run.py --qqq data/qqq_us_d.csv --tqqq data/tqqq_us_d.csv --sqqq data/sqqq_us_d.csv

# Custom output path
python run.py --qqq data/qqq_us_d.csv --out results.csv

# Custom initial capital
python run.py --qqq data/qqq_us_d.csv --initial-capital 50000

# Custom fold (start date and length)
python run.py --qqq data/qqq_us_d.csv --fold-start 2015-01-01 --fold-years 5

# MA50 confirmation, volatility targeting, execution mode
python run.py --qqq data/qqq_us_d.csv --ma-confirmation --vol-targeting --execution-mode SAME_DAY_CLOSE

# Throttle rebalances (min 5% weight change to trade)
python run.py --qqq data/qqq_us_d.csv --min-weight-change 0.05

# Include debug columns (Cash_Open, Shares_Open, etc.)
python run.py --qqq data/qqq_us_d.csv --debug-columns

# Show all options
python run.py --help
```

**Multiple folds (backtest across many time windows):**
```bash
# From project root; writes one CSV per fold under output/
python scripts/run_multiple_folds.py
```

**Tests:**
```bash
pytest -q                    # Run all tests
pytest -v                    # Verbose
pytest tests/test_regime.py  # Single test file
```

### Config options

**Set via command line (run.py):**

| Option | Default | Description |
|--------|---------|-------------|
| `--qqq` | *(required)* | Path to QQQ CSV (signal source). |
| `--tqqq` | — | Path to TQQQ CSV; if set, execution uses TQQQ in bull. |
| `--sqqq` | — | Path to SQQQ CSV; if set, execution uses SQQQ in bear (long both TQQQ and SQQQ by regime). |
| `--out` | `output/for_graphs/consolidated.csv` | Output CSV path. |
| `--initial-capital` | 10000 | Starting capital. |
| `--fold-start` | first valid date | Fold start date (YYYY-MM-DD). |
| `--fold-years` | 5 | Length of backtest window in years. |
| `--ma-confirmation` | off | Require MA50 > MA250 for bull (stricter regime). |
| `--execution-mode` | NEXT_OPEN | `NEXT_OPEN` (trade next open) or `SAME_DAY_CLOSE` (trade same-day close). |
| `--vol-targeting` | off | Size position by target vol (scale down in high vol). |
| `--min-weight-change` | 0 | Min weight change to rebalance (e.g. 0.05 = 5%); 0 = disabled. |
| `--debug-columns` | off | Add Cash_Open, Shares_Open, Decision_Price, etc. to output. |

**Options in `src/config.py` (edit the file to change; no CLI for these):**

Each option below lives in `TradingConfig` in `src/config.py`. What each one means:

| Option | Default | What it means |
|--------|---------|----------------|
| **Moving averages** | | |
| `MA_SHORT` | 50 | Number of days for the short moving average (MA50). Used for regime confirmation when MA confirmation is on. |
| `MA_LONG` | 250 | Number of days for the long moving average (MA250). Bull = QQQ close ≥ MA250; bear = below. |
| **Volatility** | | |
| `VOL_WINDOW` | 20 | Number of past days used to compute annualized volatility (for indicators and vol targeting). |
| `TRADING_DAYS_PER_YEAR` | 252 | Trading days per year; used to annualize volatility. |
| **Portfolio / fold** | | |
| `INITIAL_CAPITAL` | 1000.0 | Starting portfolio value. Overridden by `--initial-capital` when you run `run.py`. |
| `FOLD_YEARS` | 5 | Default length of the backtest window in years. Overridden by `--fold-years`. |
| **Regime** | | |
| `USE_MA_CONFIRMATION` | False | If True, bull regime also requires MA50 > MA250; otherwise regime is treated as cash. Overridden by `--ma-confirmation`. |
| **Execution timing** | | |
| `EXECUTION_MODE` | "NEXT_OPEN" | When to fill: `NEXT_OPEN` = next day’s open; `SAME_DAY_CLOSE` = same day’s close. Overridden by `--execution-mode`. |
| **Vol targeting** (when `--vol-targeting` is on) | | |
| `USE_VOL_TARGETING` | False | If True, position size = min(vol target / actual vol, max position). Overridden by `--vol-targeting`. |
| `VOL_TARGET_ANNUAL` | 0.25 | Target annualized volatility (e.g. 0.25 = 25%). Position is scaled so estimated vol matches this. |
| `MAX_POSITION_PCT` | 1.0 | Cap on position size (1.0 = 100%). Used with vol targeting so you never exceed this weight. |
| **Trade throttling** | | |
| `MIN_WEIGHT_CHANGE` | 0.05 | Don’t rebalance unless target weight moves by at least this much (e.g. 0.05 = 5%). Overridden by `--min-weight-change`. |
| **Execution instrument** | | |
| `EXEC_SYMBOL` | "QQQ" | Symbol used for execution (QQQ or TQQQ). In practice set by whether you pass `--tqqq`. |
| `USE_SQQQ_IN_BEAR` | True | If True and SQQQ data is provided, hold SQQQ in bear regime instead of going to cash. |
| **Output** | | |
| `DEFAULT_OUTPUT_PATH` | "output/for_graphs/consolidated.csv" | Default path for the output CSV. Overridden by `--out`. |
| **Exposure and rebalance** | | |
| `MAX_EFFECTIVE_EXPOSURE` | 3.0 | Max QQQ-equivalent exposure. 1.0 = 100% QQQ; with 3x TQQQ, max TQQQ weight = 1/3. Caps leverage. |
| `TQQQ_LEVERAGE` | 3.0 | TQQQ’s leverage multiple. Used to convert target exposure into a TQQQ weight (e.g. 100% QQQ → 1/3 TQQQ). With TQQQ/SQQQ, the signal outputs QQQ-equivalent exposure; run.py converts to instrument weight (exposure ÷ leverage) before sizing so portfolio volatility matches the vol target. |
| `REBALANCE_BAND_PCT` | 0.05 | Rebalance only if \|target weight − actual weight\| > this (e.g. 0.05 = 5%). Reduces noise trading. |
| **Execution friction** | | |
| `SLIPPAGE_BPS` | 5.0 | Slippage in basis points (1 bp = 0.01%). Applied to each fill to simulate realistic execution. |
| `COMMISSION_PER_TRADE` | 0.0 | Fixed commission in dollars per trade. Set > 0 to model brokerage costs. |
| **Kill switch (HALT)** | | |
| `MAX_DAILY_LOSS_PCT` | 0.02 | If daily loss (close vs open) ≥ this (e.g. 0.02 = 2%), trigger HALT: flatten position and enter cooldown. |
| `MAX_DRAWDOWN_PCT` | 0.25 | If drawdown from peak equity ≥ this (e.g. 0.25 = 25%), trigger HALT. |
| `HALT_COOLDOWN_DAYS` | 1 | After a HALT, no new entries for this many days; reduce-only allowed. |
| **Debug** | | |
| `DEBUG_COLUMNS` | False | If True, output includes Cash_Open, Shares_Open, Decision_Price, etc. Overridden by `--debug-columns`. |

To change any of these, edit `src/config.py`. Where a CLI flag exists (e.g. `--initial-capital`, `--ma-confirmation`), the command line overrides the config value when you run `run.py`.

---

## Strategy Overview

### Core Rules (Non-Negotiable)

1. **Signal always from QQQ** - regime, indicators, target weight computed from QQQ only
2. **Execution uses TQQQ prices** - fills, valuation, equity calculations use execution instrument
3. **Exposure clamp BEFORE rebalance** - Target_Weight_raw -> clamp -> Target_Weight_clamped
4. **Equity for risk checks** - Use equity_open (marked to open) and equity_close (marked to close)
5. **HALT forces flatten, bypasses ALL rules** - flatten to 0 shares ignores bands, throttles, max trades
6. **Float comparisons use epsilon** - never use `== 0`, always `abs(x) < ZERO_EPS`
7. **Dates normalized everywhere** - use `pd.Timestamp.normalize()` (date-only, no time)

### Regime Detection

- **Bull**: QQQ Close >= MA250
- **Bear**: QQQ Close < MA250
- **Cash**: Insufficient data (MA250 is NaN)

### MA50 Confirmation (Optional)

When `USE_MA_CONFIRMATION=True`:
- Bull regime requires MA50 > MA250 (filters weak rallies)
- If MA50 <= MA250, regime becomes Cash

### Position Sizing

**Without volatility targeting:**
- Bull regime: 100% target weight
- Cash regime: 0% target weight

**With volatility targeting:**
- `target_weight = min(vol_target / actual_vol, max_position)`
- Scales position inversely with volatility

## Installation

```bash
# Create virtual environment
python -m venv .venv

# Activate (Windows)
.\.venv\Scripts\Activate.ps1

# Activate (Linux/Mac)
source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

See **Ways to run** at the top for all run options.

## Running Tests

```bash
# Run all tests
pytest -q

# Run with verbose output
pytest -v

# Run specific test file
pytest tests/test_regime.py -v

# Run specific test class
pytest tests/test_regression.py::TestDeterministicContract -v
```

## Execution Timing

### NEXT_OPEN Mode (Default, Conservative)

- Decision made at day T close using T's signal
- Order executes at day T+1 open
- No look-ahead bias

### SAME_DAY_CLOSE Mode (Malik-style)

- Decision made at day T close using T's signal
- Order executes at day T close (last ~10 min of day)
- Requires real-time execution capability

## Risk Controls

### Exposure Cap

```python
MAX_EFFECTIVE_EXPOSURE = 1.0  # 100% QQQ-equivalent
TQQQ_LEVERAGE = 3.0           # TQQQ is 3x leveraged
# For TQQQ: max_weight = 1.0 / 3.0 = 0.333
```

### Daily Loss Limit

- `MAX_DAILY_LOSS_PCT = 0.02` (2%)
- Triggers HALT if (equity_close / equity_open) - 1 <= -2%

### Drawdown Limit

- `MAX_DRAWDOWN_PCT = 0.25` (25%)
- Triggers HALT if drawdown from peak >= 25%

### Rebalance Band

- `REBALANCE_BAND_PCT = 0.05` (5%)
- Only trade if |target_weight - actual_weight| > band

### Kill Switch Behavior

When HALT triggers:
1. Position flattens to 0 shares (bypasses all rules)
2. Lockout period activates (`HALT_COOLDOWN_DAYS`)
3. No new entries during lockout
4. Reduce-only orders allowed during lockout

## Output Schema (37 Columns)

| Column | Description |
|--------|-------------|
| `Fold_ID` | Fold identifier |
| `Phase` | Backtest phase ("test") |
| `Train_Start` | Training period start |
| `Train_End` | Training period end |
| `Test_Start` | Test period start |
| `Test_End` | Test period end |
| `Date` | Trading date |
| `QQQ_Open` | QQQ open price |
| `QQQ_High` | QQQ high price |
| `QQQ_Low` | QQQ low price |
| `QQQ_Close` | QQQ close price |
| `QQQ_Adj Close` | QQQ adjusted close |
| `QQQ_Volume` | QQQ volume |
| `QQQ_ann_vol` | QQQ annualized volatility (20-day) |
| `MA50` | 50-day moving average |
| `MA250` | 250-day moving average |
| `Base_Regime` | Base regime (cash/bull/bear) |
| `Confirmed_Regime` | Confirmed regime (after MA50 filter) |
| `Final_Trading_Regime` | Final regime for trading (cash/bull) |
| `Target_Weight` | Target allocation weight |
| `Exec_Target_Weight` | Execution target weight (after timing shift) |
| `Portfolio_Value_Open` | Portfolio value at open |
| `Actual_Weight` | Actual stock weight |
| `Exec_Symbol` | Execution instrument (QQQ/TQQQ) |
| `Exec_Open` | Execution instrument open price |
| `Exec_Close` | Execution instrument close price |
| `Target_Shares` | Target shares to hold |
| `Trade_Flag` | Trade indicator (0 or 1) |
| `Trade_Made_Type` | Trade type (BUY/SELL/blank) |
| `Trade_Count` | Number of trades |
| `Net_Shares_Change` | Shares bought/sold |
| `Total_Notional_Abs` | Absolute trade value |
| `Fill_Price_VWAP` | Execution price |
| `Rebalance_Reason_Code` | Reason code (REGIME_SWITCH/REBALANCE/NO_TRADE) |
| `Total_Stocks_Owned` | Shares held after trade |
| `Cash` | Cash balance after trade |
| `Remaining_Portfolio_Amount` | Total portfolio value at close |

### Debug Columns (--debug-columns)

When enabled, additional columns are included:
- `Cash_Open` - Cash at start of day
- `Shares_Open` - Shares at start of day
- `Decision_Price` - Price used for sizing
- `Fill_Price_Source` - Market price before slippage
- `Fill_Price_Effective` - Price after slippage
- `Commission_Applied` - Commission charged

## Configuration Parameters

Edit `src/config.py` to change defaults:

```python
@dataclass(frozen=True)
class TradingConfig:
    # Moving averages
    MA_SHORT: int = 50
    MA_LONG: int = 250
    
    # Volatility
    VOL_WINDOW: int = 20
    TRADING_DAYS_PER_YEAR: int = 252
    
    # Portfolio
    INITIAL_CAPITAL: float = 10000.0
    
    # Execution
    EXECUTION_MODE: str = "NEXT_OPEN"  # or "SAME_DAY_CLOSE"
    EXEC_SYMBOL: str = "QQQ"           # or "TQQQ"
    
    # Risk limits
    MAX_EFFECTIVE_EXPOSURE: float = 1.0
    TQQQ_LEVERAGE: float = 3.0
    MAX_DAILY_LOSS_PCT: float = 0.02
    MAX_DRAWDOWN_PCT: float = 0.25
    HALT_COOLDOWN_DAYS: int = 1
    REBALANCE_BAND_PCT: float = 0.05
    
    # Execution friction
    SLIPPAGE_BPS: float = 5.0
    COMMISSION_PER_TRADE: float = 0.0
    
    # Features
    USE_MA_CONFIRMATION: bool = False
    USE_VOL_TARGETING: bool = False
    USE_SQQQ_IN_BEAR: bool = False
```

## Project Structure

```
trading/
├── src/
│   ├── config.py         # Centralized configuration
│   ├── data_loader.py    # Load and validate CSV data
│   ├── indicators.py     # MA250, MA50, volatility
│   ├── regime.py         # Regime detection and weights
│   ├── fold_selection.py # Sample fold selection
│   ├── portfolio.py      # Portfolio simulation
│   ├── engine.py         # Trading engine, broker, kill switch
│   └── export.py         # CSV export
├── tests/
│   ├── test_data_loader.py
│   ├── test_indicators.py
│   ├── test_regime.py
│   ├── test_fold.py
│   ├── test_portfolio.py
│   ├── test_engine.py
│   ├── test_export.py
│   └── test_regression.py
├── data/
│   ├── qqq_us_d.csv      # QQQ historical data
│   └── tqqq_us_d.csv     # TQQQ historical data
├── output/
│   └── for_graphs/
│       └── consolidated.csv
├── reports/              # Phase implementation reports
├── run.py                # CLI entrypoint
├── requirements.txt
└── README.md
```

## Algorithm Details

### Signal Flow

1. Load QQQ data (signal instrument)
2. Compute MA250, MA50, volatility
3. Determine Base_Regime (Close vs MA250)
4. Apply MA50 confirmation if enabled
5. Compute Target_Weight (with vol targeting if enabled)

### Execution Flow

1. Apply execution timing shift (NEXT_OPEN or SAME_DAY_CLOSE)
2. Clamp weight for leverage exposure
3. Check rebalance band
4. Compute target shares
5. Execute trade with slippage/commission
6. Check risk limits (daily loss, drawdown)
7. Flatten if HALT triggered

### Accounting Identities

```python
# Open state
equity_open = Cash_Open + Shares_Open * Exec_Open

# Close state
equity_close = Cash + Total_Stocks_Owned * Exec_Close
```

## Dependencies

- pandas >= 2.0.0
- numpy >= 1.24.0
- pytest >= 8.0.0
