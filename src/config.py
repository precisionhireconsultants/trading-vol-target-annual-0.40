"""Centralized configuration for trading strategy."""
from dataclasses import dataclass


@dataclass(frozen=True)
class TradingConfig:
    """Configuration parameters for the trading strategy.
    
    All strategy constants are centralized here to prevent hardcoding creep
    and make future changes easier without touching logic code.
    """

    # -------------------------------------------------------------------------
    # Volatility targeting
    # -------------------------------------------------------------------------
    # USE_VOL_TARGETING: bool. When False, position size is 100% whenever the
    #   regime is bull (or full short in bear if enabled) – i.e., no attempt
    #   to control portfolio volatility. When True, the strategy treats
    #   `VOL_TARGET_ANNUAL` as a desired annualized volatility for QQQ‑equivalent
    #   exposure and scales position size down when realized QQQ vol is high
    #   (classic “vol targeting” / risk-parity style sizing).
    #   Examples: False = always 100% in bull/cash/bear; True = scale down when vol is high.
    USE_VOL_TARGETING: bool = True
    #   VOL_TARGET_ANNUAL: float. Target annualized volatility for the QQQ‑equivalent exposure (e.g. 0.25 = 25% annualized). 
    #   In trading terms, this is the risk budget: the strategy sizes exposure so that (exposure × QQQ vol) is roughly this value. 
    #   Higher numbers mean accepting more volatility (and potentially higher returns and drawdowns); lower numbers mean a smoother 
    #   but lower‑return equity curve. Examples: 0.20 = lower target, smaller size in vol spikes (more conservative); 0.30 = more aggressive 
    #   risk/return profile.
    VOL_TARGET_ANNUAL: float = 0.40
    # MAX_POSITION_PCT: float. Cap on QQQ‑equivalent position size (1.0 = 100%
    #   of equity in unlevered QQQ exposure). With LETFs, this is interpreted
    #   in exposure terms, not raw TQQQ weight: a value of 3.0 means “allow up
    #   to 300% QQQ‑equivalent exposure”, which corresponds to 100% of the
    #   portfolio in a 3x product like TQQQ. In trading terms this is your
    #   maximum gross long (or short) exposure regardless of how low vol gets.
    #   Examples: 1.0 = never lever more than 1×; 0.5 = max 50% QQQ exposure;
    #             3.0 = allow full 3× exposure (e.g., 100% TQQQ).
    MAX_POSITION_PCT: float = 3.0

    # -------------------------------------------------------------------------
    # Exposure cap (leverage limit)
    # -------------------------------------------------------------------------
    # MAX_EFFECTIVE_EXPOSURE: float. Hard clamp on *realized* QQQ‑equivalent
    #   exposure after converting instrument weight by its leverage. For a 3x
    #   product like TQQQ, effective_exposure = |weight| × 3.0. This guardrail
    #   ensures the strategy never runs beyond this leverage level, even if
    #   `MAX_POSITION_PCT` or vol targeting would suggest a larger position.
    #   Examples: 3.0 = allow up to 3× QQQ‑equivalent (e.g. 100% TQQQ);
    #             2.0 = cap at 2×; 1.0 = strictly unlevered exposure only.
    #   Note: This is the maximum QQQ-equivalent exposure, not the maximum TQQQ weight.   HARD BREAKING!!!
    MAX_EFFECTIVE_EXPOSURE: float = 3.0
    # TQQQ_LEVERAGE: float. Declared leverage multiple of the long LETF used
    #   for execution in bull (and SQQQ in bear). This is the factor used to
    #   translate between QQQ‑equivalent exposure and actual fund weight:
    #   TQQQ_weight ≈ QQQ_exposure / TQQQ_LEVERAGE. In trading terms, if
    #   exposure = 1.0 (100% QQQ) and TQQQ_LEVERAGE = 3.0, the engine will
    #   size roughly 33% of the portfolio in TQQQ.
    #   Examples: 3.0 = 3x LETF like TQQQ/SQQQ; 2.0 for a 2x product; 1.0 if
    #             you are executing directly in QQQ.
    TQQQ_LEVERAGE: float = 3.0

    # -------------------------------------------------------------------------
    # Regime confirmation
    # -------------------------------------------------------------------------
    # USE_MA_CONFIRMATION: bool. When False, bull = Close >= MA250 only.
    #   When True, bull also requires MA50 > MA250 (stricter; else treated as cash).
    #   Examples: False = more time in market, more trades; True = fewer weak rallies, fewer trades.
    USE_MA_CONFIRMATION: bool = True

    # -------------------------------------------------------------------------
    # Execution timing
    # -------------------------------------------------------------------------
    # EXECUTION_MODE: str. When signals are filled.
    #   "NEXT_OPEN" = decide at prior close, fill at next day open (no look-ahead; often worse fills after gaps).
    #   "SAME_DAY_CLOSE" = decide and fill at same day close (Malik-style; typically better backtest fills).
    #   Examples: NEXT_OPEN = more conservative; SAME_DAY_CLOSE = same-day execution.
    EXECUTION_MODE: str = "NEXT_OPEN"

    # -------------------------------------------------------------------------
    # Moving average windows
    # -------------------------------------------------------------------------
    # MA_SHORT: int. Short MA for regime confirmation (used when USE_MA_CONFIRMATION=True).
    #   Examples: 50 = standard MA50; 20 = faster, more signals; 100 = slower, fewer whipsaws.
    MA_SHORT: int = 50
    # MA_LONG: int. Long MA for bull/bear (Close >= MA_LONG = bull).
    #   Examples: 250 = ~1 year, standard; 200 = slightly faster; 300 = slower regime filter.
    MA_LONG: int = 250

    # -------------------------------------------------------------------------
    # Volatility parameters
    # -------------------------------------------------------------------------
    # VOL_WINDOW: int. Past days used to compute annualized vol (for vol targeting).
    #   Examples: 20 = ~1 month; 10 = more reactive; 60 = smoother.
    VOL_WINDOW: int = 20
    # TRADING_DAYS_PER_YEAR: int. Used to annualize volatility.
    #   Examples: 252 = US equities; 365 = calendar days.
    TRADING_DAYS_PER_YEAR: int = 252

    # -------------------------------------------------------------------------
    # Portfolio parameters
    # -------------------------------------------------------------------------
    # INITIAL_CAPITAL: float. Starting capital in dollars. Overridden by --initial-capital.
    #   Examples: 1000 = small account; 10000 = typical backtest; 100000 = larger sizing.
    INITIAL_CAPITAL: float = 100000.0

    # -------------------------------------------------------------------------
    # Fold selection
    # -------------------------------------------------------------------------
    # FOLD_YEARS: int. Default backtest length in years. Overridden by --fold-years.
    #   Examples: 5 = 5-year fold; 10 = decade; 15 = long-term.
    FOLD_YEARS: int = 5

    # -------------------------------------------------------------------------
    # Trade throttling
    # -------------------------------------------------------------------------
    # MIN_WEIGHT_CHANGE: float. Only rebalance if target weight moves by at least this (0 = disabled).
    #   Examples: 0 = rebalance every signal; 0.05 = 5%, fewer trades; 0.10 = 10%, even fewer.
    MIN_WEIGHT_CHANGE: float = 0.05

    # -------------------------------------------------------------------------
    # Execution instrument (overridden by run.py when --tqqq/--sqqq passed)
    # -------------------------------------------------------------------------
    # EXEC_SYMBOL: str. Which instrument is used for execution in bull.
    #   Examples: "QQQ" = unleveraged; "TQQQ" = 3x long (when --tqqq path provided).
    EXEC_SYMBOL: str = "QQQ"
    # -------------------------------------------------------------------------
    # Bear regime handling
    # -------------------------------------------------------------------------
    # USE_SQQQ_IN_BEAR: bool. In bear regime: hold SQQQ (if --sqqq provided) vs go to cash.
    #   Examples: True = short exposure in bear; False = cash in bear.
    USE_SQQQ_IN_BEAR: bool = False

    # -------------------------------------------------------------------------
    # Output
    # -------------------------------------------------------------------------
    # DEFAULT_OUTPUT_PATH: str. Default CSV path. Overridden by --out.
    #   Examples: "output/for_graphs/consolidated.csv"; "results/backtest.csv".
    DEFAULT_OUTPUT_PATH: str = "output/for_graphs/consolidated.csv"



    # -------------------------------------------------------------------------
    # Rebalance band
    # -------------------------------------------------------------------------
    # REBALANCE_BAND_PCT: float. Only rebalance if |target_weight - actual_weight| > this.
    #   Examples: 0.05 = 5% band; 0 = rebalance every day; 0.10 = 10% band, fewer trades.
    REBALANCE_BAND_PCT: float = 0.05

    # -------------------------------------------------------------------------
    # Execution friction
    # -------------------------------------------------------------------------
    # SLIPPAGE_BPS: float. Slippage in basis points (1 bp = 0.01%). Applied per fill.
    #   Examples: 5 = 0.05% per trade; 0 = no slippage; 10 = 0.10% more conservative.
    SLIPPAGE_BPS: float = 5.0
    # COMMISSION_PER_TRADE: float. Fixed commission in dollars per trade.
    #   Examples: 0 = none; 1.0 = $1/trade; 5.0 = $5/trade.
    COMMISSION_PER_TRADE: float = 0.0

    # -------------------------------------------------------------------------
    # Risk limits (kill switch / HALT)
    # -------------------------------------------------------------------------
    # MAX_DAILY_LOSS_PCT: float. If daily loss >= this, trigger HALT (flatten, cooldown).
    #   Examples: 0.02 = 2%; 0.05 = 5% daily loss limit; 0.10 = 10%.
    MAX_DAILY_LOSS_PCT: float = 0.02
    # MAX_DRAWDOWN_PCT: float. If drawdown from peak >= this, trigger HALT.
    #   Examples: 0.25 = 25%; 0.10 = strict; 0.50 = lenient.
    MAX_DRAWDOWN_PCT: float = 0.25
    # HALT_COOLDOWN_DAYS: int. After HALT, no new entries for this many days (reduce-only allowed).
    #   Examples: 1 = one day; 5 = week; 0 = can re-enter same day (if logic allows).
    HALT_COOLDOWN_DAYS: int = 1

    # -------------------------------------------------------------------------
    # Backtest mode
    # -------------------------------------------------------------------------
    # BACKTEST_MODE: str. Pipeline selection.
    #   "daily"    = load daily CSVs from DATA_ROOT; use EXECUTION_MODE for timing.
    #   "intraday" = load 1-min parquet from ALPACA_INTRADAY_DIR; aggregate to
    #                daily Signal_Price / Exec_Price / Close; near-close execution.
    BACKTEST_MODE: str = "daily"

    # -------------------------------------------------------------------------
    # Data paths (BACKTEST_MODE = "daily")
    # -------------------------------------------------------------------------
    # DATA_ROOT: str. Root directory for daily CSV data. Paths are built as
    #   DATA_ROOT / DAILY_QQQ_FILE, DATA_ROOT / DAILY_TQQQ_FILE, etc.
    #   Use "data" for legacy qqq_us_d.csv; use "alpaca_data/data/alpaca" for
    #   Alpaca-derived QQQ_daily.csv (same source as intraday).
    DATA_ROOT: str = "alpaca_data/data/alpaca"
    # DATA_ROOT: str = "data"
    # Daily CSV filenames under DATA_ROOT. Match your data source:
    #   Legacy data/: qqq_us_d.csv, tqqq_us_d.csv, sqqq_us_d.csv
    #   Alpaca daily: QQQ_daily.csv, TQQQ_daily.csv, SQQQ_daily.csv
    DAILY_QQQ_FILE: str = "QQQ_daily.csv"
    DAILY_TQQQ_FILE: str = "TQQQ_daily.csv"
    DAILY_SQQQ_FILE: str = "SQQQ_daily.csv"
    # DAILY_QQQ_FILE: str = "qqq_us_d.csv"
    # DAILY_TQQQ_FILE: str = "tqqq_us_d.csv"
    # DAILY_SQQQ_FILE: str = "sqqq_us_d.csv"
    # ALPACA_INTRADAY_DIR: str. Directory containing 1-min parquet files when
    #   BACKTEST_MODE = "intraday". Files: QQQ_1min.parquet, TQQQ_1min.parquet, …
    ALPACA_INTRADAY_DIR: str = "alpaca_data/data/alpaca"

    # -------------------------------------------------------------------------
    # Intraday offsets (only used when BACKTEST_MODE = "intraday")
    # -------------------------------------------------------------------------
    # SIGNAL_OFFSET_BARS: int. Nth bar from the last RTH minute (15:59 ET) used
    #   as Signal_Price for regime / sizing. 10 → 15:50 ET.
    SIGNAL_OFFSET_BARS: int = 10
    # EXEC_OFFSET_BARS: int. Nth bar from the last RTH minute used as Exec_Price
    #   for order fill. 2 → 15:58 ET.
    EXEC_OFFSET_BARS: int = 2

    # -------------------------------------------------------------------------
    # Debug output
    # -------------------------------------------------------------------------
    # DEBUG_COLUMNS: bool. When True, add Cash_Open, Shares_Open, Decision_Price, etc. to CSV. Overridden by --debug-columns.
    #   Examples: False = minimal output; True = extra columns for debugging.
    DEBUG_COLUMNS: bool = False


# Float comparison epsilon. Use abs(x) < ZERO_EPS instead of x == 0 for floats.
ZERO_EPS = 1e-6

# Floor epsilon for target_shares (avoids div-by-zero in price denominators).
FLOOR_EPS = 1e-12

# CSV float format for deterministic output (e.g. "%.10f" for 10 decimal places).
CSV_FLOAT_FORMAT = "%.10f"


# Default instance used throughout the codebase
DEFAULT_CONFIG = TradingConfig()
