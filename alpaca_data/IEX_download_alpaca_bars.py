import argparse
import os
import json
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

try:
    from dotenv import load_dotenv
except ImportError:
    load_dotenv = None  # optional; only needed for download (API keys)


# -----------------------------
# Config
# -----------------------------
DEFAULT_SYMBOLS = ["QQQ", "TQQQ", "SQQQ"]
EARLIEST_MINUTE_UTC = datetime(2016, 1, 1, tzinfo=timezone.utc)  # Alpaca minute history starts ~2016
PAGE_LIMIT = 10_000  # Alpaca bars endpoint limit per request


@dataclass(frozen=True)
class DownloadConfig:
    symbols: List[str]
    timeframe: Any  # TimeFrame when used from download path
    start_utc: datetime
    end_utc: datetime
    out_dir: Path
    state_path: Path
    max_retries: int = 6
    base_sleep_s: float = 1.0
    feed: Any = None  # DataFeed.IEX when used from download path


# -----------------------------
# Helpers
# -----------------------------
def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def ensure_dirs(*paths: Path) -> None:
    for p in paths:
        p.mkdir(parents=True, exist_ok=True)


def load_state(state_path: Path) -> Dict[str, str]:
    if state_path.exists():
        return json.loads(state_path.read_text())
    return {}


def save_state(state_path: Path, state: Dict[str, str]) -> None:
    state_path.write_text(json.dumps(state, indent=2, sort_keys=True))


def parquet_path(out_dir: Path, symbol: str, tf_label: str) -> Path:
    return out_dir / f"{symbol}_{tf_label}.parquet"


def csv_path(out_dir: Path, symbol: str, tf_label: str) -> Path:
    return out_dir / f"{symbol}_{tf_label}.csv"


def tf_label(tf: TimeFrame) -> str:
    unit = tf.unit.value if hasattr(tf.unit, "value") else str(tf.unit)
    return f"{tf.amount}{unit}".lower()  # e.g., "1min", "5min"


def backoff_sleep(attempt: int, base: float) -> None:
    sleep_s = min(base * (2 ** attempt), 60.0)
    time.sleep(sleep_s)


def normalize_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize Alpaca bars DF to columns:
      timestamp, symbol, open, high, low, close, volume, trade_count, vwap
    """
    if df.empty:
        return df

    df2 = df.copy()

    # MultiIndex commonly: (symbol, timestamp)
    df2 = df2.reset_index()

    if "timestamp" not in df2.columns:
        if "index" in df2.columns:
            df2 = df2.rename(columns={"index": "timestamp"})

    df2["timestamp"] = pd.to_datetime(df2["timestamp"], utc=True)

    if "symbol" not in df2.columns:
        for alt in ["symbols", "ticker"]:
            if alt in df2.columns:
                df2 = df2.rename(columns={alt: "symbol"})
                break

    cols = ["timestamp", "symbol", "open", "high", "low", "close", "volume", "trade_count", "vwap"]
    keep = [c for c in cols if c in df2.columns]
    df2 = df2[keep].sort_values(["symbol", "timestamp"]).drop_duplicates(["symbol", "timestamp"])
    return df2


def append_parquet(path: Path, new_df: pd.DataFrame) -> None:
    if new_df.empty:
        return

    if path.exists():
        old = pd.read_parquet(path)
        combined = pd.concat([old, new_df], ignore_index=True)
        combined = combined.drop_duplicates(["symbol", "timestamp"]).sort_values(["symbol", "timestamp"])
        combined.to_parquet(path, index=False)
    else:
        new_df.to_parquet(path, index=False)


def get_last_timestamp(path: Path, symbol: str) -> Optional[datetime]:
    if not path.exists():
        return None
    df = pd.read_parquet(path, columns=["symbol", "timestamp"])
    df = df[df["symbol"] == symbol]
    if df.empty:
        return None
    ts = pd.to_datetime(df["timestamp"], utc=True).max()
    return ts.to_pydatetime().replace(tzinfo=timezone.utc)


# -----------------------------
# Core download
# -----------------------------
def fetch_page(
    client: Any,
    symbols: List[str],
    tf: Any,
    start_utc: datetime,
    end_utc: datetime,
    feed: Any,
    limit: int = PAGE_LIMIT,
) -> pd.DataFrame:
    from alpaca.data.requests import StockBarsRequest

    req = StockBarsRequest(
        symbol_or_symbols=symbols,
        timeframe=tf,
        start=start_utc,
        end=end_utc,
        limit=limit,
        feed=feed,  # <-- NEW: force IEX on free tier
    )
    bars = client.get_stock_bars(req).df
    return normalize_df(bars)


def download_minutes_max_history(cfg: DownloadConfig) -> None:
    from alpaca.data.historical import StockHistoricalDataClient

    # Load .env next to this script (alpaca_data/.env) so it works when run from project root
    if load_dotenv is not None:
        load_dotenv(Path(__file__).resolve().parent / ".env")

    api_key = os.getenv("ALPACA_API_KEY")
    secret_key = os.getenv("ALPACA_SECRET_KEY")

    if not api_key or not secret_key:
        raise RuntimeError("Missing ALPACA_API_KEY / ALPACA_SECRET_KEY in environment or .env")

    ensure_dirs(cfg.out_dir, cfg.state_path.parent)

    client = StockHistoricalDataClient(api_key, secret_key)

    state = load_state(cfg.state_path)
    label = tf_label(cfg.timeframe)

    paths = {s: parquet_path(cfg.out_dir, s, label) for s in cfg.symbols}

    cursors: Dict[str, datetime] = {}
    for s in cfg.symbols:
        if s in state:
            cursors[s] = datetime.fromisoformat(state[s]).astimezone(timezone.utc)
        else:
            last_ts = get_last_timestamp(paths[s], s)
            cursors[s] = last_ts if last_ts else cfg.start_utc

    print(f"Downloading {label} bars for {cfg.symbols}")
    print(f"Feed: {cfg.feed}")
    print(f"Range: {cfg.start_utc.isoformat()} -> {cfg.end_utc.isoformat()}")
    print(f"Output: {cfg.out_dir}")
    print(f"State:  {cfg.state_path}")

    done = {s: False for s in cfg.symbols}

    while not all(done.values()):
        active = [s for s in cfg.symbols if not done[s]]
        min_symbol = min(active, key=lambda s: cursors[s])
        window_start = cursors[min_symbol]

        if window_start >= cfg.end_utc:
            done[min_symbol] = True
            continue

        symbols = [min_symbol]

        page_df = pd.DataFrame()
        for attempt in range(cfg.max_retries):
            try:
                page_df = fetch_page(
                    client=client,
                    symbols=symbols,
                    tf=cfg.timeframe,
                    start_utc=window_start,
                    end_utc=cfg.end_utc,
                    feed=cfg.feed,
                    limit=PAGE_LIMIT,
                )
                break
            except Exception as e:
                print(f"[WARN] fetch failed ({min_symbol}) attempt {attempt+1}/{cfg.max_retries}: {e}")
                backoff_sleep(attempt, cfg.base_sleep_s)

        if page_df.empty:
            done[min_symbol] = True
            state[min_symbol] = cfg.end_utc.isoformat()
            save_state(cfg.state_path, state)
            print(f"[OK] {min_symbol}: no more data, done.")
            continue

        append_parquet(paths[min_symbol], page_df)

        last_ts = page_df["timestamp"].max().to_pydatetime().replace(tzinfo=timezone.utc)
        cursors[min_symbol] = last_ts + pd.Timedelta(minutes=1)

        state[min_symbol] = cursors[min_symbol].isoformat()
        save_state(cfg.state_path, state)

        print(f"[OK] {min_symbol}: +{len(page_df)} rows, now at {cursors[min_symbol].isoformat()}")

        time.sleep(0.25)

    print("\nDone.")
    for s in cfg.symbols:
        p = paths[s]
        if p.exists():
            full_df = pd.read_parquet(p)
            sub = full_df[full_df["symbol"] == s]
            if not sub.empty:
                print(f"{s}: {len(sub):,} rows, {sub['timestamp'].min()} -> {sub['timestamp'].max()}")
            full_df.to_csv(csv_path(cfg.out_dir, s, label), index=False)
            print(f"  -> {csv_path(cfg.out_dir, s, label).name}")


def make_5min_from_1min(in_path: Path, out_path: Path) -> None:
    """
    Resample per symbol to 5-minute bars in UTC.
    If you want RTH-only (9:30-16:00 ET) resampling, tell me and I’ll add the session filter.
    """
    df = pd.read_parquet(in_path)
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)

    out = []
    for sym, g in df.groupby("symbol"):
        g = g.sort_values("timestamp").set_index("timestamp")
        agg = g.resample("5min").agg(
            open=("open", "first"),
            high=("high", "max"),
            low=("low", "min"),
            close=("close", "last"),
            volume=("volume", "sum"),
        ).dropna()
        agg = agg.reset_index()
        agg["symbol"] = sym
        out.append(agg)

    out_df = pd.concat(out, ignore_index=True).sort_values(["symbol", "timestamp"])
    out_df.to_parquet(out_path, index=False)
    # Also write 5-min CSV alongside parquet
    csv_path_5 = out_path.with_suffix(".csv")
    out_df.to_csv(csv_path_5, index=False)


# Column names matching data/ CSV format (Date, Open, High, Low, Close, Volume)
DAILY_CSV_COLUMNS = ["Date", "Open", "High", "Low", "Close", "Volume"]
NY_TZ = "America/New_York"
RTH_START = (9, 30)   # 9:30 ET
RTH_END = (16, 0)     # 16:00 ET (inclusive)


def aggregate_1min_to_daily(in_path: Path, out_path: Path, symbol: str) -> int:
    """
    Aggregate 1-min parquet to daily bars and write CSV with columns matching
    data/ CSVs: Date, Open, High, Low, Close, Volume (for use in BACKTEST_MODE="daily").
    Uses America/New_York for date; RTH 9:30–16:00 ET.
    Returns number of daily rows written.
    """
    df = pd.read_parquet(in_path)
    if df.empty:
        return 0
    df = df[df["symbol"] == symbol].copy()
    if df.empty:
        return 0

    ts = pd.to_datetime(df["timestamp"], utc=True)
    df["_ts_ny"] = ts.dt.tz_convert(NY_TZ)
    df["_date"] = df["_ts_ny"].dt.date

    # RTH filter: 9:30–16:00 ET inclusive
    t = df["_ts_ny"].dt.time
    rth_start = pd.Timestamp("09:30").time()
    rth_end = pd.Timestamp("16:00").time()
    mask = (t >= rth_start) & (t <= rth_end)
    df = df.loc[mask]

    agg = df.groupby("_date", as_index=False).agg(
        Open=("open", "first"),
        High=("high", "max"),
        Low=("low", "min"),
        Close=("close", "last"),
        Volume=("volume", "sum"),
    )
    agg = agg.rename(columns={"_date": "Date"})
    agg["Date"] = pd.to_datetime(agg["Date"])
    agg = agg[DAILY_CSV_COLUMNS].sort_values("Date").reset_index(drop=True)
    agg.to_csv(out_path, index=False)
    return len(agg)


def build_daily_csvs(out_dir: Path, symbols: List[str]) -> None:
    """
    Build daily CSVs from 1-min parquet for each symbol.
    Output: {symbol}_daily.csv with columns Date, Open, High, Low, Close, Volume
    (matches data/ CSV format for BACKTEST_MODE="daily" using Alpaca data).
    """
    label_1m = "1min"
    for sym in symbols:
        in_p = parquet_path(out_dir, sym, label_1m)
        out_p = out_dir / f"{sym}_daily.csv"
        if not in_p.exists():
            print(f"[SKIP] {in_p.name} not found, cannot build {out_p.name}")
            continue
        n = aggregate_1min_to_daily(in_p, out_p, sym)
        print(f"[OK] {sym}: {out_p.name} ({n:,} rows)")


def convert_parquet_to_csv(out_dir: Path, symbols: List[str]) -> None:
    """
    Convert existing 1min and 5min parquet files to CSV for the given symbols.
    Skips missing parquet files.
    """
    label_1m = "1min"
    for sym in symbols:
        for label in (label_1m, "5min"):
            p = parquet_path(out_dir, sym, label) if label == label_1m else out_dir / f"{sym}_5min.parquet"
            if not p.exists():
                print(f"[SKIP] {p.name} not found")
                continue
            df = pd.read_parquet(p)
            csv_p = p.with_suffix(".csv")
            df.to_csv(csv_p, index=False)
            print(f"[OK] {sym} {label}: {csv_p.name} ({len(df):,} rows)")


if __name__ == "__main__":
    script_dir = Path(__file__).resolve().parent
    out_dir = script_dir / "data" / "alpaca"

    parser = argparse.ArgumentParser(description="Download IEX bars and/or convert parquet to CSV")
    parser.add_argument(
        "--csv-only",
        action="store_true",
        help="Only convert existing parquet files to CSV (1min and 5min) for QQQ, TQQQ, SQQQ",
    )
    parser.add_argument(
        "--build-daily",
        action="store_true",
        help="Build daily CSVs (Date, Open, High, Low, Close, Volume) from 1min parquet for BACKTEST_MODE=daily",
    )
    args = parser.parse_args()

    if args.csv_only:
        print("Converting parquet -> CSV for 1min and 5min...")
        convert_parquet_to_csv(out_dir, DEFAULT_SYMBOLS)
        print("Done.")
        raise SystemExit(0)

    if args.build_daily:
        print("Building daily CSVs from 1min parquet (columns: Date, Open, High, Low, Close, Volume)...")
        build_daily_csvs(out_dir, DEFAULT_SYMBOLS)
        print("Done.")
        raise SystemExit(0)

    # Safer end time for free-tier during market hours: avoid most-recent window.
    # If you want "up to now", set end_utc=utcnow().
    from alpaca.data.enums import DataFeed
    from alpaca.data.timeframe import TimeFrame, TimeFrameUnit

    end_utc = utcnow() - pd.Timedelta(minutes=20)
    state_path = script_dir / "data" / "state" / "alpaca_iex_minute_state.json"

    cfg = DownloadConfig(
        symbols=DEFAULT_SYMBOLS,
        timeframe=TimeFrame(1, TimeFrameUnit.Minute),
        start_utc=EARLIEST_MINUTE_UTC,
        end_utc=end_utc,
        out_dir=out_dir,
        state_path=state_path,
        feed=DataFeed.IEX,  # force IEX on free tier
    )

    download_minutes_max_history(cfg)

    label_1m = tf_label(cfg.timeframe)
    for sym in cfg.symbols:
        in_p = parquet_path(out_dir, sym, label_1m)
        out_p = out_dir / f"{sym}_5min.parquet"
        if in_p.exists():
            print(f"Building 5-min for {sym}...")
            make_5min_from_1min(in_p, out_p)
            print(f"[OK] {out_p}")

    print("Building daily CSVs (for BACKTEST_MODE=daily)...")
    build_daily_csvs(out_dir, cfg.symbols)