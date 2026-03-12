import os
import json
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
from dotenv import load_dotenv

from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame, TimeFrameUnit

# -----------------------------
# Config
# -----------------------------
DEFAULT_SYMBOLS = ["QQQ", "TQQQ", "SQQQ"]
EARLIEST_MINUTE_UTC = datetime(2016, 1, 1, tzinfo=timezone.utc)  # Alpaca minute history starts ~2016
PAGE_LIMIT = 10_000  # Alpaca bars endpoint limit per request


@dataclass(frozen=True)
class DownloadConfig:
    symbols: List[str]
    timeframe: TimeFrame
    start_utc: datetime
    end_utc: datetime
    out_dir: Path
    state_path: Path
    max_retries: int = 6
    base_sleep_s: float = 1.0


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


def tf_label(tf: TimeFrame) -> str:
    # alpaca-py TimeFrame stores amount+unit
    unit = tf.unit.value if hasattr(tf.unit, "value") else str(tf.unit)
    return f"{tf.amount}{unit}".lower()  # e.g., "1min", "5min"


def backoff_sleep(attempt: int, base: float) -> None:
    # exponential with cap
    sleep_s = min(base * (2 ** attempt), 60.0)
    time.sleep(sleep_s)


def normalize_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Alpaca returns multi-index df often:
      index: timestamp
      columns include 'symbol' or symbol in index level.
    We normalize to columns:
      timestamp_utc, symbol, open, high, low, close, volume, trade_count, vwap
    """
    if df.empty:
        return df

    # alpaca-py bars df often indexed by ['symbol', 'timestamp'] or just timestamp with symbol column
    df2 = df.copy()

    # If symbol is in index level, bring it out
    if isinstance(df2.index, pd.MultiIndex):
        df2 = df2.reset_index()
    else:
        df2 = df2.reset_index()

    # Standardize timestamp column name
    # alpaca-py tends to use 'timestamp'
    if "timestamp" not in df2.columns:
        # sometimes index reset yields 'index'
        if "index" in df2.columns:
            df2 = df2.rename(columns={"index": "timestamp"})

    # Ensure tz-aware UTC
    df2["timestamp"] = pd.to_datetime(df2["timestamp"], utc=True)

    # Some versions use 'symbol' column after reset_index, ensure it exists
    if "symbol" not in df2.columns:
        # If symbol is named differently, try common alternatives
        for alt in ["symbols", "ticker"]:
            if alt in df2.columns:
                df2 = df2.rename(columns={alt: "symbol"})
                break

    # Keep a stable column order if present
    cols = ["timestamp", "symbol", "open", "high", "low", "close", "volume", "trade_count", "vwap"]
    keep = [c for c in cols if c in df2.columns]
    df2 = df2[keep].sort_values(["symbol", "timestamp"]).drop_duplicates(["symbol", "timestamp"])
    return df2


def append_parquet(path: Path, new_df: pd.DataFrame) -> None:
    """
    Append by reading existing and de-duping.
    For large datasets, you might prefer partitioned parquet,
    but this is robust + simple.
    """
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
    # Add 1 minute so we don't re-fetch the last bar forever
    return (ts.to_pydatetime().replace(tzinfo=timezone.utc))


# -----------------------------
# Core download
# -----------------------------
def fetch_page(
    client: StockHistoricalDataClient,
    symbols: List[str],
    tf: TimeFrame,
    start_utc: datetime,
    end_utc: datetime,
    limit: int = PAGE_LIMIT,
) -> pd.DataFrame:
    req = StockBarsRequest(
        symbol_or_symbols=symbols,
        timeframe=tf,
        start=start_utc,
        end=end_utc,
        limit=limit,
    )
    bars = client.get_stock_bars(req).df
    return normalize_df(bars)


def download_minutes_max_history(cfg: DownloadConfig) -> None:
    load_dotenv(Path(__file__).resolve().parent / ".env")

    api_key = os.getenv("ALPACA_API_KEY")
    secret_key = os.getenv("ALPACA_SECRET_KEY")

    if not api_key or not secret_key:
        raise RuntimeError("Missing ALPACA_API_KEY / ALPACA_SECRET_KEY in environment or .env")

    ensure_dirs(cfg.out_dir, cfg.state_path.parent)

    client = StockHistoricalDataClient(api_key, secret_key)

    state = load_state(cfg.state_path)
    label = tf_label(cfg.timeframe)

    # One parquet per symbol for simplicity
    paths = {s: parquet_path(cfg.out_dir, s, label) for s in cfg.symbols}

    # Initialize per-symbol cursors (resume)
    cursors: Dict[str, datetime] = {}
    for s in cfg.symbols:
        if s in state:
            cursors[s] = datetime.fromisoformat(state[s]).astimezone(timezone.utc)
        else:
            last_ts = get_last_timestamp(paths[s], s)
            cursors[s] = (last_ts if last_ts else cfg.start_utc)

    # Download loop: we request pages by time windows, advancing cursors per symbol.
    # Alpaca paginates with 'limit'. We advance by using the max timestamp received per symbol.
    print(f"Downloading {label} bars for {cfg.symbols}")
    print(f"Range: {cfg.start_utc.isoformat()} -> {cfg.end_utc.isoformat()}")
    print(f"Output: {cfg.out_dir}")
    print(f"State:  {cfg.state_path}")

    done = {s: False for s in cfg.symbols}

    while not all(done.values()):
        # Choose the earliest cursor among symbols still downloading
        active = [s for s in cfg.symbols if not done[s]]
        min_symbol = min(active, key=lambda s: cursors[s])
        window_start = cursors[min_symbol]

        if window_start >= cfg.end_utc:
            done[min_symbol] = True
            continue

        # Fetch a page from window_start to end_utc (Alpaca will return up to limit bars total across symbols)
        # To avoid one symbol dominating, we fetch only the min_symbol per request (most stable).
        # This is slower but safer and avoids weird cross-symbol pagination edge cases.
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
                    limit=PAGE_LIMIT,
                )
                break
            except Exception as e:
                print(f"[WARN] fetch failed ({min_symbol}) attempt {attempt+1}/{cfg.max_retries}: {e}")
                backoff_sleep(attempt, cfg.base_sleep_s)

        if page_df.empty:
            # No more data for this symbol
            done[min_symbol] = True
            state[min_symbol] = cfg.end_utc.isoformat()
            save_state(cfg.state_path, state)
            print(f"[OK] {min_symbol}: no more data, done.")
            continue

        # Append to parquet
        append_parquet(paths[min_symbol], page_df)

        # Advance cursor to last timestamp + 1 minute
        last_ts = page_df["timestamp"].max().to_pydatetime().replace(tzinfo=timezone.utc)
        next_cursor = last_ts + pd.Timedelta(minutes=1)
        cursors[min_symbol] = next_cursor

        # Save state
        state[min_symbol] = cursors[min_symbol].isoformat()
        save_state(cfg.state_path, state)

        # Progress
        rows = len(page_df)
        print(f"[OK] {min_symbol}: +{rows} rows, now at {cursors[min_symbol].isoformat()}")

        # Gentle pacing to avoid rate limits
        time.sleep(0.25)

    print("\nDone.")
    for s in cfg.symbols:
        p = paths[s]
        if p.exists():
            df = pd.read_parquet(p, columns=["symbol", "timestamp"])
            df = df[df["symbol"] == s]
            print(f"{s}: {len(df):,} rows, {df['timestamp'].min()} -> {df['timestamp'].max()}")


def make_5min_from_1min(in_path: Path, out_path: Path) -> None:
    """
    Resample per symbol to 5-minute bars in UTC.
    NOTE: For strict US market session rules (RTH only), you’d filter to 9:30-16:00 ET first.
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


if __name__ == "__main__":
    load_dotenv(Path(__file__).resolve().parent / ".env")

    out_dir = Path(__file__).resolve().parent / "data" / "alpaca"
    state_path = Path(__file__).resolve().parent / "data" / "state" / "alpaca_minute_state.json"

    cfg = DownloadConfig(
        symbols=DEFAULT_SYMBOLS,
        timeframe=TimeFrame(1, TimeFrameUnit.Minute),
        start_utc=EARLIEST_MINUTE_UTC,
        end_utc=utcnow(),
        out_dir=out_dir,
        state_path=state_path,
    )

    download_minutes_max_history(cfg)

    # Optional: build 5-min files from the 1-min parquet outputs
    # This keeps your backtests fast.
    label_1m = tf_label(cfg.timeframe)
    for sym in cfg.symbols:
        in_p = parquet_path(out_dir, sym, label_1m)
        out_p = out_dir / f"{sym}_5min.parquet"
        if in_p.exists():
            print(f"Building 5-min for {sym}...")
            make_5min_from_1min(in_p, out_p)
            print(f"[OK] {out_p}")