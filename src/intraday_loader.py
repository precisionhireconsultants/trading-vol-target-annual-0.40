"""Intraday (1-min) data loader for Alpaca IEX parquet files.

Loads 1-minute bars, resamples to a full RTH minute grid (handling IEX
gaps), and aggregates to a daily DataFrame with Signal_Price, Exec_Price,
and Close columns for the intraday backtest pipeline.
"""
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import time as dt_time

NY_TZ = "America/New_York"
RTH_START = dt_time(9, 30)
RTH_END = dt_time(15, 59)
RTH_MINUTES = 390  # 09:30 … 15:59 inclusive


def load_intraday_parquet(alpaca_dir: str | Path, symbol: str) -> pd.DataFrame:
    """Load a single symbol's 1-min parquet and prepare for resampling.

    Steps:
        1. Read ``{symbol}_1min.parquet``.
        2. Convert ``timestamp`` to America/New_York.
        3. Derive ``Date`` from the NY timestamp (avoids UTC boundary bugs).
        4. Filter to regular trading hours (09:30–15:59 ET).

    Returns:
        DataFrame with columns:
            timestamp_ny, Date, symbol, open, high, low, close, volume
    """
    path = Path(alpaca_dir) / f"{symbol}_1min.parquet"
    if not path.exists():
        raise FileNotFoundError(f"Intraday parquet not found: {path}")

    df = pd.read_parquet(path)

    ts = pd.to_datetime(df["timestamp"], utc=True)
    df["timestamp_ny"] = ts.dt.tz_convert(NY_TZ)
    df["Date"] = df["timestamp_ny"].dt.date
    df["Date"] = pd.to_datetime(df["Date"])

    mask = (df["timestamp_ny"].dt.time >= RTH_START) & (
        df["timestamp_ny"].dt.time <= RTH_END
    )
    df = df.loc[mask].copy()
    df = df.sort_values(["symbol", "timestamp_ny"]).reset_index(drop=True)
    return df


def resample_to_full_grid(df: pd.DataFrame) -> pd.DataFrame:
    """Reindex minute bars to a full 390-bar RTH grid per (symbol, Date).

    Missing minutes are forward-filled for ``close``; synthetic rows get
    ``open = high = low = close`` and ``volume = 0``.
    """
    groups = df.groupby(["symbol", "Date"], sort=True)
    out_parts: list[pd.DataFrame] = []

    for (sym, date_val), grp in groups:
        if isinstance(date_val, pd.Timestamp):
            date_py = date_val.date()
        else:
            date_py = date_val

        expected = pd.date_range(
            start=pd.Timestamp(date_py).replace(hour=9, minute=30),
            end=pd.Timestamp(date_py).replace(hour=15, minute=59),
            freq="min",
            tz=NY_TZ,
        )

        grp = grp.set_index("timestamp_ny")
        grp = grp.reindex(expected)

        grp["close"] = grp["close"].ffill()
        still_nan = grp["close"].isna()
        if still_nan.any():
            grp["close"] = grp["close"].bfill()

        for col in ("open", "high", "low"):
            grp[col] = grp[col].fillna(grp["close"])

        grp["volume"] = grp["volume"].fillna(0.0)
        grp["symbol"] = sym
        grp["Date"] = pd.Timestamp(date_py)
        grp.index.name = "timestamp_ny"
        grp = grp.reset_index()

        out_parts.append(grp)

    if not out_parts:
        return pd.DataFrame(
            columns=[
                "timestamp_ny", "symbol", "Date",
                "open", "high", "low", "close", "volume",
            ]
        )
    return pd.concat(out_parts, ignore_index=True)


def aggregate_intraday_to_daily(
    minute_df: pd.DataFrame,
    signal_offset: int = 10,
    exec_offset: int = 2,
) -> pd.DataFrame:
    """Aggregate resampled minute bars into one row per (symbol, Date).

    Parameters:
        signal_offset: bars from end for Signal_Price (10 → 15:50 ET).
        exec_offset:   bars from end for Exec_Price   (2  → 15:58 ET).

    For days with fewer bars than ``max(signal_offset, exec_offset)``,
    Signal_Price and Exec_Price are NaN and ``can_trade = False``.

    Returns:
        DataFrame with columns:
            Date, symbol, Open, High, Low, Close, Volume,
            Signal_Price, Exec_Price, can_trade
    """
    min_bars = max(signal_offset, exec_offset)
    groups = minute_df.groupby(["symbol", "Date"], sort=True)
    rows: list[dict] = []

    for (sym, date_val), grp in groups:
        grp = grp.sort_values("timestamp_ny")
        n = len(grp)

        row: dict = {
            "Date": date_val,
            "symbol": sym,
            "Open": float(grp["open"].iloc[0]),
            "High": float(grp["high"].max()),
            "Low": float(grp["low"].min()),
            "Close": float(grp["close"].iloc[-1]),
            "Volume": float(grp["volume"].sum()),
        }

        if n < min_bars:
            row["Signal_Price"] = np.nan
            row["Exec_Price"] = np.nan
            row["can_trade"] = False
        else:
            row["Signal_Price"] = float(grp["close"].iloc[n - signal_offset])
            row["Exec_Price"] = float(grp["close"].iloc[n - exec_offset])
            row["can_trade"] = True

        rows.append(row)

    if not rows:
        return pd.DataFrame(
            columns=[
                "Date", "symbol", "Open", "High", "Low", "Close", "Volume",
                "Signal_Price", "Exec_Price", "can_trade",
            ]
        )

    result = pd.DataFrame(rows)
    result["Date"] = pd.to_datetime(result["Date"])
    return result
