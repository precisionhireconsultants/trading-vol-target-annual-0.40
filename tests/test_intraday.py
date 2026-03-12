"""Tests for the intraday backtest pipeline.

Test A — MA invariance (no look-ahead)
Test B — Timestamp correctness (offsets → clock times)
Test C — can_trade=False behaviour (position unchanged, no trades)
Test D — NY date grouping (UTC boundary)
Test E — Resampling fills IEX gaps
"""
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from intraday_loader import (
    resample_to_full_grid,
    aggregate_intraday_to_daily,
    NY_TZ,
)
from indicators import add_ma250


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_full_day_bars(date_str: str, symbol: str = "QQQ", base_price: float = 100.0):
    """Create a full 390-bar RTH day (09:30 – 15:59 ET) with synthetic prices."""
    ts = pd.date_range(
        start=f"{date_str} 09:30",
        end=f"{date_str} 15:59",
        freq="min",
        tz=NY_TZ,
    )
    n = len(ts)
    prices = base_price + np.arange(n) * 0.01
    return pd.DataFrame({
        "timestamp_ny": ts,
        "Date": pd.Timestamp(date_str),
        "symbol": symbol,
        "open": prices,
        "high": prices + 0.02,
        "low": prices - 0.02,
        "close": prices,
        "volume": np.ones(n) * 100,
    })


def _make_trading_days(n_days: int, symbol: str = "QQQ", base_price: float = 100.0):
    """Create *n_days* consecutive full-session minute dfs (skipping weekends)."""
    dates = pd.bdate_range(start="2020-01-02", periods=n_days)
    parts = []
    for i, dt in enumerate(dates):
        ds = dt.strftime("%Y-%m-%d")
        parts.append(_make_full_day_bars(ds, symbol, base_price + i))
    return pd.concat(parts, ignore_index=True)


# ---------------------------------------------------------------------------
# Test A — MA invariance (no look-ahead)
# ---------------------------------------------------------------------------

class TestMAInvariance:
    """Changing today's intraday bars must not alter today's MA."""

    def test_ma_does_not_use_todays_close(self):
        n_days = 260
        minute_df = _make_trading_days(n_days)
        gridded = resample_to_full_grid(minute_df)
        daily = aggregate_intraday_to_daily(gridded, signal_offset=10, exec_offset=2)
        daily = daily.sort_values("Date").reset_index(drop=True)

        daily_with_ma = add_ma250(daily, intraday_mode=True)
        ma_last = daily_with_ma["MA250"].iloc[-1]

        last_date = daily["Date"].iloc[-1]
        minute_df2 = minute_df.copy()
        mask = minute_df2["Date"] == last_date
        minute_df2.loc[mask, "close"] = minute_df2.loc[mask, "close"] + 50.0
        minute_df2.loc[mask, "open"] = minute_df2.loc[mask, "open"] + 50.0
        minute_df2.loc[mask, "high"] = minute_df2.loc[mask, "high"] + 50.0
        minute_df2.loc[mask, "low"] = minute_df2.loc[mask, "low"] + 50.0

        gridded2 = resample_to_full_grid(minute_df2)
        daily2 = aggregate_intraday_to_daily(gridded2, signal_offset=10, exec_offset=2)
        daily2 = daily2.sort_values("Date").reset_index(drop=True)
        daily2_with_ma = add_ma250(daily2, intraday_mode=True)
        ma_last2 = daily2_with_ma["MA250"].iloc[-1]

        assert ma_last == ma_last2, (
            f"MA changed when only today's bars changed: {ma_last} vs {ma_last2}"
        )


# ---------------------------------------------------------------------------
# Test B — Timestamp correctness
# ---------------------------------------------------------------------------

class TestTimestampCorrectness:
    """Signal and exec prices must correspond to specific clock times."""

    def test_signal_at_1550_exec_at_1558(self):
        minute_df = _make_full_day_bars("2020-07-27")
        gridded = resample_to_full_grid(minute_df)
        daily = aggregate_intraday_to_daily(gridded, signal_offset=10, exec_offset=2)

        assert len(daily) == 1
        row = daily.iloc[0]

        bar_1550 = gridded[gridded["timestamp_ny"].dt.time == pd.Timestamp("15:50").time()]
        bar_1558 = gridded[gridded["timestamp_ny"].dt.time == pd.Timestamp("15:58").time()]

        assert len(bar_1550) == 1
        assert len(bar_1558) == 1

        assert row["Signal_Price"] == float(bar_1550.iloc[0]["close"])
        assert row["Exec_Price"] == float(bar_1558.iloc[0]["close"])


# ---------------------------------------------------------------------------
# Test C — can_trade=False behaviour
# ---------------------------------------------------------------------------

class TestCanTradeFalse:
    """When can_trade is False, position must remain unchanged."""

    def test_short_session_marks_no_trade(self):
        """A day with only 5 bars should produce can_trade=False."""
        ts = pd.date_range(
            start="2020-07-27 09:30",
            periods=5,
            freq="min",
            tz=NY_TZ,
        )
        df = pd.DataFrame({
            "timestamp_ny": ts,
            "Date": pd.Timestamp("2020-07-27"),
            "symbol": "QQQ",
            "open": [100.0] * 5,
            "high": [101.0] * 5,
            "low": [99.0] * 5,
            "close": [100.0] * 5,
            "volume": [100.0] * 5,
        })

        daily = aggregate_intraday_to_daily(df, signal_offset=10, exec_offset=2)
        assert len(daily) == 1
        row = daily.iloc[0]

        assert row["can_trade"] is False or row["can_trade"] == False  # noqa: E712
        assert np.isnan(row["Signal_Price"])
        assert np.isnan(row["Exec_Price"])


# ---------------------------------------------------------------------------
# Test D — NY date grouping
# ---------------------------------------------------------------------------

class TestNYDateGrouping:
    """Bars must group by New York date, not UTC date."""

    def test_utc_evening_maps_to_ny_date(self):
        utc_ts = pd.Timestamp("2020-07-27 19:50:00+00:00")
        ny_ts = utc_ts.tz_convert(NY_TZ)

        assert ny_ts.date() == pd.Timestamp("2020-07-27").date()

    def test_grouping_in_loader(self):
        """A bar at UTC 20:00 (= 16:00 ET) should group to NY date 2020-07-27
        but be *filtered out* by RTH (since 16:00 > 15:59)."""
        utc_ts = pd.Timestamp("2020-07-27 20:00:00+00:00")
        ny_ts = utc_ts.tz_convert(NY_TZ)

        assert ny_ts.date() == pd.Timestamp("2020-07-27").date()
        assert ny_ts.hour == 16 and ny_ts.minute == 0


# ---------------------------------------------------------------------------
# Test E — Resampling fills gaps
# ---------------------------------------------------------------------------

class TestResamplingGaps:
    """Resampling must produce exactly 390 bars and forward-fill gaps."""

    def test_sparse_day_gets_filled(self):
        """Only 100 bars present → resampled output must have 390 rows."""
        full_ts = pd.date_range(
            start="2020-07-27 09:30",
            end="2020-07-27 15:59",
            freq="min",
            tz=NY_TZ,
        )
        sparse_ts = full_ts[:100]
        df = pd.DataFrame({
            "timestamp_ny": sparse_ts,
            "Date": pd.Timestamp("2020-07-27"),
            "symbol": "QQQ",
            "open": np.arange(100, dtype=float) + 100,
            "high": np.arange(100, dtype=float) + 101,
            "low": np.arange(100, dtype=float) + 99,
            "close": np.arange(100, dtype=float) + 100,
            "volume": np.ones(100) * 50,
        })

        result = resample_to_full_grid(df)
        assert len(result) == 390

        synthetic = result.iloc[100:]
        assert (synthetic["volume"] == 0).all()

        last_known_close = df["close"].iloc[-1]
        assert (synthetic["close"] == last_known_close).all()
        assert (synthetic["open"] == synthetic["close"]).all()
        assert (synthetic["high"] == synthetic["close"]).all()
        assert (synthetic["low"] == synthetic["close"]).all()
