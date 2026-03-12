"""Regression tests for execution symbol switching in run.py."""
import numpy as np
import pandas as pd

from run import run_backtest


def _write_ohlcv_csv(path, dates, close_prices):
    """Write minimal OHLCV CSV expected by loader."""
    df = pd.DataFrame(
        {
            "Date": dates,
            "Open": close_prices,
            "High": close_prices + 1.0,
            "Low": close_prices - 1.0,
            "Close": close_prices,
            "Volume": np.full(len(dates), 1_000_000.0),
        }
    )
    df.to_csv(path, index=False)


def _build_switch_fixture(tmp_path):
    """
    Build synthetic QQQ/TQQQ/SQQQ files that force bear->bull switch.

    SQQQ uses index-scaled prices (run.py divides by 100_000).
    """
    dates = pd.date_range("2020-01-01", periods=360, freq="B")

    # Warmup + downtrend (bear) + recovery (bull) to force at least one switch.
    qqq_close = np.concatenate(
        [
            np.full(260, 100.0),
            np.linspace(100.0, 70.0, 40),
            np.linspace(70.0, 130.0, 60),
        ]
    )

    # Keep TQQQ and SQQQ on very different scales to expose switch bugs.
    tqqq_close = np.full(len(dates), 30.0)
    sqqq_scaled_close = np.full(len(dates), 0.008)
    sqqq_index_scaled_close = sqqq_scaled_close * 100_000.0

    qqq_path = tmp_path / "qqq.csv"
    tqqq_path = tmp_path / "tqqq.csv"
    sqqq_path = tmp_path / "sqqq.csv"
    out_path = tmp_path / "out.csv"

    _write_ohlcv_csv(qqq_path, dates, qqq_close)
    _write_ohlcv_csv(tqqq_path, dates, tqqq_close)
    _write_ohlcv_csv(sqqq_path, dates, sqqq_index_scaled_close)

    return {
        "qqq": qqq_path,
        "tqqq": tqqq_path,
        "sqqq": sqqq_path,
        "out": out_path,
        "fold_start": str(dates[255].date()),
        "sqqq_scaled_open": float(sqqq_scaled_close[0]),
    }


def test_exec_symbol_matches_exec_target_weight_sign(tmp_path):
    """Exec symbol must align with shifted execution weight sign."""
    fixture = _build_switch_fixture(tmp_path)

    run_backtest(
        str(fixture["qqq"]),
        str(fixture["out"]),
        initial_capital=10_000.0,
        fold_start=fixture["fold_start"],
        fold_years=1,
        tqqq_path=str(fixture["tqqq"]),
        sqqq_path=str(fixture["sqqq"]),
    )

    out_df = pd.read_csv(fixture["out"])
    pos = out_df["Exec_Target_Weight"] > 0
    neg = out_df["Exec_Target_Weight"] < 0
    zero = out_df["Exec_Target_Weight"] == 0

    assert (out_df.loc[pos, "Exec_Symbol"] == "TQQQ").all()
    assert (out_df.loc[neg, "Exec_Symbol"] == "SQQQ").all()
    assert (out_df.loc[zero, "Exec_Symbol"] == "CASH").all()


def test_switch_uses_held_symbol_valuation_before_resizing(tmp_path):
    """
    On SQQQ->TQQQ switch, Portfolio_Value_Open must be marked with SQQQ open,
    then target shares resized from that equity (no synthetic equity jump).
    """
    fixture = _build_switch_fixture(tmp_path)

    run_backtest(
        str(fixture["qqq"]),
        str(fixture["out"]),
        initial_capital=10_000.0,
        fold_start=fixture["fold_start"],
        fold_years=1,
        tqqq_path=str(fixture["tqqq"]),
        sqqq_path=str(fixture["sqqq"]),
    )

    out_df = pd.read_csv(fixture["out"])
    prev_symbol = out_df["Exec_Symbol"].shift(1)
    switch_rows = out_df[(prev_symbol == "SQQQ") & (out_df["Exec_Symbol"] == "TQQQ")]
    assert len(switch_rows) >= 1, "Expected at least one SQQQ->TQQQ switch row"

    switch_idx = switch_rows.index[0]
    prev_row = out_df.iloc[switch_idx - 1]
    row = out_df.iloc[switch_idx]

    expected_open_value = (
        prev_row["Cash"]
        + prev_row["Total_Stocks_Owned"] * fixture["sqqq_scaled_open"]
    )
    assert np.isclose(row["Portfolio_Value_Open"], expected_open_value, rtol=0.0, atol=1e-9)

    # Exec_Target_Weight is QQQ-equivalent exposure; run.py converts to instrument weight
    # (exposure / leverage) before computing target shares (leverage-aware vol targeting).
    leverage = 3.0 if row["Exec_Symbol"] in ("TQQQ", "SQQQ") else 1.0
    instrument_weight = abs(row["Exec_Target_Weight"]) / leverage
    expected_target = int(
        np.floor(
            row["Portfolio_Value_Open"]
            * instrument_weight
            / row["Exec_Open"]
        )
    )
    assert int(row["Target_Shares"]) == expected_target
    assert int(row["Total_Stocks_Owned"]) == expected_target

    # Old bug created huge synthetic jumps on symbol relabeling.
    ratio = row["Portfolio_Value_Open"] / max(prev_row["Remaining_Portfolio_Amount"], 1e-12)
    assert ratio < 2.0
