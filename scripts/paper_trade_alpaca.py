"""Alpaca daily paper-trading runner.

Computes the daily regime signal from yesterday's completed close, then
executes at today's market open via the Alpaca paper-trading API.

Usage (local):
    .venv\\Scripts\\Activate.ps1
    python scripts/paper_trade_alpaca.py
    python scripts/paper_trade_alpaca.py --dry-run
    python scripts/paper_trade_alpaca.py --mode github-pre
    python scripts/paper_trade_alpaca.py --mode github-post
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import os
import subprocess
import sys
import time
import uuid
from datetime import datetime, timedelta
from datetime import timezone as _tz
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

_UTC = _tz.utc

# ---------------------------------------------------------------------------
# Ensure src/ is importable when running from scripts/
# ---------------------------------------------------------------------------
_REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_REPO_ROOT / "src"))

from config import DEFAULT_CONFIG
from indicators import add_ma250, add_ma50, add_annualized_volatility
from regime import (
    add_base_regime,
    add_confirmed_regime,
    add_final_trading_regime,
    add_target_weight,
)

# ---------------------------------------------------------------------------
# Late / optional imports (alpaca-py, dotenv)
# ---------------------------------------------------------------------------
try:
    from dotenv import load_dotenv
except ImportError:
    load_dotenv = None  # type: ignore[assignment]

try:
    from alpaca.trading.client import TradingClient
    from alpaca.trading.requests import (
        GetOrdersRequest,
        MarketOrderRequest,
    )
    from alpaca.trading.enums import (
        OrderSide,
        OrderStatus,
        QueryOrderStatus,
        TimeInForce,
    )
    from alpaca.data.historical import StockHistoricalDataClient
    from alpaca.data.requests import StockBarsRequest
    from alpaca.data.timeframe import TimeFrame
except ImportError as _alpaca_err:
    _ALPACA_MISSING = _alpaca_err
else:
    _ALPACA_MISSING = None  # type: ignore[assignment]

# =========================================================================
# Constants & defaults
# =========================================================================
TRADED_SYMBOLS = {"TQQQ", "SQQQ"}
SIGNAL_SYMBOL = "QQQ"
LOOKBACK_CALENDAR_DAYS = 450
LOG_DIR = _REPO_ROOT / "logs"
LOCK_DIR = LOG_DIR / "locks"
RUNS_DIR = LOG_DIR / "paper" / "runs"
ORDERS_DIR = LOG_DIR / "paper" / "orders"

DEFAULT_MAX_DAILY_LOSS_PCT = 0.02
DEFAULT_MAX_GROSS_EXPOSURE_PCT = 1.0
DEFAULT_MAX_POSITION_NOTIONAL = 100_000
DEFAULT_ORDER_FILL_TIMEOUT_MIN = 3
DEFAULT_LATE_RUN_CUTOFF_MIN = 15

# =========================================================================
# Two-table log schemas
# =========================================================================
RUNS_FIELDS = [
    # -- run metadata --
    "run_id",
    "run_source",
    "dry_run",
    "git_branch",
    "git_commit_sha",
    # -- timing --
    "trade_date",
    "decision_timestamp_et",
    "execution_timestamp_et",
    "clock_is_open",
    "clock_timestamp_et",
    "next_open_et",
    "next_close_et",
    "seconds_to_open",
    # -- signal inputs (yesterday-based) --
    "signal_date",
    "qqq_open",
    "qqq_high",
    "qqq_low",
    "qqq_close",
    "ma250",
    "ma50",
    "ann_vol",
    "base_regime",
    "confirmed_regime",
    "final_regime",
    "raw_target_weight",
    # -- strategy config snapshot --
    "use_ma_confirmation",
    "use_vol_targeting",
    "vol_target_annual",
    "max_position_pct",
    "tqqq_leverage",
    "max_effective_exposure",
    # -- target decision --
    "target_instrument",
    "target_shares",
    "target_notional_est",
    "price_estimate",
    "price_estimate_source",
    # -- account before --
    "equity_before",
    "cash_before",
    "buying_power_before",
    "last_equity",
    "daily_pnl_pct",
    # -- positions before --
    "tqqq_qty_before",
    "tqqq_mv_before",
    "sqqq_qty_before",
    "sqqq_mv_before",
    # -- account after --
    "equity_after",
    "cash_after",
    "tqqq_qty_after",
    "sqqq_qty_after",
    # -- outcome --
    "status",
    "reason_code",
    "orders_submitted",
    "orders_filled",
    "error_message",
    "kill_switch_triggered",
    "kill_switch_reason",
    "duplicate_guard_hit",
    "already_at_target",
]

ORDERS_FIELDS = [
    "run_id",
    "trade_date",
    "order_leg",
    "symbol",
    "side",
    "qty_submitted",
    "order_type",
    "time_in_force",
    "extended_hours",
    "client_order_id",
    "alpaca_order_id",
    "order_submit_ts",
    "order_status_initial",
    "order_status_final",
    "filled_qty",
    "filled_avg_price",
    "filled_at",
    "timed_out",
    "timeout_minutes",
    "reject_reason",
]

# =========================================================================
# Environment / config helpers
# =========================================================================

def _load_env() -> None:
    """Load .env from repo root (local) or rely on shell env (CI)."""
    if load_dotenv is not None:
        env_path = _REPO_ROOT / ".env"
        if env_path.exists():
            load_dotenv(env_path)
        else:
            alpaca_env = _REPO_ROOT / "alpaca_data" / ".env"
            if alpaca_env.exists():
                load_dotenv(alpaca_env)


def _env_float(key: str, default: float) -> float:
    val = os.getenv(key)
    return float(val) if val else default


def _env_int(key: str, default: int) -> int:
    val = os.getenv(key)
    return int(val) if val else default


class PaperTradeConfig:
    """Runtime configuration built from env vars with sensible defaults."""

    def __init__(self) -> None:
        self.api_key: str = os.getenv("ALPACA_API_KEY", "")
        self.secret_key: str = os.getenv("ALPACA_SECRET_KEY", "")
        self.base_url: str = os.getenv(
            "ALPACA_BASE_URL", "https://paper-api.alpaca.markets"
        )
        self.max_daily_loss_pct = _env_float(
            "MAX_DAILY_LOSS_PCT", DEFAULT_MAX_DAILY_LOSS_PCT
        )
        self.max_gross_exposure_pct = _env_float(
            "MAX_GROSS_EXPOSURE_PCT", DEFAULT_MAX_GROSS_EXPOSURE_PCT
        )
        self.max_position_notional = _env_float(
            "MAX_POSITION_NOTIONAL", DEFAULT_MAX_POSITION_NOTIONAL
        )
        self.order_fill_timeout_min = _env_int(
            "ORDER_FILL_TIMEOUT_MIN", DEFAULT_ORDER_FILL_TIMEOUT_MIN
        )
        self.late_run_cutoff_min = _env_int(
            "LATE_RUN_CUTOFF_MIN", DEFAULT_LATE_RUN_CUTOFF_MIN
        )
        self.use_ma_confirmation: bool = DEFAULT_CONFIG.USE_MA_CONFIRMATION
        self.use_vol_targeting: bool = DEFAULT_CONFIG.USE_VOL_TARGETING
        self.vol_target: float = DEFAULT_CONFIG.VOL_TARGET_ANNUAL
        self.max_position_pct: float = DEFAULT_CONFIG.MAX_POSITION_PCT
        self.tqqq_leverage: float = DEFAULT_CONFIG.TQQQ_LEVERAGE
        self.max_effective_exposure: float = DEFAULT_CONFIG.MAX_EFFECTIVE_EXPOSURE
        self.use_sqqq_in_bear: bool = DEFAULT_CONFIG.USE_SQQQ_IN_BEAR

    def validate(self) -> None:
        if not self.api_key or not self.secret_key:
            raise RuntimeError(
                "ALPACA_API_KEY and ALPACA_SECRET_KEY must be set "
                "(via .env or environment variables)"
            )


# =========================================================================
# Git metadata
# =========================================================================

def _get_git_info() -> Dict[str, str]:
    info: Dict[str, str] = {"git_branch": "", "git_commit_sha": ""}
    try:
        info["git_branch"] = subprocess.check_output(
            ["git", "rev-parse", "--abbrev-ref", "HEAD"],
            cwd=str(_REPO_ROOT), stderr=subprocess.DEVNULL,
        ).decode().strip()
    except Exception:
        pass
    try:
        info["git_commit_sha"] = subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd=str(_REPO_ROOT), stderr=subprocess.DEVNULL,
        ).decode().strip()
    except Exception:
        pass
    return info


# =========================================================================
# CSV logging (two-table)
# =========================================================================

def _append_csv(directory: Path, prefix: str, fields: List[str],
                record: Dict[str, Any]) -> None:
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"{prefix}_{datetime.now(tz=_UTC).strftime('%Y%m')}.csv"
    write_header = not path.exists()
    with open(path, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        if write_header:
            writer.writeheader()
        writer.writerow({k: record.get(k, "") for k in fields})


def append_run_log(record: Dict[str, Any]) -> None:
    _append_csv(RUNS_DIR, "paper_runs", RUNS_FIELDS, record)


def append_order_log(record: Dict[str, Any]) -> None:
    _append_csv(ORDERS_DIR, "paper_orders", ORDERS_FIELDS, record)


# =========================================================================
# Alpaca client helpers
# =========================================================================

def get_alpaca_clients(
    cfg: PaperTradeConfig,
) -> Tuple["TradingClient", "StockHistoricalDataClient"]:
    if _ALPACA_MISSING is not None:
        raise ImportError(
            "alpaca-py is required for paper trading. "
            "Install with: pip install alpaca-py"
        ) from _ALPACA_MISSING
    trading = TradingClient(cfg.api_key, cfg.secret_key, paper=True)
    data = StockHistoricalDataClient(cfg.api_key, cfg.secret_key)
    return trading, data


# =========================================================================
# Market clock
# =========================================================================

def get_market_clock(trading_client: "TradingClient") -> Dict[str, Any]:
    clock = trading_client.get_clock()
    return {
        "timestamp": clock.timestamp,
        "is_open": clock.is_open,
        "next_open": clock.next_open,
        "next_close": clock.next_close,
    }


# =========================================================================
# Daily bar retrieval
# =========================================================================

def load_daily_bars(
    data_client: "StockHistoricalDataClient",
    symbol: str,
    lookback_days: int = LOOKBACK_CALENDAR_DAYS,
) -> pd.DataFrame:
    """Fetch daily bars from Alpaca ending at yesterday's completed session."""
    from zoneinfo import ZoneInfo

    ny = ZoneInfo("America/New_York")
    now_ny = datetime.now(ny)
    end = now_ny.replace(hour=0, minute=0, second=0, microsecond=0)
    start = end - timedelta(days=lookback_days)

    request = StockBarsRequest(
        symbol_or_symbols=symbol,
        timeframe=TimeFrame.Day,
        start=start,
        end=end,
    )
    bars = data_client.get_stock_bars(request)

    records: List[Dict[str, Any]] = []
    for bar in bars[symbol]:
        records.append(
            {
                "Date": pd.Timestamp(bar.timestamp).tz_localize(None),
                "Open": float(bar.open),
                "High": float(bar.high),
                "Low": float(bar.low),
                "Close": float(bar.close),
                "Volume": float(bar.volume),
            }
        )
    if not records:
        raise RuntimeError(f"No daily bars returned for {symbol}")

    df = pd.DataFrame(records).sort_values("Date").reset_index(drop=True)
    return df


# =========================================================================
# Signal computation
# =========================================================================

def compute_daily_signal(
    qqq_df: pd.DataFrame,
    cfg: PaperTradeConfig,
) -> Dict[str, Any]:
    """Run indicators + regime on completed daily data (last row = yesterday).

    Returns dict with regime, target instrument, weight, and the signal row.
    """
    min_rows = DEFAULT_CONFIG.MA_LONG + 10
    if len(qqq_df) < min_rows:
        raise RuntimeError(
            f"Insufficient data: got {len(qqq_df)} rows, need >= {min_rows}"
        )

    df = qqq_df.copy()
    df = add_ma250(df)
    df = add_ma50(df)
    df = add_annualized_volatility(df)
    df = add_base_regime(df, price_col="Close")
    df = add_confirmed_regime(df, use_ma_confirmation=cfg.use_ma_confirmation)
    df = add_final_trading_regime(df, use_sqqq_in_bear=cfg.use_sqqq_in_bear)
    df = add_target_weight(
        df,
        use_vol_targeting=cfg.use_vol_targeting,
        vol_target=cfg.vol_target,
        max_position=cfg.max_position_pct,
    )

    last = df.iloc[-1]
    regime = str(last["Final_Trading_Regime"])
    weight = float(last["Target_Weight"])

    if regime == "bull":
        target_symbol = "TQQQ"
    elif regime == "bear" and cfg.use_sqqq_in_bear:
        target_symbol = "SQQQ"
    else:
        target_symbol = "CASH"

    return {
        "regime": regime,
        "target_symbol": target_symbol,
        "raw_weight": weight,
        "signal_date": str(last["Date"].date()) if hasattr(last["Date"], "date") else str(last["Date"])[:10],
        "qqq_open": float(last["Open"]),
        "qqq_high": float(last["High"]),
        "qqq_low": float(last["Low"]),
        "close": float(last["Close"]),
        "ma250": float(last.get("MA250", np.nan)),
        "ma50": float(last.get("MA50", np.nan)),
        "ann_vol": float(last.get("QQQ_ann_vol", np.nan)),
        "base_regime": str(last.get("Base_Regime", "")),
        "confirmed_regime": str(last.get("Confirmed_Regime", "")),
    }


# =========================================================================
# Account & positions
# =========================================================================

def get_account_state(trading_client: "TradingClient") -> Dict[str, Any]:
    acct = trading_client.get_account()
    return {
        "equity": float(acct.equity),
        "cash": float(acct.cash),
        "buying_power": float(acct.buying_power),
        "portfolio_value": float(acct.portfolio_value),
        "last_equity": float(acct.last_equity),
    }


def get_current_positions(
    trading_client: "TradingClient",
) -> Dict[str, Dict[str, Any]]:
    raw = trading_client.get_all_positions()
    positions: Dict[str, Dict[str, Any]] = {}
    for p in raw:
        positions[p.symbol] = {
            "qty": int(float(p.qty)),
            "market_value": float(p.market_value),
            "avg_entry_price": float(p.avg_entry_price),
            "side": str(p.side),
            "current_price": float(p.current_price),
        }
    return positions


def _positions_snapshot(positions: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    """Extract TQQQ/SQQQ qty and market value from positions dict."""
    tqqq = positions.get("TQQQ", {})
    sqqq = positions.get("SQQQ", {})
    return {
        "tqqq_qty": tqqq.get("qty", 0),
        "tqqq_mv": tqqq.get("market_value", 0.0),
        "sqqq_qty": sqqq.get("qty", 0),
        "sqqq_mv": sqqq.get("market_value", 0.0),
    }


# =========================================================================
# Target shares computation
# =========================================================================

def compute_target_shares_for_paper(
    equity: float,
    raw_weight: float,
    leverage: float,
    max_effective_exposure: float,
    current_price: float,
    max_position_notional: float,
) -> int:
    if current_price <= 0:
        return 0
    qqq_exposure = abs(raw_weight)
    capped_exposure = min(qqq_exposure, max_effective_exposure)
    instrument_weight = capped_exposure / leverage
    target_notional = equity * instrument_weight
    target_notional = min(target_notional, max_position_notional)
    return max(0, int(np.floor(target_notional / current_price)))


# =========================================================================
# Order helpers
# =========================================================================

def make_client_order_id(trade_date: str, symbol: str, side: str,
                         retry: int = 0) -> str:
    raw = f"daily_{trade_date}_{symbol}_{side}"
    short_hash = hashlib.sha256(raw.encode()).hexdigest()[:8]
    base = f"{raw}_{short_hash}"
    return base if retry == 0 else f"{base}_r{retry}"


def check_existing_orders(
    trading_client: "TradingClient", client_order_id_prefix: str
) -> Tuple[Optional[Any], int]:
    """Check for existing orders whose client_order_id starts with *prefix*.

    Returns (blocking_order, failed_count):
      - blocking_order: a filled or still-open order (dedup should apply), or None.
      - failed_count:   how many canceled/expired/rejected attempts exist,
                        so the caller can generate a unique retry id.
    """
    _terminal_failed = {OrderStatus.CANCELED, OrderStatus.EXPIRED, OrderStatus.REJECTED}
    failed_count = 0
    try:
        request = GetOrdersRequest(
            status=QueryOrderStatus.ALL,
            limit=100,
        )
        orders = trading_client.get_orders(request)
        for o in orders:
            cid = o.client_order_id or ""
            if not cid.startswith(client_order_id_prefix):
                continue
            if o.status in _terminal_failed:
                print(f"  [DEDUP-SKIP] Ignoring prior {o.symbol} order "
                      f"({o.status}); will retry.")
                failed_count += 1
                continue
            return o, failed_count
    except Exception:
        pass
    return None, failed_count


def submit_market_order(
    trading_client: "TradingClient",
    symbol: str,
    qty: int,
    side: str,
    client_order_id: str,
) -> Any:
    order_side = OrderSide.BUY if side == "BUY" else OrderSide.SELL
    req = MarketOrderRequest(
        symbol=symbol,
        qty=qty,
        side=order_side,
        time_in_force=TimeInForce.DAY,
        client_order_id=client_order_id,
    )
    return trading_client.submit_order(req)


def wait_for_fill_or_cancel(
    trading_client: "TradingClient",
    order_id: str,
    timeout_min: int,
) -> Dict[str, Any]:
    deadline = time.time() + timeout_min * 60
    terminal = {
        OrderStatus.FILLED,
        OrderStatus.CANCELED,
        OrderStatus.EXPIRED,
        OrderStatus.REJECTED,
    }
    while time.time() < deadline:
        order = trading_client.get_order_by_id(order_id)
        if order.status in terminal:
            return _order_to_dict(order)
        time.sleep(2)

    try:
        trading_client.cancel_order_by_id(order_id)
    except Exception:
        pass
    time.sleep(1)
    order = trading_client.get_order_by_id(order_id)
    result = _order_to_dict(order)
    result["timed_out"] = True
    return result


def _order_to_dict(order: Any) -> Dict[str, Any]:
    return {
        "order_id": str(order.id),
        "client_order_id": str(order.client_order_id),
        "symbol": str(order.symbol),
        "side": str(order.side),
        "qty": str(order.qty),
        "filled_qty": str(order.filled_qty) if order.filled_qty else "0",
        "filled_avg_price": str(order.filled_avg_price) if order.filled_avg_price else "",
        "status": str(order.status),
        "submitted_at": str(order.submitted_at),
        "filled_at": str(order.filled_at) if order.filled_at else "",
        "timed_out": False,
    }


# =========================================================================
# Guardrails
# =========================================================================

def check_max_daily_loss(
    account_state: Dict[str, Any],
    max_loss_pct: float,
) -> Tuple[bool, str]:
    equity = account_state["equity"]
    last_equity = account_state["last_equity"]
    if last_equity <= 0:
        return False, ""
    loss_pct = (last_equity - equity) / last_equity
    if loss_pct >= max_loss_pct:
        reason = (
            f"KILL_SWITCH: daily loss {loss_pct:.2%} >= limit {max_loss_pct:.2%} "
            f"(equity={equity:.2f}, last_equity={last_equity:.2f})"
        )
        return True, reason
    return False, ""


def check_max_position_notional(
    positions: Dict[str, Dict[str, Any]],
    max_notional: float,
) -> Tuple[bool, str]:
    for sym, pos in positions.items():
        if sym in TRADED_SYMBOLS and abs(pos["market_value"]) > max_notional:
            return True, (
                f"POSITION_CAP: {sym} market_value={pos['market_value']:.2f} "
                f"> limit {max_notional:.2f}"
            )
    return False, ""


# =========================================================================
# Duplicate-run protection
# =========================================================================

def check_duplicate_run(trade_date: str) -> bool:
    lock_file = LOCK_DIR / f"paper_trade_{trade_date}.lock"
    return lock_file.exists()


def write_lock_file(trade_date: str, info: str = "") -> None:
    LOCK_DIR.mkdir(parents=True, exist_ok=True)
    lock_file = LOCK_DIR / f"paper_trade_{trade_date}.lock"
    lock_file.write_text(
        f"executed_at={datetime.now(tz=_UTC).isoformat()}\n{info}\n"
    )


# =========================================================================
# Rebalance logic
# =========================================================================

def rebalance_to_target(
    trading_client: "TradingClient",
    target_symbol: str,
    target_shares: int,
    current_positions: Dict[str, Dict[str, Any]],
    trade_date: str,
    timeout_min: int,
    est_price: float = 0.0,
    buying_power: float = 0.0,
    dry_run: bool = False,
) -> List[Dict[str, Any]]:
    """Flatten opposite side, then open target. Returns list of order results."""
    results: List[Dict[str, Any]] = []

    for sym in TRADED_SYMBOLS:
        if sym == target_symbol:
            continue
        pos = current_positions.get(sym)
        if pos and pos["qty"] > 0:
            base_oid = make_client_order_id(trade_date, sym, "SELL")
            existing, failed = check_existing_orders(trading_client, base_oid)
            if existing is not None:
                print(f"  [DEDUP] Flatten order for {sym} already exists: {existing.status}")
                r = _order_to_dict(existing)
                r["_leg"] = "flatten"
                results.append(r)
                continue

            client_oid = make_client_order_id(trade_date, sym, "SELL", retry=failed)
            print(f"  Flattening {sym}: selling {pos['qty']} shares")
            if dry_run:
                results.append({"symbol": sym, "side": "SELL", "qty": pos["qty"],
                                "status": "DRY_RUN", "_leg": "flatten"})
                continue
            order = submit_market_order(
                trading_client, sym, pos["qty"], "SELL", client_oid
            )
            fill_result = wait_for_fill_or_cancel(
                trading_client, str(order.id), timeout_min
            )
            fill_result["_leg"] = "flatten"
            results.append(fill_result)

    if target_symbol == "CASH" or target_shares <= 0:
        return results

    current_qty = current_positions.get(target_symbol, {}).get("qty", 0)
    delta_shares = target_shares - current_qty
    action = "NONE"
    if delta_shares > 0:
        action = "BUY"
    elif delta_shares < 0:
        action = "SELL"
    print(
        f"  Target rebalance: symbol={target_symbol} current={current_qty} "
        f"target={target_shares} delta={delta_shares} action={action}"
    )

    if delta_shares == 0:
        print(f"  No target order: current position exactly matches target for {target_symbol}.")
        return results

    side = "BUY" if delta_shares > 0 else "SELL"
    desired_qty = delta_shares if side == "BUY" else abs(delta_shares)
    if side == "SELL":
        desired_qty = min(desired_qty, current_qty)

    if side == "BUY" and est_price > 0 and buying_power > 0:
        max_buy_qty = int(np.floor(buying_power / est_price))
        if max_buy_qty <= 0:
            print(
                f"  No target order: insufficient buying power ${buying_power:.2f} "
                f"at est_price ${est_price:.4f}."
            )
            return results
        if desired_qty > max_buy_qty:
            print(
                f"  Clamping BUY qty from {desired_qty} to {max_buy_qty} "
                f"due to buying power ${buying_power:.2f}."
            )
            desired_qty = max_buy_qty

    if desired_qty <= 0:
        print(
            f"  No target order: computed quantity is non-positive "
            f"(side={side}, desired_qty={desired_qty})."
        )
        return results

    base_oid = make_client_order_id(trade_date, target_symbol, side)
    existing, failed = check_existing_orders(trading_client, base_oid)
    if existing is not None:
        print(
            f"  [DEDUP] {side} order for {target_symbol} already exists: {existing.status}"
        )
        r = _order_to_dict(existing)
        r["_leg"] = "open"
        results.append(r)
        return results

    client_oid = make_client_order_id(trade_date, target_symbol, side, retry=failed)

    print(f"  Submitting target {side}: {desired_qty} shares of {target_symbol}")
    if dry_run:
        results.append({"symbol": target_symbol, "side": side, "qty": desired_qty,
                        "status": "DRY_RUN", "_leg": "open"})
        return results

    order = submit_market_order(
        trading_client, target_symbol, desired_qty, side, client_oid
    )
    fill_result = wait_for_fill_or_cancel(
        trading_client, str(order.id), timeout_min
    )
    fill_result["_leg"] = "open"
    results.append(fill_result)
    return results


# =========================================================================
# Market open wait
# =========================================================================

def wait_for_market_open(
    trading_client: "TradingClient", max_wait_seconds: int = 660
) -> bool:
    deadline = time.time() + max_wait_seconds
    while time.time() < deadline:
        clock = get_market_clock(trading_client)
        if clock["is_open"]:
            return True
        remaining = (clock["next_open"] - clock["timestamp"]).total_seconds()
        if remaining <= 0:
            time.sleep(2)
            continue
        if remaining > max_wait_seconds:
            return False
        sleep_secs = min(remaining + 1, 30)
        print(f"  Market opens in {remaining:.0f}s, sleeping {sleep_secs:.0f}s ...")
        time.sleep(sleep_secs)
    return get_market_clock(trading_client)["is_open"]


# =========================================================================
# Run-log builder helper
# =========================================================================

def _build_run_record(
    run_id: str,
    mode: str,
    dry_run: bool,
    git_info: Dict[str, str],
    trade_date: str,
    *,
    clock: Optional[Dict[str, Any]] = None,
    signal: Optional[Dict[str, Any]] = None,
    cfg: Optional[PaperTradeConfig] = None,
    account: Optional[Dict[str, Any]] = None,
    positions: Optional[Dict[str, Dict[str, Any]]] = None,
    target_symbol: str = "",
    target_shares: int = 0,
    target_notional_est: float = 0.0,
    price_estimate: float = 0.0,
    price_estimate_source: str = "",
    post_account: Optional[Dict[str, Any]] = None,
    post_positions: Optional[Dict[str, Dict[str, Any]]] = None,
    status: str = "",
    reason_code: str = "",
    orders_submitted: int = 0,
    orders_filled: int = 0,
    error_message: str = "",
    kill_switch_triggered: str = "",
    kill_switch_reason: str = "",
    duplicate_guard_hit: str = "",
    already_at_target: str = "",
    decision_ts: str = "",
    execution_ts: str = "",
) -> Dict[str, Any]:
    rec: Dict[str, Any] = {
        "run_id": run_id,
        "run_source": mode,
        "dry_run": str(dry_run),
        "git_branch": git_info.get("git_branch", ""),
        "git_commit_sha": git_info.get("git_commit_sha", ""),
        "trade_date": trade_date,
        "decision_timestamp_et": decision_ts,
        "execution_timestamp_et": execution_ts,
        "status": status,
        "reason_code": reason_code,
        "orders_submitted": str(orders_submitted),
        "orders_filled": str(orders_filled),
        "error_message": error_message,
        "kill_switch_triggered": kill_switch_triggered,
        "kill_switch_reason": kill_switch_reason,
        "duplicate_guard_hit": duplicate_guard_hit,
        "already_at_target": already_at_target,
        "target_instrument": target_symbol,
        "target_shares": str(target_shares),
        "target_notional_est": str(round(target_notional_est, 2)) if target_notional_est else "",
        "price_estimate": str(round(price_estimate, 4)) if price_estimate else "",
        "price_estimate_source": price_estimate_source,
    }

    if clock:
        rec["clock_is_open"] = str(clock.get("is_open", ""))
        rec["clock_timestamp_et"] = str(clock.get("timestamp", ""))
        rec["next_open_et"] = str(clock.get("next_open", ""))
        rec["next_close_et"] = str(clock.get("next_close", ""))
        ts = clock.get("timestamp")
        no = clock.get("next_open")
        if ts and no:
            try:
                rec["seconds_to_open"] = str(round((no - ts).total_seconds()))
            except Exception:
                pass

    if signal:
        rec["signal_date"] = signal.get("signal_date", "")
        rec["qqq_open"] = str(signal.get("qqq_open", ""))
        rec["qqq_high"] = str(signal.get("qqq_high", ""))
        rec["qqq_low"] = str(signal.get("qqq_low", ""))
        rec["qqq_close"] = str(signal.get("close", ""))
        rec["ma250"] = str(signal.get("ma250", ""))
        rec["ma50"] = str(signal.get("ma50", ""))
        rec["ann_vol"] = str(signal.get("ann_vol", ""))
        rec["base_regime"] = signal.get("base_regime", "")
        rec["confirmed_regime"] = signal.get("confirmed_regime", "")
        rec["final_regime"] = signal.get("regime", "")
        rec["raw_target_weight"] = str(signal.get("raw_weight", ""))

    if cfg:
        rec["use_ma_confirmation"] = str(cfg.use_ma_confirmation)
        rec["use_vol_targeting"] = str(cfg.use_vol_targeting)
        rec["vol_target_annual"] = str(cfg.vol_target)
        rec["max_position_pct"] = str(cfg.max_position_pct)
        rec["tqqq_leverage"] = str(cfg.tqqq_leverage)
        rec["max_effective_exposure"] = str(cfg.max_effective_exposure)

    if account:
        rec["equity_before"] = str(account.get("equity", ""))
        rec["cash_before"] = str(account.get("cash", ""))
        rec["buying_power_before"] = str(account.get("buying_power", ""))
        rec["last_equity"] = str(account.get("last_equity", ""))
        last_eq = account.get("last_equity", 0)
        eq = account.get("equity", 0)
        if last_eq and last_eq > 0:
            rec["daily_pnl_pct"] = str(round((eq - last_eq) / last_eq, 6))

    if positions:
        snap = _positions_snapshot(positions)
        rec["tqqq_qty_before"] = str(snap["tqqq_qty"])
        rec["tqqq_mv_before"] = str(snap["tqqq_mv"])
        rec["sqqq_qty_before"] = str(snap["sqqq_qty"])
        rec["sqqq_mv_before"] = str(snap["sqqq_mv"])

    if post_account:
        rec["equity_after"] = str(post_account.get("equity", ""))
        rec["cash_after"] = str(post_account.get("cash", ""))

    if post_positions:
        snap = _positions_snapshot(post_positions)
        rec["tqqq_qty_after"] = str(snap["tqqq_qty"])
        rec["sqqq_qty_after"] = str(snap["sqqq_qty"])

    return rec


# =========================================================================
# GitHub Actions reconciliation (post-open)
# =========================================================================

def run_github_post(
    trading_client: "TradingClient",
    cfg: PaperTradeConfig,
    trade_date: str,
    run_id: str,
    git_info: Dict[str, str],
) -> None:
    from zoneinfo import ZoneInfo

    print(f"\n=== GitHub Post-Open Reconciliation ({trade_date}) ===")

    request = GetOrdersRequest(status=QueryOrderStatus.ALL, limit=50)
    orders = trading_client.get_orders(request)
    today_orders = [
        o for o in orders
        if o.client_order_id and o.client_order_id.startswith(f"daily_{trade_date}")
    ]

    account = get_account_state(trading_client)
    positions = get_current_positions(trading_client)
    now_str = datetime.now(ZoneInfo("America/New_York")).isoformat()

    if not today_orders:
        print("No orders found for today. Nothing to reconcile.")
        append_run_log(_build_run_record(
            run_id, "github-post", False, git_info, trade_date,
            account=account, positions=positions,
            post_account=account, post_positions=positions,
            status="SKIPPED", reason_code="NO_ORDERS_TO_RECONCILE",
            decision_ts=now_str,
        ))
        return

    filled_count = 0
    for order in today_orders:
        status = str(order.status)
        filled_qty = str(order.filled_qty) if order.filled_qty else "0"
        filled_price = str(order.filled_avg_price) if order.filled_avg_price else ""

        if order.status != OrderStatus.FILLED:
            if order.status in (OrderStatus.NEW, OrderStatus.ACCEPTED, OrderStatus.PENDING_NEW):
                print(f"  Order {order.client_order_id} still open ({status}). Cancelling.")
                try:
                    trading_client.cancel_order_by_id(str(order.id))
                except Exception as e:
                    print(f"    Cancel failed: {e}")
        else:
            filled_count += 1

        append_order_log({
            "run_id": run_id,
            "trade_date": trade_date,
            "order_leg": "reconcile",
            "symbol": str(order.symbol),
            "side": str(order.side),
            "qty_submitted": str(order.qty),
            "order_type": str(order.type),
            "time_in_force": str(order.time_in_force),
            "extended_hours": "False",
            "client_order_id": str(order.client_order_id),
            "alpaca_order_id": str(order.id),
            "order_submit_ts": str(order.submitted_at),
            "order_status_initial": "",
            "order_status_final": status,
            "filled_qty": filled_qty,
            "filled_avg_price": filled_price,
            "filled_at": str(order.filled_at) if order.filled_at else "",
            "timed_out": "False",
            "timeout_minutes": "",
            "reject_reason": "",
        })
        print(f"  Logged {order.client_order_id}: {status} "
              f"(filled {filled_qty} @ {filled_price})")

    append_run_log(_build_run_record(
        run_id, "github-post", False, git_info, trade_date,
        account=account, positions=positions,
        post_account=account, post_positions=positions,
        status="RECONCILED", reason_code="RECONCILE",
        orders_submitted=len(today_orders), orders_filled=filled_count,
        decision_ts=now_str, execution_ts=now_str,
    ))

    print("\nFinal positions:")
    for sym, pos in positions.items():
        if sym in TRADED_SYMBOLS:
            print(f"  {sym}: {pos['qty']} shares, value={pos['market_value']:.2f}")
    print(f"Account equity: ${account['equity']:.2f}")
    print("Reconciliation complete.")


# =========================================================================
# Main workflow
# =========================================================================

def run_paper_trade(
    mode: str = "local",
    dry_run: bool = False,
) -> None:
    from zoneinfo import ZoneInfo

    ny = ZoneInfo("America/New_York")
    now_ny = datetime.now(ny)
    trade_date = now_ny.strftime("%Y-%m-%d")

    run_id = str(uuid.uuid4())
    git_info = _get_git_info()

    _load_env()
    cfg = PaperTradeConfig()
    cfg.validate()

    trading_client, data_client = get_alpaca_clients(cfg)
    print(f"Paper trading runner started at {now_ny.isoformat()}")
    print(f"Trade date: {trade_date}  Mode: {mode}  Dry-run: {dry_run}  Run: {run_id[:8]}")

    # --- GitHub post-open reconciliation mode ---
    if mode == "github-post":
        run_github_post(trading_client, cfg, trade_date, run_id, git_info)
        return

    # ------------------------------------------------------------------
    # 1. Market clock check
    # ------------------------------------------------------------------
    clock = get_market_clock(trading_client)
    print(f"\nMarket clock: is_open={clock['is_open']}, "
          f"next_open={clock['next_open']}, next_close={clock['next_close']}")

    if not clock["is_open"] and not dry_run:
        next_open = clock["next_open"]
        seconds_to_open = (next_open - clock["timestamp"]).total_seconds()
        if seconds_to_open < 0 or seconds_to_open > 14400:
            print("Market is closed and next open is far away. Exiting.")
            append_run_log(_build_run_record(
                run_id, mode, dry_run, git_info, trade_date,
                clock=clock, cfg=cfg,
                status="SKIPPED", reason_code="MARKET_CLOSED",
                decision_ts=now_ny.isoformat(),
            ))
            return

    # ------------------------------------------------------------------
    # 2. Late-run skip (for GitHub Actions)
    # ------------------------------------------------------------------
    if mode.startswith("github") and clock["is_open"]:
        elapsed = (clock["timestamp"] - clock["next_open"]).total_seconds()
        if elapsed >= 0:
            market_open_time = clock["next_close"] - timedelta(hours=6, minutes=30)
            elapsed_since_open = (clock["timestamp"] - market_open_time).total_seconds()
            if elapsed_since_open > cfg.late_run_cutoff_min * 60:
                print(f"Late run: market open {elapsed_since_open/60:.0f} min > "
                      f"cutoff {cfg.late_run_cutoff_min} min. Skipping.")
                append_run_log(_build_run_record(
                    run_id, mode, dry_run, git_info, trade_date,
                    clock=clock, cfg=cfg,
                    status="SKIPPED", reason_code="LATE_RUN_SKIP",
                    decision_ts=now_ny.isoformat(),
                ))
                return

    # ------------------------------------------------------------------
    # 3. Duplicate-run protection
    # ------------------------------------------------------------------
    if check_duplicate_run(trade_date):
        print(f"Lock file exists for {trade_date}. Already executed today.")
        append_run_log(_build_run_record(
            run_id, mode, dry_run, git_info, trade_date,
            clock=clock, cfg=cfg,
            status="SKIPPED", reason_code="DUPLICATE_RUN",
            duplicate_guard_hit="True",
            decision_ts=now_ny.isoformat(),
        ))
        return

    # ------------------------------------------------------------------
    # 4. Fetch daily bars
    # ------------------------------------------------------------------
    print(f"\nFetching {SIGNAL_SYMBOL} daily bars from Alpaca ...")
    try:
        qqq_df = load_daily_bars(data_client, SIGNAL_SYMBOL)
    except Exception as e:
        print(f"ERROR: Failed to load daily bars: {e}")
        append_run_log(_build_run_record(
            run_id, mode, dry_run, git_info, trade_date,
            clock=clock, cfg=cfg,
            status="HALTED", reason_code="DATA_FETCH_FAILED",
            error_message=str(e), decision_ts=now_ny.isoformat(),
        ))
        return

    print(f"Loaded {len(qqq_df)} daily bars, last date: {qqq_df.iloc[-1]['Date']}")

    # ------------------------------------------------------------------
    # 5. Compute signal
    # ------------------------------------------------------------------
    try:
        signal = compute_daily_signal(qqq_df, cfg)
    except Exception as e:
        print(f"ERROR: Signal computation failed: {e}")
        append_run_log(_build_run_record(
            run_id, mode, dry_run, git_info, trade_date,
            clock=clock, cfg=cfg,
            status="HALTED", reason_code="SIGNAL_FAILED",
            error_message=str(e), decision_ts=now_ny.isoformat(),
        ))
        return

    decision_ts = datetime.now(ny).isoformat()
    print(f"\n--- Signal (based on {signal['signal_date']} close) ---")
    print(f"  Regime:        {signal['regime']}")
    print(f"  Target:        {signal['target_symbol']}")
    print(f"  Raw weight:    {signal['raw_weight']:.4f}")
    print(f"  QQQ OHLC:      O={signal['qqq_open']:.2f}  H={signal['qqq_high']:.2f}  "
          f"L={signal['qqq_low']:.2f}  C={signal['close']:.2f}")
    print(f"  MA250:         {signal['ma250']:.2f}")
    print(f"  MA50:          {signal['ma50']:.2f}")
    print(f"  Ann. vol:      {signal['ann_vol']:.4f}")
    print(f"  Regimes:       base={signal['base_regime']}  "
          f"confirmed={signal['confirmed_regime']}  final={signal['regime']}")

    # ------------------------------------------------------------------
    # 6. Account state & positions
    # ------------------------------------------------------------------
    account = get_account_state(trading_client)
    positions = get_current_positions(trading_client)
    print(f"\n--- Account ---")
    print(f"  Equity:        ${account['equity']:.2f}")
    print(f"  Cash:          ${account['cash']:.2f}")
    print(f"  Buying power:  ${account['buying_power']:.2f}")
    for sym in TRADED_SYMBOLS:
        pos = positions.get(sym)
        if pos:
            print(f"  {sym}:  {pos['qty']} shares @ ${pos['current_price']:.2f} "
                  f"(value=${pos['market_value']:.2f})")

    # ------------------------------------------------------------------
    # 7. Guardrails
    # ------------------------------------------------------------------
    kill_flag = ""
    kill_reason = ""

    halted, reason = check_max_daily_loss(account, cfg.max_daily_loss_pct)
    if halted:
        print(f"\n{reason}")
        kill_flag = "True"
        kill_reason = reason
        append_run_log(_build_run_record(
            run_id, mode, dry_run, git_info, trade_date,
            clock=clock, signal=signal, cfg=cfg,
            account=account, positions=positions,
            status="HALTED", reason_code="KILL_SWITCH_DAILY_LOSS",
            kill_switch_triggered="True", kill_switch_reason=reason,
            decision_ts=decision_ts,
        ))
        write_lock_file(trade_date, reason)
        return

    halted, reason = check_max_position_notional(
        positions, cfg.max_position_notional
    )
    if halted:
        print(f"\n{reason}")
        kill_flag = "True"
        kill_reason = reason

    # ------------------------------------------------------------------
    # 8. Compute target shares
    # ------------------------------------------------------------------
    target_symbol = signal["target_symbol"]
    target_shares = 0
    est_price = 0.0
    price_source = ""

    if target_symbol != "CASH":
        current_price_data = positions.get(target_symbol)
        if current_price_data:
            est_price = current_price_data["current_price"]
            price_source = "position_current"
        else:
            try:
                price_bars = load_daily_bars(data_client, target_symbol, lookback_days=10)
                if len(price_bars) > 0:
                    est_price = float(price_bars.iloc[-1]["Close"])
                    price_source = "daily_bar_close"
            except Exception:
                pass

        if est_price <= 0:
            print(f"ERROR: Cannot get price for {target_symbol}")
            append_run_log(_build_run_record(
                run_id, mode, dry_run, git_info, trade_date,
                clock=clock, signal=signal, cfg=cfg,
                account=account, positions=positions,
                target_symbol=target_symbol,
                status="HALTED", reason_code="NO_PRICE",
                error_message=f"No price for {target_symbol}",
                decision_ts=decision_ts,
            ))
            return

        target_shares = compute_target_shares_for_paper(
            equity=account["equity"],
            raw_weight=signal["raw_weight"],
            leverage=cfg.tqqq_leverage,
            max_effective_exposure=cfg.max_effective_exposure,
            current_price=est_price,
            max_position_notional=cfg.max_position_notional,
        )
        print(f"\n  Target: {target_shares} shares of {target_symbol} "
              f"@ ~${est_price:.2f} (notional ~${target_shares * est_price:,.2f})")

    target_notional = target_shares * est_price

    # Check if already at target
    current_qty = positions.get(target_symbol, {}).get("qty", 0)
    delta_shares = target_shares - current_qty
    opposite_held = any(
        positions.get(s, {}).get("qty", 0) > 0
        for s in TRADED_SYMBOLS if s != target_symbol
    )

    if (
        not opposite_held
        and target_symbol != "CASH"
        and delta_shares == 0
    ):
        print(
            f"\n  Target rebalance decision: symbol={target_symbol} "
            f"current={current_qty} target={target_shares} "
            f"delta={delta_shares} action=NONE (already exact target)"
        )
        append_run_log(_build_run_record(
            run_id, mode, dry_run, git_info, trade_date,
            clock=clock, signal=signal, cfg=cfg,
            account=account, positions=positions,
            target_symbol=target_symbol, target_shares=target_shares,
            target_notional_est=target_notional,
            price_estimate=est_price, price_estimate_source=price_source,
            post_account=account, post_positions=positions,
            status="AT_TARGET", reason_code="NO_ACTION",
            already_at_target="True", decision_ts=decision_ts,
        ))
        write_lock_file(trade_date, "at_target")
        return

    if target_symbol == "CASH" and not any(
        positions.get(s, {}).get("qty", 0) > 0 for s in TRADED_SYMBOLS
    ):
        print("\n  Target is CASH and no positions held. No trade.")
        append_run_log(_build_run_record(
            run_id, mode, dry_run, git_info, trade_date,
            clock=clock, signal=signal, cfg=cfg,
            account=account, positions=positions,
            target_symbol="CASH",
            post_account=account, post_positions=positions,
            status="AT_TARGET", reason_code="NO_ACTION",
            already_at_target="True", decision_ts=decision_ts,
        ))
        write_lock_file(trade_date, "cash_no_position")
        return

    # ------------------------------------------------------------------
    # 9. Wait for market open (skip in dry-run and github-pre)
    # ------------------------------------------------------------------
    if dry_run:
        print("\n[DRY RUN] Would submit orders at market open. Exiting.")
        append_run_log(_build_run_record(
            run_id, mode, dry_run, git_info, trade_date,
            clock=clock, signal=signal, cfg=cfg,
            account=account, positions=positions,
            target_symbol=target_symbol, target_shares=target_shares,
            target_notional_est=target_notional,
            price_estimate=est_price, price_estimate_source=price_source,
            post_account=account, post_positions=positions,
            status="DRY_RUN", reason_code="DRY_RUN",
            decision_ts=decision_ts,
        ))
        return

    if not clock["is_open"]:
        max_wait = 4200 if mode.startswith("github") else 660
        print(f"\nWaiting for market open (max {max_wait}s) ...")
        opened = wait_for_market_open(trading_client, max_wait_seconds=max_wait)
        if not opened:
            print("Market did not open within wait window. Exiting.")
            append_run_log(_build_run_record(
                run_id, mode, dry_run, git_info, trade_date,
                clock=clock, signal=signal, cfg=cfg,
                account=account, positions=positions,
                target_symbol=target_symbol, target_shares=target_shares,
                status="SKIPPED", reason_code="MARKET_DID_NOT_OPEN",
                decision_ts=decision_ts,
            ))
            return
        print("Market is open!")
        time.sleep(3)

    # ------------------------------------------------------------------
    # 10. Execute rebalance
    # ------------------------------------------------------------------
    exec_ts = datetime.now(ny).isoformat()
    print(f"\n=== Executing rebalance at {exec_ts} ===")

    order_results = rebalance_to_target(
        trading_client=trading_client,
        target_symbol=target_symbol,
        target_shares=target_shares,
        current_positions=positions,
        trade_date=trade_date,
        timeout_min=cfg.order_fill_timeout_min,
        est_price=est_price,
        buying_power=account.get("buying_power", 0.0),
        dry_run=dry_run,
    )

    # ------------------------------------------------------------------
    # 11. Confirm & log
    # ------------------------------------------------------------------
    post_account = get_account_state(trading_client) if not dry_run else account
    post_positions = get_current_positions(trading_client) if not dry_run else positions

    filled_count = 0
    for result in order_results:
        is_filled = str(result.get("status", "")).lower() in ("orderstatus.filled", "filled")
        if is_filled:
            filled_count += 1

        append_order_log({
            "run_id": run_id,
            "trade_date": trade_date,
            "order_leg": result.get("_leg", ""),
            "symbol": result.get("symbol", ""),
            "side": result.get("side", ""),
            "qty_submitted": str(result.get("qty", "")),
            "order_type": "market",
            "time_in_force": "day",
            "extended_hours": "False",
            "client_order_id": result.get("client_order_id", ""),
            "alpaca_order_id": result.get("order_id", ""),
            "order_submit_ts": result.get("submitted_at", ""),
            "order_status_initial": "",
            "order_status_final": str(result.get("status", "")),
            "filled_qty": result.get("filled_qty", ""),
            "filled_avg_price": result.get("filled_avg_price", ""),
            "filled_at": result.get("filled_at", ""),
            "timed_out": str(result.get("timed_out", False)),
            "timeout_minutes": str(cfg.order_fill_timeout_min),
            "reject_reason": "",
        })

    append_run_log(_build_run_record(
        run_id, mode, dry_run, git_info, trade_date,
        clock=clock, signal=signal, cfg=cfg,
        account=account, positions=positions,
        target_symbol=target_symbol, target_shares=target_shares,
        target_notional_est=target_notional,
        price_estimate=est_price, price_estimate_source=price_source,
        post_account=post_account, post_positions=post_positions,
        status="SUBMITTED", reason_code="REBALANCE",
        orders_submitted=len(order_results), orders_filled=filled_count,
        kill_switch_triggered=kill_flag, kill_switch_reason=kill_reason,
        decision_ts=decision_ts, execution_ts=exec_ts,
    ))

    write_lock_file(trade_date, f"target={target_symbol},shares={target_shares}")

    # Summary
    print(f"\n=== Post-trade summary ===")
    print(f"  Account equity: ${post_account['equity']:.2f}")
    print(f"  Cash:           ${post_account['cash']:.2f}")
    for sym in TRADED_SYMBOLS:
        pos = post_positions.get(sym)
        if pos:
            print(f"  {sym}: {pos['qty']} shares (${pos['market_value']:.2f})")
    print(f"  Orders submitted: {len(order_results)}")
    for r in order_results:
        print(f"    {r.get('symbol','?')} {r.get('side','?')} "
              f"qty={r.get('qty','?')} status={r.get('status','?')} "
              f"fill={r.get('filled_avg_price','')}")
    print("\nDone.")


# =========================================================================
# CLI
# =========================================================================

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Alpaca daily paper-trading runner"
    )
    parser.add_argument(
        "--mode",
        choices=["local", "github-pre", "github-post"],
        default="local",
        help="Execution mode (default: local)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Compute signal and print plan without submitting orders",
    )
    args = parser.parse_args()
    try:
        run_paper_trade(mode=args.mode, dry_run=args.dry_run)
    except Exception as e:
        print(f"\nFATAL ERROR: {e}")
        import traceback
        traceback.print_exc()
        try:
            append_run_log({
                "run_id": str(uuid.uuid4()),
                "trade_date": datetime.now(tz=_UTC).strftime("%Y-%m-%d"),
                "run_source": args.mode,
                "status": "ERROR",
                "reason_code": "FATAL_ERROR",
                "error_message": str(e),
            })
        except Exception:
            pass
        sys.exit(1)


if __name__ == "__main__":
    main()
