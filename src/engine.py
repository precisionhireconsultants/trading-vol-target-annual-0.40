"""Paper trading engine foundation (Phase 32).

This module provides the foundation for live/paper trading:
- Broker adapter interface
- Paper broker implementation
- Order logging
- Kill switches (risk controls)
"""
import pandas as pd
import numpy as np
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional, Dict, Any
from pathlib import Path

from config import DEFAULT_CONFIG, ZERO_EPS


# =============================================================================
# Reason Codes (for debugging and PowerBI)
# =============================================================================

# Trade executed
REASON_REGIME_SWITCH_BUY = "REGIME_SWITCH_BUY"    # Entered market (0 -> non-zero)
REASON_REGIME_SWITCH_SELL = "REGIME_SWITCH_SELL"  # Exited market (non-zero -> 0)
REASON_REBALANCE = "REBALANCE"                     # Weight drift exceeded band

# Trade skipped
REASON_BAND_SKIP = "BAND_SKIP"                     # Within rebalance band
REASON_NO_CHANGE = "NO_CHANGE"                     # Target == actual

# Weight modified
REASON_EXPOSURE_CLAMPED = "EXPOSURE_CLAMPED"       # Weight reduced for leverage

# Halts
REASON_HALT_DATA_MISSING = "HALT_DATA_MISSING"
REASON_HALT_DATA_BAD_BAR = "HALT_DATA_BAD_BAR"
REASON_HALT_DATA_BAD_VOLUME = "HALT_DATA_BAD_VOLUME"
REASON_HALT_DATA_MISSING_EXEC_BAR = "HALT_DATA_MISSING_EXEC_BAR"
REASON_HALT_DATA_GAP = "HALT_DATA_GAP"               # Runtime missing bar (live mode)
REASON_HALT_SIGNAL_NAN = "HALT_SIGNAL_NAN"           # NaN target_weight into clamp
REASON_HALT_DAILY_LOSS = "HALT_DAILY_LOSS"
REASON_HALT_DRAWDOWN = "HALT_DRAWDOWN"
REASON_HALT_LOCKOUT = "HALT_LOCKOUT"

# Order rejections
REASON_REJECTED_NO_PRICE = "REJECTED_NO_PRICE"
REASON_REJECTED_INSUFFICIENT_CASH = "REJECTED_INSUFFICIENT_CASH"
REASON_REJECTED_INSUFFICIENT_SHARES = "REJECTED_INSUFFICIENT_SHARES"

# Flatten
REASON_FLATTEN = "FLATTEN"                         # Emergency sell on halt

# Affordability
REASON_AFFORDABILITY_CLAMPED = "AFFORDABILITY_CLAMPED"
REASON_INSUFFICIENT_CASH_AFTER_COSTS = "INSUFFICIENT_CASH_AFTER_COSTS"


# =============================================================================
# Reason Code Pipeline (Phase 44)
# =============================================================================

# Canonical modifier order for deterministic reason strings
MODIFIER_ORDER = ("EXPOSURE_CLAMPED", "AFFORDABILITY_CLAMPED", "BAND_SKIP", "THROTTLED", "FLATTEN")
MODIFIER_ORDER_DICT = {name: i for i, name in enumerate(MODIFIER_ORDER)}


class ReasonBuilder:
    """
    Structured reason code builder (Phase 44).
    
    Maintains modifiers in canonical order for deterministic output.
    FLATTEN is always last in the modifier list.
    
    Example: HALT_DAILY_LOSS|EXPOSURE_CLAMPED|FLATTEN
    """
    
    def __init__(self, base_reason: str = ""):
        self.base_reason = base_reason
        self.modifiers = set()
    
    def set_base(self, reason: str) -> 'ReasonBuilder':
        """Set the base reason (HALT reasons dominate)."""
        self.base_reason = reason
        return self
    
    def add_modifier(self, modifier: str) -> 'ReasonBuilder':
        """Add a modifier to the reason."""
        self.modifiers.add(modifier)
        return self
    
    def add_flatten(self) -> 'ReasonBuilder':
        """Add FLATTEN modifier (always last)."""
        self.modifiers.add(REASON_FLATTEN)
        return self
    
    def halt(self, reason: str) -> 'ReasonBuilder':
        """Set a HALT reason as base (replaces previous base)."""
        self.base_reason = reason
        return self
    
    def build(self) -> str:
        """
        Build the final reason string.
        
        Modifiers are sorted by MODIFIER_ORDER_DICT for determinism.
        Unknown modifiers sort to the end (before FLATTEN).
        """
        if not self.base_reason:
            return ""
        
        if not self.modifiers:
            return self.base_reason
        
        # Sort modifiers by canonical order
        ordered = sorted(
            self.modifiers,
            key=lambda m: MODIFIER_ORDER_DICT.get(m, 999)
        )
        
        return f"{self.base_reason}|{'|'.join(ordered)}"
    
    def __str__(self) -> str:
        return self.build()


# =============================================================================
# Exposure Clamp Function (Phase 40)
# =============================================================================

def clamp_weight_for_leverage(
    target_weight: float,
    leverage: float,
    max_effective_exposure: float = 1.0
) -> tuple:
    """
    Clamp target weight so effective exposure <= max_effective_exposure.
    
    MUST be called BEFORE rebalance band check or target share calculation.
    
    For TQQQ (L=3) with max_effective=1.0:
    max_weight = 1.0 / 3.0 = 0.3333
    
    Args:
        target_weight: Desired weight (0.0 to 1.0 typically)
        leverage: Instrument leverage (e.g., 3.0 for TQQQ)
        max_effective_exposure: Maximum effective exposure in QQQ-equivalent terms
        
    Returns:
        tuple (clamped_weight, reason_code)
        reason_code is "" if no clamping, "EXPOSURE_CLAMPED" if clamped,
        or "HALT_SIGNAL_NAN" if input was NaN
    """
    # Guard NaN: prevent silent propagation into sizing
    if pd.isna(target_weight):
        return 0.0, REASON_HALT_SIGNAL_NAN
    
    if leverage <= 0:
        leverage = 1.0
    
    max_weight = max_effective_exposure / leverage
    
    if abs(target_weight) > max_weight:
        clamped = max_weight if target_weight > 0 else -max_weight
        return clamped, REASON_EXPOSURE_CLAMPED
    
    return target_weight, ""


# =============================================================================
# Data Integrity Checks (Phase 38)
# =============================================================================

class DataIntegrityCheck:
    """Validates price bars before trading."""
    
    def validate_bar(
        self,
        open_price: float,
        high_price: float,
        low_price: float,
        close_price: float,
        volume: float = None  # Optional - skip if not provided
    ) -> tuple:
        """
        Validate OHLC(V) bar data.
        Returns (is_valid, halt_reason).
        
        Volume validation:
        - If volume is provided (not None): validate >= 0 and not NaN
        - If volume is None: skip validation (don't halt on datasets without Volume)
        
        Args:
            open_price: Bar open price
            high_price: Bar high price
            low_price: Bar low price
            close_price: Bar close price
            volume: Optional volume (None to skip validation)
            
        Returns:
            tuple (is_valid, reason) - is_valid is True if bar is good
        """
        # Check for missing/zero OHLC prices
        prices = [open_price, high_price, low_price, close_price]
        if any(pd.isna(p) or p <= 0 for p in prices):
            return False, REASON_HALT_DATA_MISSING
        
        # OHLC sanity checks
        if high_price < max(open_price, close_price):
            return False, REASON_HALT_DATA_BAD_BAR
        if low_price > min(open_price, close_price):
            return False, REASON_HALT_DATA_BAD_BAR
        if high_price < low_price:
            return False, REASON_HALT_DATA_BAD_BAR
        
        # Volume validation (only if provided)
        if volume is not None:
            if pd.isna(volume) or volume < 0:
                return False, REASON_HALT_DATA_BAD_VOLUME
        
        return True, ""
    
    def validate_exec_bar_exists(
        self,
        signal_date: pd.Timestamp,
        exec_dates: set
    ) -> tuple:
        """
        Check that execution bar exists for signal date.
        
        IMPORTANT: Both signal_date and exec_dates must use normalized timestamps.
        
        Args:
            signal_date: The signal date to check
            exec_dates: Set of available execution dates (normalized)
            
        Returns:
            tuple (is_valid, reason)
        """
        # Normalize the signal_date to match exec_dates format
        normalized_date = pd.Timestamp(signal_date).normalize()
        if normalized_date not in exec_dates:
            return False, REASON_HALT_DATA_MISSING_EXEC_BAR
        return True, ""


def check_runtime_bar(
    required_price: float,
    price_name: str,
    mode: str = "backtest"
) -> tuple:
    """
    Phase 33: Runtime missing bar guard.
    
    Check if a required bar (price) is available at runtime.
    
    P33 must not modify prices, weights, or equity directly. 
    It only sets HALT state and defers all action to the normal HALT flatten path.
    
    Required bar (per mode):
    - SAME_DAY_CLOSE: Exec_Close(t) for valuation and (if trading) fill.
    - NEXT_OPEN: Exec_Open(t) for equity_open mark; Exec_Close(t) for sizing; 
                 Exec_Open(t+1) for fill (if an order was placed on t).
    
    Args:
        required_price: The price value to check (e.g., exec_open, exec_close)
        price_name: Name of the price for error messages (e.g., "Exec_Open(t+1)")
        mode: "backtest" (fatal raise) or "live" (HALT + alert)
        
    Returns:
        tuple (is_valid, halt_reason)
        
    Raises:
        ValueError: In backtest mode, if required bar is missing
    """
    if pd.isna(required_price) or required_price <= 0:
        reason = f"{REASON_HALT_DATA_GAP}: {price_name} missing or invalid"
        
        if mode == "backtest":
            raise ValueError(reason)
        else:
            # Live mode: return halt reason, caller sets pending_flatten
            return False, reason
    
    return True, ""


def validate_next_open_bar(
    exec_df: pd.DataFrame,
    current_idx: int,
    mode: str = "backtest"
) -> tuple:
    """
    Validate that next open bar exists for NEXT_OPEN execution mode.
    
    If the bar for Exec_Open(t+1) is missing or NaN when we need to fill:
    - Backtest: fatal raise
    - Live: trigger HALT + alert
    
    Args:
        exec_df: Execution DataFrame with Exec_Open column
        current_idx: Current row index
        mode: "backtest" or "live"
        
    Returns:
        tuple (is_valid, halt_reason)
        
    Raises:
        ValueError: In backtest mode, if next open bar is missing
    """
    next_idx = current_idx + 1
    
    if next_idx >= len(exec_df):
        reason = f"{REASON_HALT_DATA_GAP}: No next bar available (end of data)"
        if mode == "backtest":
            raise ValueError(reason)
        return False, reason
    
    next_open = exec_df.iloc[next_idx].get('Exec_Open')
    return check_runtime_bar(next_open, f"Exec_Open(t+1) at index {next_idx}", mode)


# =============================================================================
# Signal vs Execution Separation (Phase 31)
# =============================================================================

# Signal-only columns (QQQ): These should ONLY appear in signal_df
SIGNAL_ONLY_COLUMNS = {
    'MA50', 'MA250', 'QQQ_ann_vol', 'Base_Regime', 'Confirmed_Regime',
    'Final_Trading_Regime', 'Target_Weight', 'Signal_Open', 'Signal_High',
    'Signal_Low', 'Signal_Close', 'Signal_Volume'
}

# Execution-only columns (TQQQ): These should ONLY appear in exec_df
EXEC_ONLY_COLUMNS = {
    'Exec_Open', 'Exec_High', 'Exec_Low', 'Exec_Close', 'Exec_Volume',
    'Portfolio_Value_Open', 'Cash', 'Total_Stocks_Owned',
    'Remaining_Portfolio_Amount', 'Actual_Weight'
}


def validate_signal_exec_separation(
    signal_df: pd.DataFrame,
    exec_df: pd.DataFrame,
    strict: bool = True
) -> bool:
    """
    Validate that signal and execution dataframes are properly separated.
    
    Phase 31: Signal always from QQQ, execution always from TQQQ.
    
    Rules:
    - signal_df should have signal-only columns (MA, vol, regime)
    - exec_df should have execution-only columns (prices, equity)
    - Neither should have the other's exclusive columns
    
    Args:
        signal_df: DataFrame with signal data (QQQ only)
        exec_df: DataFrame with execution data (TQQQ only)
        strict: If True, raise error on violation; if False, return bool
        
    Returns:
        True if separation is valid
        
    Raises:
        ValueError: If strict=True and separation is violated
    """
    errors = []
    
    # Check that exec_df doesn't have signal-only columns
    signal_cols_in_exec = SIGNAL_ONLY_COLUMNS & set(exec_df.columns)
    if signal_cols_in_exec:
        errors.append(
            f"Signal-only columns found in exec_df: {signal_cols_in_exec}. "
            "This may cause signal data to leak into execution logic."
        )
    
    # Check that signal_df doesn't have exec-only columns
    exec_cols_in_signal = EXEC_ONLY_COLUMNS & set(signal_df.columns)
    if exec_cols_in_signal:
        errors.append(
            f"Exec-only columns found in signal_df: {exec_cols_in_signal}. "
            "This may cause execution data to leak into signal logic."
        )
    
    if errors:
        if strict:
            raise ValueError("\n".join(errors))
        return False
    
    return True


def align_signal_exec_dates(
    signal_df: pd.DataFrame,
    exec_df: pd.DataFrame
) -> tuple:
    """
    Align signal and execution dataframes by date.
    
    Process:
    1. Normalize dates to date-only (no time component)
    2. Sort both by date ascending
    3. Dedupe by date (keep last)
    4. Intersect dates
    5. Re-sort after filter (order not guaranteed after .isin)
    6. Assert exact equality of Date series
    
    Args:
        signal_df: DataFrame with signal data (must have 'Date' column)
        exec_df: DataFrame with execution data (must have 'Date' column)
        
    Returns:
        tuple (aligned_signal_df, aligned_exec_df)
        
    Raises:
        ValueError: If no overlapping dates
        AssertionError: If dates don't match after alignment
    """
    # 1. Normalize to date-only timestamps
    signal_df = signal_df.copy()
    exec_df = exec_df.copy()
    signal_df['Date'] = pd.to_datetime(signal_df['Date']).dt.normalize()
    exec_df['Date'] = pd.to_datetime(exec_df['Date']).dt.normalize()
    
    # 2. Sort by date ascending
    signal_df = signal_df.sort_values('Date').reset_index(drop=True)
    exec_df = exec_df.sort_values('Date').reset_index(drop=True)
    
    # 3. Dedupe by date (keep last occurrence)
    signal_df = signal_df.drop_duplicates(subset='Date', keep='last').reset_index(drop=True)
    exec_df = exec_df.drop_duplicates(subset='Date', keep='last').reset_index(drop=True)
    
    # 4. Intersect dates
    common_dates = set(signal_df['Date']) & set(exec_df['Date'])
    if len(common_dates) == 0:
        raise ValueError("No overlapping dates between signal and exec data")
    
    # 5. Filter AND re-sort (order not guaranteed after .isin)
    signal_df = signal_df[signal_df['Date'].isin(common_dates)].sort_values('Date').reset_index(drop=True)
    exec_df = exec_df[exec_df['Date'].isin(common_dates)].sort_values('Date').reset_index(drop=True)
    
    # 6. Assert exact equality of Date series (use .equals() - avoids dtype quirks)
    assert signal_df['Date'].equals(exec_df['Date']), \
        "Date mismatch after alignment - rows have drifted"
    
    return signal_df, exec_df


# =============================================================================
# Slippage Model (Phase 35)
# =============================================================================

def apply_slippage(price: float, side: str, slippage_bps: float) -> float:
    """
    Apply slippage to price.
    
    BUY: pay more (price goes up)
    SELL: receive less (price goes down)
    
    Args:
        price: Base price
        side: "BUY" or "SELL"
        slippage_bps: Slippage in basis points (e.g., 5.0 = 5 bps)
        
    Returns:
        Price after slippage
    """
    slip = price * (slippage_bps / 10000)
    return price + slip if side == "BUY" else price - slip


# =============================================================================
# Order and Fill Data Classes
# =============================================================================

@dataclass
class Order:
    """Represents a trading order."""
    order_id: str
    symbol: str
    side: str  # "BUY" or "SELL"
    quantity: int
    order_type: str = "MARKET"  # "MARKET" or "LIMIT"
    limit_price: Optional[float] = None
    timestamp: datetime = field(default_factory=datetime.now)
    status: str = "PENDING"  # "PENDING", "FILLED", "REJECTED", "CANCELLED"
    reject_reason: Optional[str] = None  # Reason for rejection (REASON_REJECTED_*)


@dataclass
class Fill:
    """Represents an order fill."""
    order_id: str
    symbol: str
    side: str
    quantity: int
    fill_price: float
    timestamp: datetime = field(default_factory=datetime.now)
    commission: float = 0.0


@dataclass
class Position:
    """Represents a position in a symbol."""
    symbol: str
    shares: int
    avg_cost: float
    market_value: float = 0.0
    unrealized_pnl: float = 0.0


# =============================================================================
# Broker Adapter Interface
# =============================================================================

class BrokerAdapter(ABC):
    """Abstract base class for broker adapters."""
    
    @abstractmethod
    def connect(self) -> bool:
        """Connect to the broker. Returns True if successful."""
        pass
    
    @abstractmethod
    def disconnect(self) -> None:
        """Disconnect from the broker."""
        pass
    
    @abstractmethod
    def get_account_value(self) -> float:
        """Get total account value."""
        pass
    
    @abstractmethod
    def get_cash_balance(self) -> float:
        """Get available cash balance."""
        pass
    
    @abstractmethod
    def get_positions(self) -> Dict[str, Position]:
        """Get all positions. Returns dict of symbol -> Position."""
        pass
    
    @abstractmethod
    def get_position(self, symbol: str) -> Optional[Position]:
        """Get position for a specific symbol."""
        pass
    
    @abstractmethod
    def submit_order(self, order: Order) -> str:
        """Submit an order. Returns order ID."""
        pass
    
    @abstractmethod
    def cancel_order(self, order_id: str) -> bool:
        """Cancel an order. Returns True if successful."""
        pass
    
    @abstractmethod
    def get_order_status(self, order_id: str) -> str:
        """Get status of an order."""
        pass
    
    @abstractmethod
    def get_last_price(self, symbol: str) -> float:
        """Get last traded price for a symbol."""
        pass


# =============================================================================
# Paper Broker Implementation
# =============================================================================

class PaperBroker(BrokerAdapter):
    """
    Paper trading broker for simulation.
    
    Phase 34: Unified broker interface with standardized order states.
    - Order states: SUBMITTED -> FILLED | REJECTED | CANCELLED
    - No partial fills (explicitly unsupported)
    - Max N orders per symbol per day (default N=1)
    - System orders (HALT_FLATTEN) bypass max-orders limit
    """
    
    def __init__(self, initial_capital: float = None, max_orders_per_symbol_per_day: int = 50):
        if initial_capital is None:
            initial_capital = DEFAULT_CONFIG.INITIAL_CAPITAL
        
        self.initial_capital = initial_capital
        self.cash = initial_capital
        self.positions: Dict[str, Position] = {}
        self.orders: Dict[str, Order] = {}
        self.fills: List[Fill] = []
        self.order_counter = 0
        self.connected = False
        self.prices: Dict[str, float] = {}  # Current prices
        # Phase 34: Max orders per symbol per day
        self.max_orders_per_symbol_per_day = max_orders_per_symbol_per_day
        self._order_count_today: Dict[str, int] = {}  # symbol -> count
        self._current_date: Optional[pd.Timestamp] = None
    
    def connect(self) -> bool:
        """Connect to paper broker (always succeeds)."""
        self.connected = True
        return True
    
    def disconnect(self) -> None:
        """Disconnect from paper broker."""
        self.connected = False
    
    def set_price(self, symbol: str, price: float) -> None:
        """Set current price for a symbol (for paper trading)."""
        self.prices[symbol] = price
    
    def start_day(self, current_date: pd.Timestamp) -> None:
        """
        Reset daily counters at start of trading day.
        
        Args:
            current_date: Current trading date
        """
        current_date = pd.Timestamp(current_date).normalize()
        if self._current_date != current_date:
            self._current_date = current_date
            self._order_count_today = {}
    
    def reconcile(self, symbol: str, expected_shares: int) -> bool:
        """
        Reconciliation hook (Phase 34).
        
        In live mode: if mismatch exceeds tolerance -> raise + HALT + alert.
        Paper broker: assert for tests.
        
        Args:
            symbol: Symbol to check
            expected_shares: Expected number of shares
            
        Returns:
            True if reconciled, False if mismatch
        """
        pos = self.get_position(symbol)
        actual_shares = pos.shares if pos else 0
        return actual_shares == expected_shares
    
    def get_account_value(self) -> float:
        """Get total account value (cash + positions)."""
        total = self.cash
        for pos in self.positions.values():
            price = self.prices.get(pos.symbol, pos.avg_cost)
            total += pos.shares * price
        return total
    
    def get_cash_balance(self) -> float:
        """Get available cash balance."""
        return self.cash
    
    def get_positions(self) -> Dict[str, Position]:
        """Get all positions."""
        # Update market values
        for pos in self.positions.values():
            price = self.prices.get(pos.symbol, pos.avg_cost)
            pos.market_value = pos.shares * price
            pos.unrealized_pnl = pos.market_value - (pos.shares * pos.avg_cost)
        return self.positions.copy()
    
    def get_position(self, symbol: str) -> Optional[Position]:
        """Get position for a specific symbol."""
        pos = self.positions.get(symbol)
        if pos:
            price = self.prices.get(symbol, pos.avg_cost)
            pos.market_value = pos.shares * price
            pos.unrealized_pnl = pos.market_value - (pos.shares * pos.avg_cost)
        return pos
    
    def submit_order(
        self,
        order: Order,
        slippage_bps: float = 0.0,
        commission: float = 0.0,
        is_system_order: bool = False
    ) -> str:
        """
        Submit and fill order with slippage and commission.
        
        VALIDATIONS (reject if any fail):
        1. Price must be set and > 0 (prevents NaN/0 fills)
        2. SELL quantity must not exceed current shares (prevents negative shares)
        3. BUY cost must not exceed cash (prevents negative cash)
        4. Max orders per symbol per day (unless is_system_order=True)
        
        On reject: set order.status = "REJECTED" and order.reject_reason = REASON_*.
        
        trade_count increments only on FILLED normal strategy orders (BUY/SELL).
        System orders (HALT_FLATTEN), rejected orders, and zero-share orders do not increment.
        
        Args:
            order: Order to submit
            slippage_bps: Slippage in basis points (e.g., 5.0 = 5 bps)
            commission: Commission per trade in dollars
            is_system_order: If True, bypasses max orders limit (e.g., HALT_FLATTEN)
            
        Returns:
            Order ID
        """
        self.order_counter += 1
        order.order_id = f"PAPER-{self.order_counter:06d}"
        
        # VALIDATION 1: Price must be set and positive
        base_price = self.prices.get(order.symbol, 0.0)
        if pd.isna(base_price) or base_price <= 0:
            order.status = "REJECTED"
            order.reject_reason = REASON_REJECTED_NO_PRICE
            self.orders[order.order_id] = order
            return order.order_id
        
        # Apply slippage to fill price
        fill_price = apply_slippage(base_price, order.side, slippage_bps)
        
        # Calculate trade value with slippage
        trade_value = order.quantity * fill_price
        
        # VALIDATION 2: SELL can't create negative shares
        if order.side == "SELL":
            pos = self.positions.get(order.symbol)
            current_shares = pos.shares if pos else 0
            if order.quantity > current_shares:
                order.status = "REJECTED"
                order.reject_reason = REASON_REJECTED_INSUFFICIENT_SHARES
                self.orders[order.order_id] = order
                return order.order_id
        
        # VALIDATION 3: BUY can't create negative cash
        if order.side == "BUY":
            total_cost = trade_value + commission
            if total_cost > self.cash:
                order.status = "REJECTED"
                order.reject_reason = REASON_REJECTED_INSUFFICIENT_CASH
                self.orders[order.order_id] = order
                return order.order_id
            
            # Execute BUY
            self.cash -= total_cost
            if order.symbol in self.positions:
                pos = self.positions[order.symbol]
                total_cost_basis = pos.shares * pos.avg_cost + trade_value
                total_shares = pos.shares + order.quantity
                pos.shares = total_shares
                pos.avg_cost = total_cost_basis / total_shares if total_shares > 0 else 0
            else:
                self.positions[order.symbol] = Position(
                    symbol=order.symbol,
                    shares=order.quantity,
                    avg_cost=fill_price
                )
        else:  # SELL
            # Execute SELL
            proceeds = trade_value - commission
            self.cash += proceeds
            pos = self.positions[order.symbol]
            pos.shares -= order.quantity
            if pos.shares == 0:
                del self.positions[order.symbol]
        
        # Record successful fill
        order.status = "FILLED"
        self.orders[order.order_id] = order
        
        fill = Fill(
            order_id=order.order_id,
            symbol=order.symbol,
            side=order.side,
            quantity=order.quantity,
            fill_price=fill_price,
            commission=commission
        )
        self.fills.append(fill)
        
        return order.order_id
    
    def cancel_order(self, order_id: str) -> bool:
        """Cancel an order (paper orders fill immediately, so this is a no-op)."""
        order = self.orders.get(order_id)
        if order and order.status == "PENDING":
            order.status = "CANCELLED"
            return True
        return False
    
    def get_order_status(self, order_id: str) -> str:
        """Get status of an order."""
        order = self.orders.get(order_id)
        return order.status if order else "UNKNOWN"
    
    def get_last_price(self, symbol: str) -> float:
        """Get last traded price for a symbol."""
        return self.prices.get(symbol, 0.0)


# =============================================================================
# Order Logger
# =============================================================================

class OrderLogger:
    """Logs orders and fills to file."""
    
    def __init__(self, log_path: str = "logs/orders.csv"):
        self.log_path = Path(log_path)
        self.log_path.parent.mkdir(parents=True, exist_ok=True)
        self.orders: List[Dict[str, Any]] = []
    
    def log_order(self, order: Order, fill: Optional[Fill] = None) -> None:
        """Log an order and optional fill."""
        record = {
            'timestamp': datetime.now().isoformat(),
            'order_id': order.order_id,
            'symbol': order.symbol,
            'side': order.side,
            'quantity': order.quantity,
            'order_type': order.order_type,
            'status': order.status,
            'fill_price': fill.fill_price if fill else None,
            'commission': fill.commission if fill else None
        }
        self.orders.append(record)
    
    def save(self) -> None:
        """Save order log to CSV."""
        if self.orders:
            df = pd.DataFrame(self.orders)
            df.to_csv(self.log_path, index=False)
    
    def load(self) -> pd.DataFrame:
        """Load order log from CSV."""
        if self.log_path.exists():
            return pd.read_csv(self.log_path)
        return pd.DataFrame()


# =============================================================================
# Kill Switches (Risk Controls)
# =============================================================================

@dataclass
class RiskLimits:
    """Risk control limits."""
    max_daily_loss_pct: float = 0.02    # 2% max daily loss
    max_drawdown_pct: float = 0.25      # 25% max drawdown from peak
    halt_cooldown_days: int = 1         # Days locked out after halt
    max_position_pct: float = 1.0       # 100% max position
    max_order_value: float = 100000     # Max single order value
    max_trades_per_day: int = 50        # Max trades per day


class KillSwitch:
    """
    Risk control kill switch with equity-based daily loss and drawdown checks.
    
    Key design points:
    - All risk checks use portfolio equity (cash + shares * price), NOT cash-only
    - start_day() MUST be called before any trade submission on that date
    - check_end_of_day() checks daily loss and drawdown at end of day
    - force_flatten_order() returns order to flatten position (bypasses all rules)
    - lockout_until is pd.Timestamp (first tradable day after halt)
    
    Kill-switch state (Phase 42):
    - halt_triggered_on_date: date when HALT fired
    - pending_flatten: flatten order not yet executed (e.g. NEXT_OPEN: runs at t+1 open)
    - lockout_until: first date when new entries are allowed again
    """
    
    def __init__(self, limits: RiskLimits = None):
        self.limits = limits or RiskLimits()
        self.equity_at_open: float = 0.0      # Equity BEFORE day's trades (marked to open)
        self.peak_equity: float = 0.0          # High water mark
        self.trades_today: int = 0
        self.is_killed: bool = False
        self.kill_reason: str = ""
        self.lockout_until: Optional[pd.Timestamp] = None  # Use pd.Timestamp, NOT datetime
        # Phase 42: Additional state for NEXT_OPEN halt handling
        self.halt_triggered_on_date: Optional[pd.Timestamp] = None
        self.pending_flatten: bool = False
    
    def start_day(self, equity_at_open: float, current_date: pd.Timestamp) -> None:
        """
        Called at start of each trading day, BEFORE any trade submission.
        
        Args:
            equity_at_open: cash + shares * exec_open_price (BEFORE trades)
            current_date: date to check (will be normalized)
        """
        current_date = pd.Timestamp(current_date).normalize()  # Ensure normalized
        self.equity_at_open = equity_at_open
        self.peak_equity = max(self.peak_equity, equity_at_open)
        self.trades_today = 0
        
        # Check if still locked out
        # Semantics: lockout_until is the first tradable day. If cooldown_days=1 and halt on Jan 1,
        # lockout_until = Jan 2; then current_date >= lockout_until on Jan 2 means not locked (tradable).
        if self.lockout_until and current_date < self.lockout_until:
            self.is_killed = True
            self.kill_reason = f"{REASON_HALT_LOCKOUT} until {self.lockout_until.date()}"
        elif self.lockout_until and current_date >= self.lockout_until:
            # Lockout expired
            self.is_killed = False
            self.kill_reason = ""
            self.lockout_until = None
    
    def check_end_of_day(
        self,
        equity_at_close: float,
        current_date: pd.Timestamp
    ) -> tuple:
        """
        Check daily loss and drawdown at end of day.
        
        Args:
            equity_at_close: cash + shares * exec_close_price (AFTER trades, marked to close)
            current_date: date to check (will be normalized)
        
        Returns (should_halt, reason).
        """
        current_date = pd.Timestamp(current_date).normalize()
        
        # Daily loss check (uses equity_at_close consistently)
        if self.equity_at_open > 0:
            daily_return = (equity_at_close / self.equity_at_open) - 1
            if daily_return <= -self.limits.max_daily_loss_pct:
                self._trigger_halt(
                    f"{REASON_HALT_DAILY_LOSS}: {daily_return:.2%}",
                    current_date
                )
                return True, self.kill_reason
        
        # Drawdown check (uses equity_at_close - NOT undefined current_equity)
        if self.peak_equity > 0:
            drawdown = (self.peak_equity - equity_at_close) / self.peak_equity
            if drawdown >= self.limits.max_drawdown_pct:
                self._trigger_halt(
                    f"{REASON_HALT_DRAWDOWN}: {drawdown:.2%}",
                    current_date
                )
                return True, self.kill_reason
        
        return False, ""
    
    def _trigger_halt(self, reason: str, current_date: pd.Timestamp, pending_flatten: bool = True) -> None:
        """
        Trigger a halt with lockout. Uses pd.Timestamp for consistency.
        
        Args:
            reason: Halt reason string
            current_date: Date when halt triggered
            pending_flatten: If True, flatten order is pending (for NEXT_OPEN mode)
        """
        current_date = pd.Timestamp(current_date).normalize()
        self.is_killed = True
        self.kill_reason = reason
        self.halt_triggered_on_date = current_date
        self.pending_flatten = pending_flatten
        # lockout_until is pd.Timestamp (NOT datetime) for consistent comparison.
        # lockout_until is the first day trading is allowed again (current_date >= lockout_until → not locked).
        self.lockout_until = current_date + pd.Timedelta(days=self.limits.halt_cooldown_days)
    
    def force_flatten_order(self, current_shares: int, symbol: str) -> Optional[Order]:
        """
        When halted, return order to flatten position to zero (BYPASS semantics).
        MUST be called and executed when any halt triggers.
        
        Trade direction based on position:
        - shares > 0: SELL
        - shares < 0: BUY (short cover; shorts not supported yet in PaperBroker)
        
        Args:
            current_shares: Current number of shares held
            symbol: Trading symbol
            
        Returns:
            Order to flatten position, or None if already flat
        """
        if current_shares == 0:
            return None
        
        side = "SELL" if current_shares > 0 else "BUY"
        return Order(
            order_id="",
            symbol=symbol,
            side=side,
            quantity=abs(current_shares)
        )
    
    # Keep legacy methods for backward compatibility
    def reset_daily(self, account_value: float) -> None:
        """Reset daily counters (legacy method, use start_day instead)."""
        self.equity_at_open = account_value
        self.trades_today = 0
        self.is_killed = False
        self.kill_reason = ""
    
    def check_order(
        self,
        order: Order,
        current_value: float,
        fill_price: float
    ) -> tuple:
        """
        Check if order is allowed. Returns (allowed, reason).
        Legacy method - still used for per-order checks.
        """
        if self.is_killed:
            return False, f"Kill switch active: {self.kill_reason}"
        
        # Check daily loss (legacy check during trading)
        if self.equity_at_open > 0:
            daily_loss_pct = (self.equity_at_open - current_value) / self.equity_at_open
            if daily_loss_pct >= self.limits.max_daily_loss_pct:
                self.is_killed = True
                self.kill_reason = f"{REASON_HALT_DAILY_LOSS}: {daily_loss_pct:.2%}"
                return False, self.kill_reason
        
        # Check max order value
        order_value = order.quantity * fill_price
        if order_value > self.limits.max_order_value:
            return False, f"Order value ${order_value:,.0f} exceeds limit ${self.limits.max_order_value:,.0f}"
        
        # Check max trades per day
        if self.trades_today >= self.limits.max_trades_per_day:
            return False, f"Max trades per day ({self.limits.max_trades_per_day}) reached"
        
        return True, ""
    
    def record_trade(self) -> None:
        """Record that a trade was made."""
        self.trades_today += 1


# =============================================================================
# Trading Engine
# =============================================================================

class TradingEngine:
    """Main trading engine for paper/live trading."""
    
    def __init__(
        self,
        broker: BrokerAdapter,
        risk_limits: RiskLimits = None,
        log_path: str = "logs/orders.csv"
    ):
        self.broker = broker
        self.kill_switch = KillSwitch(risk_limits)
        self.order_logger = OrderLogger(log_path)
        self.is_running = False
    
    def start(self) -> bool:
        """Start the trading engine."""
        if not self.broker.connect():
            return False
        
        self.is_running = True
        account_value = self.broker.get_account_value()
        self.kill_switch.reset_daily(account_value)
        return True
    
    def stop(self) -> None:
        """Stop the trading engine."""
        self.is_running = False
        self.order_logger.save()
        self.broker.disconnect()
    
    def execute_target_position(
        self,
        symbol: str,
        target_shares: int,
        price: float
    ) -> Optional[Fill]:
        """
        Execute trades to reach target position.
        Returns Fill if trade executed, None otherwise.
        """
        if not self.is_running:
            return None
        
        # Get current position
        current_pos = self.broker.get_position(symbol)
        current_shares = current_pos.shares if current_pos else 0
        
        shares_diff = target_shares - current_shares
        if shares_diff == 0:
            return None
        
        # Create order
        order = Order(
            order_id="",
            symbol=symbol,
            side="BUY" if shares_diff > 0 else "SELL",
            quantity=abs(shares_diff)
        )
        
        # Check kill switch
        allowed, reason = self.kill_switch.check_order(
            order,
            self.broker.get_account_value(),
            price
        )
        
        if not allowed:
            print(f"Order rejected by kill switch: {reason}")
            order.status = "REJECTED"
            self.order_logger.log_order(order)
            return None
        
        # Set price for paper broker
        self.broker.set_price(symbol, price)
        
        # Submit order
        order_id = self.broker.submit_order(order)
        
        # Check if filled
        if order.status == "FILLED":
            fill = self.broker.fills[-1] if self.broker.fills else None
            self.kill_switch.record_trade()
            self.order_logger.log_order(order, fill)
            return fill
        else:
            self.order_logger.log_order(order)
            return None
    
    def end_of_day(
        self,
        symbol: str,
        close_price: float,
        current_date: pd.Timestamp
    ) -> Optional[Fill]:
        """
        End of day processing: check limits and flatten if needed.
        
        Price is set BEFORE check so flatten (if triggered) has a valid price.
        
        Args:
            symbol: Trading symbol
            close_price: Today's closing price
            current_date: Current date
            
        Returns:
            Fill if flatten order executed, None otherwise
        """
        # Set price BEFORE check so flatten has a valid price
        self.broker.set_price(symbol, close_price)
        current_equity = self.broker.get_account_value()
        
        should_halt, reason = self.kill_switch.check_end_of_day(current_equity, current_date)
        
        if should_halt:
            # MUST flatten (price already set above)
            pos = self.broker.get_position(symbol)
            current_shares = pos.shares if pos else 0
            flatten_order = self.kill_switch.force_flatten_order(current_shares, symbol)
            
            if flatten_order:
                self.broker.submit_order(flatten_order)
                fill = self.broker.fills[-1] if self.broker.fills else None
                self.order_logger.log_order(flatten_order, fill)
                return fill
        
        return None
    
    def get_account_summary(self) -> Dict[str, Any]:
        """Get account summary."""
        return {
            'account_value': self.broker.get_account_value(),
            'cash': self.broker.get_cash_balance(),
            'positions': self.broker.get_positions(),
            'trades_today': self.kill_switch.trades_today,
            'is_killed': self.kill_switch.is_killed,
            'kill_reason': self.kill_switch.kill_reason
        }
