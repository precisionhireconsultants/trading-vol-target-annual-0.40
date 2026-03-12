"""Portfolio simulation and trade execution."""
import pandas as pd
import numpy as np
from dataclasses import dataclass
from typing import Dict, Any

from config import DEFAULT_CONFIG, ZERO_EPS, FLOOR_EPS

# Default initial capital
DEFAULT_INITIAL_CAPITAL = DEFAULT_CONFIG.INITIAL_CAPITAL


# =============================================================================
# Phase 43: Single-Definition Enforcement - Centralized Math Functions
# =============================================================================

def compute_equity(cash: float, shares: int, price: float) -> float:
    """
    Compute portfolio equity value.
    
    SINGLE SOURCE OF TRUTH for equity calculation.
    All code paths must use this function - no inline `cash + shares * price`.
    
    Args:
        cash: Cash balance
        shares: Number of shares held
        price: Current price per share
        
    Returns:
        Total equity (cash + shares * price)
    """
    return cash + shares * price


# =============================================================================
# Float-Safe Comparison Functions (Phase 41)
# =============================================================================

def is_near_zero(x: float) -> bool:
    """
    Check if value is effectively zero using epsilon comparison.
    
    Args:
        x: Value to check
        
    Returns:
        True if abs(x) < ZERO_EPS
    """
    return abs(x) < ZERO_EPS


def should_rebalance(target_weight: float, actual_weight: float, band: float) -> bool:
    """
    Determine if rebalancing is needed.
    
    Rules:
    - Regime switch (0 <-> non-zero) always triggers rebalance
    - Otherwise, only rebalance if drift exceeds band
    
    Args:
        target_weight: Target portfolio weight (clamped)
        actual_weight: Current actual weight
        band: Rebalance band threshold (e.g., 0.05 for 5%)
        
    Returns:
        True if rebalancing should occur
    """
    target_is_zero = is_near_zero(target_weight)
    actual_is_zero = is_near_zero(actual_weight)
    
    # Regime switch: 0 <-> non-zero always rebalances
    if target_is_zero != actual_is_zero:
        return True
    
    # Both near zero: no trade needed
    if target_is_zero and actual_is_zero:
        return False
    
    # Both non-zero: check band
    return abs(target_weight - actual_weight) > band


@dataclass
class PortfolioState:
    """Represents the current state of the portfolio."""
    cash: float = DEFAULT_INITIAL_CAPITAL
    shares: int = 0
    
    def copy(self) -> 'PortfolioState':
        """Create a copy of the current state."""
        return PortfolioState(cash=self.cash, shares=self.shares)


def init_portfolio(initial_capital: float = DEFAULT_INITIAL_CAPITAL) -> PortfolioState:
    """
    Initialize portfolio state.
    
    Args:
        initial_capital: Starting cash (default $10,000)
        
    Returns:
        PortfolioState with cash and shares initialized
    """
    return PortfolioState(cash=initial_capital, shares=0)


def add_exec_target_weight(
    df: pd.DataFrame,
    execution_mode: str = "NEXT_OPEN",
    min_weight_change: float = None
) -> pd.DataFrame:
    """
    Add Exec_Target_Weight column with proper execution timing.
    
    Modes:
        NEXT_OPEN (default, conservative):
            - Decision uses YESTERDAY's Target_Weight
            - Execution happens at TODAY's Open
            - Exec_Target_Weight = Target_Weight.shift(1), first row = 0
        
        SAME_DAY_CLOSE (Malik-style):
            - Decision uses TODAY's Target_Weight (computed at close)
            - Execution happens at TODAY's Close
            - Exec_Target_Weight = Target_Weight (no shift)
            - Note: This is closer to how Malik operates (last ~10 min of day)
    
    Trade Throttling:
        If min_weight_change > 0, small weight changes are suppressed.
        Only update Exec_Target_Weight if change exceeds threshold.
    
    Args:
        df: DataFrame with 'Target_Weight' column
        execution_mode: "NEXT_OPEN" or "SAME_DAY_CLOSE"
        min_weight_change: Minimum change to trigger rebalance (None uses config)
        
    Returns:
        DataFrame with new 'Exec_Target_Weight' column
    """
    if min_weight_change is None:
        min_weight_change = DEFAULT_CONFIG.MIN_WEIGHT_CHANGE
    
    df = df.copy()
    
    if execution_mode in ("SAME_DAY_CLOSE", "INTRADAY"):
        # Same-day execution: use today's signal, execute at today's close
        # (INTRADAY mode uses near-close Signal_Price / Exec_Price, same shift logic)
        raw_weight = df['Target_Weight'].copy()
    else:
        # Default: Next-day open execution (conservative, no look-ahead)
        raw_weight = df['Target_Weight'].shift(1).fillna(0.0)
    
    # Apply trade throttling if enabled
    if min_weight_change > 0:
        throttled_weight = apply_trade_throttling(raw_weight, min_weight_change)
        df['Exec_Target_Weight'] = throttled_weight
    else:
        df['Exec_Target_Weight'] = raw_weight
    
    return df


def apply_trade_throttling(weights: pd.Series, min_change: float) -> pd.Series:
    """
    Apply trade throttling to prevent small rebalances.
    
    Only update weight if change from previous weight exceeds threshold.
    Exception: Always allow changes to/from 0 (regime switches).
    
    Args:
        weights: Series of target weights
        min_change: Minimum change to trigger update
        
    Returns:
        Throttled weight series
    """
    result = weights.copy()
    current_weight = 0.0
    
    for i in range(len(weights)):
        target = weights.iloc[i]
        
        # Always allow regime switches (to/from 0)
        if current_weight == 0 or target == 0:
            result.iloc[i] = target
            current_weight = target
        # Only update if change exceeds threshold
        elif abs(target - current_weight) >= min_change:
            result.iloc[i] = target
            current_weight = target
        else:
            # Keep previous weight (suppress small change)
            result.iloc[i] = current_weight
    
    return result


def compute_target_shares(
    portfolio_value_open: float,
    exec_target_weight: float,
    open_price: float
) -> int:
    """
    Compute target shares based on portfolio value and target weight.
    
    Formula: target_shares = floor((portfolio_value_open * exec_target_weight) / open_price)
    
    Args:
        portfolio_value_open: Portfolio value at open (cash + shares * open_price)
        exec_target_weight: Target weight to achieve (0.0 to 1.0)
        open_price: Today's open price
        
    Returns:
        Target number of shares (integer, floored)
    """
    if open_price <= 0:
        return 0
    
    target_value = portfolio_value_open * exec_target_weight
    target_shares = int(np.floor(target_value / open_price))
    
    return max(0, target_shares)  # Never negative shares


@dataclass
class TradeResult:
    """Result of a trade execution."""
    shares_diff: int  # Positive = buy, negative = sell
    trade_type: str  # "BUY", "SELL", or ""
    fill_price: float
    notional: float  # Absolute value of trade
    new_cash: float
    new_shares: int


def execute_trade(
    state: PortfolioState,
    target_shares: int,
    open_price: float
) -> TradeResult:
    """
    Execute a trade to reach target shares.
    
    Args:
        state: Current portfolio state
        target_shares: Desired number of shares
        open_price: Execution price (today's open)
        
    Returns:
        TradeResult with trade details and new state
    """
    shares_diff = target_shares - state.shares
    
    if shares_diff == 0:
        # No trade needed
        return TradeResult(
            shares_diff=0,
            trade_type="",
            fill_price=np.nan,
            notional=0.0,
            new_cash=state.cash,
            new_shares=state.shares
        )
    
    if shares_diff > 0:
        # BUY
        cost = shares_diff * open_price
        new_cash = state.cash - cost
        new_shares = state.shares + shares_diff
        return TradeResult(
            shares_diff=shares_diff,
            trade_type="BUY",
            fill_price=open_price,
            notional=abs(cost),
            new_cash=new_cash,
            new_shares=new_shares
        )
    else:
        # SELL
        proceeds = abs(shares_diff) * open_price
        new_cash = state.cash + proceeds
        new_shares = state.shares + shares_diff  # shares_diff is negative
        return TradeResult(
            shares_diff=shares_diff,
            trade_type="SELL",
            fill_price=open_price,
            notional=proceeds,
            new_cash=new_cash,
            new_shares=new_shares
        )


def determine_rebalance_reason(
    prev_exec_weight: float,
    curr_exec_weight: float,
    trade_made: bool
) -> str:
    """
    Determine the reason code for a rebalance.
    
    Uses explicit 0↔1 detection based on Exec_Target_Weight change,
    not a 0.5 threshold. This ensures correct detection even if
    thresholds are added later.
    
    Args:
        prev_exec_weight: Previous day's Exec_Target_Weight
        curr_exec_weight: Current day's Exec_Target_Weight
        trade_made: Whether a trade was executed
        
    Returns:
        Reason code: "REGIME_SWITCH", "REBALANCE", or "NO_TRADE"
    """
    if not trade_made:
        return "NO_TRADE"
    
    # Explicit 0↔1 detection based on Exec_Target_Weight change
    # prev_in_market: True if weight > 0 (any market exposure)
    # curr_in_market: True if weight > 0 (any market exposure)
    prev_in_market = prev_exec_weight > 0.0
    curr_in_market = curr_exec_weight > 0.0
    
    if prev_in_market != curr_in_market:
        return "REGIME_SWITCH"
    
    return "REBALANCE"


@dataclass
class DailyTradeFields:
    """Trade-related fields for a single day."""
    Trade_Flag: int  # 1 if traded, 0 otherwise
    Trade_Count: int  # 1 if traded, 0 otherwise
    Net_Shares_Change: int
    Trade_Made_Type: str  # "BUY", "SELL", or ""
    Fill_Price_VWAP: float  # Open price if traded, NaN otherwise
    Total_Notional_Abs: float
    Rebalance_Reason_Code: str


def compute_trade_fields(
    trade_result: TradeResult,
    prev_exec_weight: float,
    curr_exec_weight: float
) -> DailyTradeFields:
    """
    Compute all trade-related fields for a day.
    
    Args:
        trade_result: Result from execute_trade
        prev_exec_weight: Previous day's Exec_Target_Weight
        curr_exec_weight: Current day's Exec_Target_Weight
        
    Returns:
        DailyTradeFields with all trade columns
    """
    trade_made = trade_result.shares_diff != 0
    
    return DailyTradeFields(
        Trade_Flag=1 if trade_made else 0,
        Trade_Count=1 if trade_made else 0,
        Net_Shares_Change=trade_result.shares_diff,
        Trade_Made_Type=trade_result.trade_type,
        Fill_Price_VWAP=trade_result.fill_price,
        Total_Notional_Abs=trade_result.notional,
        Rebalance_Reason_Code=determine_rebalance_reason(
            prev_exec_weight, curr_exec_weight, trade_made
        )
    )


@dataclass
class DailyHoldings:
    """Holdings-related fields for a single day."""
    Total_Stocks_Owned: int
    Cash: float


def compute_holdings(trade_result: TradeResult) -> DailyHoldings:
    """
    Compute holdings columns after trade execution.
    
    Args:
        trade_result: Result from execute_trade
        
    Returns:
        DailyHoldings with Total_Stocks_Owned and Cash
    """
    return DailyHoldings(
        Total_Stocks_Owned=trade_result.new_shares,
        Cash=trade_result.new_cash
    )


def compute_eod_valuation(shares: int, cash: float, close_price: float) -> float:
    """
    Compute end-of-day portfolio valuation.
    
    Formula: Remaining_Portfolio_Amount = Cash + Total_Stocks_Owned * Close
    
    Args:
        shares: Number of shares owned
        cash: Cash balance
        close_price: Today's closing price
        
    Returns:
        Total portfolio value at end of day
    """
    return cash + (shares * close_price)


def compute_actual_weight(shares: int, close_price: float, portfolio_value: float) -> float:
    """
    Compute actual weight of stock holdings.
    
    Formula: Actual_Weight = (Total_Stocks_Owned * Close) / Remaining_Portfolio_Amount
    
    Args:
        shares: Number of shares owned
        close_price: Today's closing price
        portfolio_value: Total portfolio value (from compute_eod_valuation)
        
    Returns:
        Actual weight (0.0 to 1.0), or 0.0 if portfolio_value is zero
    """
    if portfolio_value <= 0:
        return 0.0
    
    stock_value = shares * close_price
    return stock_value / portfolio_value
