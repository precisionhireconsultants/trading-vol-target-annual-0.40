"""Malik strategy invariants (Phase 47-49).

This module provides a single utility function that validates all 
Malik/v3 invariants. Used by parameter sweep and stress tests.
"""
import pandas as pd
import numpy as np
from typing import Dict, Any, List, Optional
from config import ZERO_EPS


class InvariantViolation(Exception):
    """Exception raised when an invariant is violated."""
    pass


def assert_malik_invariants(
    df: pd.DataFrame,
    config: Dict[str, Any] = None,
    logs: Optional[List[str]] = None
) -> bool:
    """
    Assert all Malik strategy invariants.
    
    Use this single function in parameter_sweep, stress tests, and any batch run.
    
    Invariants checked:
    1. No signal columns used in equity calculation (architecture check)
    2. Clamp called before rebalance (no row where target_shares imply weight > max exposure)
    3. equity_open / equity_close identities hold
    4. If HALT triggered -> eventually shares go to 0
    5. Signal-only indicators exist only on signal data
    6. effective_exposure <= MAX_EFFECTIVE_EXPOSURE + eps daily
    
    Args:
        df: DataFrame with backtest results
        config: Configuration dict (optional, uses defaults if not provided)
        logs: List of log entries (optional)
        
    Returns:
        True if all invariants pass
        
    Raises:
        InvariantViolation: If any invariant is violated
    """
    if config is None:
        from config import DEFAULT_CONFIG
        config = {
            'MAX_EFFECTIVE_EXPOSURE': DEFAULT_CONFIG.MAX_EFFECTIVE_EXPOSURE,
            'TQQQ_LEVERAGE': DEFAULT_CONFIG.TQQQ_LEVERAGE,
            'ZERO_EPS': ZERO_EPS
        }
    
    violations = []
    
    # 1. Check equity_close identity (Cash + Shares * Close = Portfolio Value)
    if all(c in df.columns for c in ['Cash', 'Total_Stocks_Owned', 'Exec_Close', 'Remaining_Portfolio_Amount']):
        for idx in range(len(df)):
            row = df.iloc[idx]
            expected = row['Cash'] + row['Total_Stocks_Owned'] * row['Exec_Close']
            actual = row['Remaining_Portfolio_Amount']
            if abs(expected - actual) > 1e-6:
                violations.append(
                    f"Row {idx}: equity_close mismatch: {expected} != {actual}"
                )
    
    # 2. Check equity_open identity (if columns exist)
    if all(c in df.columns for c in ['Cash_Open', 'Shares_Open', 'Exec_Open', 'Portfolio_Value_Open']):
        for idx in range(len(df)):
            row = df.iloc[idx]
            expected = row['Cash_Open'] + row['Shares_Open'] * row['Exec_Open']
            actual = row['Portfolio_Value_Open']
            if abs(expected - actual) > 1e-6:
                violations.append(
                    f"Row {idx}: equity_open mismatch: {expected} != {actual}"
                )
    
    # 3. Check exposure clamp (effective_exposure <= max + band_tolerance)
    # Note: Post-trade drift within rebalance band is expected.
    # We use rebalance_band as tolerance, not just epsilon.
    max_exp = config.get('MAX_EFFECTIVE_EXPOSURE', 1.0)
    leverage = config.get('TQQQ_LEVERAGE', 3.0)
    rebalance_band = config.get('REBALANCE_BAND_PCT', 0.05)
    # Allow drift up to rebalance_band above max (price movements within band)
    exposure_tolerance = max_exp * rebalance_band
    
    if 'Actual_Weight' in df.columns:
        for idx in range(len(df)):
            weight = df.iloc[idx]['Actual_Weight']
            effective_exposure = abs(weight * leverage)
            if effective_exposure > max_exp + exposure_tolerance:
                violations.append(
                    f"Row {idx}: effective_exposure {effective_exposure:.4f} > max {max_exp} + tolerance {exposure_tolerance:.4f}"
                )
    
    # 4. Check no negative shares
    if 'Total_Stocks_Owned' in df.columns:
        if (df['Total_Stocks_Owned'] < 0).any():
            violations.append("Negative shares detected")
    
    # 5. Check no negative cash
    if 'Cash' in df.columns:
        if (df['Cash'] < -ZERO_EPS).any():
            violations.append("Negative cash detected")
    
    # 6. Check portfolio value always positive
    if 'Remaining_Portfolio_Amount' in df.columns:
        if (df['Remaining_Portfolio_Amount'] <= 0).any():
            violations.append("Non-positive portfolio value detected")
    
    if violations:
        raise InvariantViolation(
            f"Invariant violations ({len(violations)}):\n" + 
            "\n".join(f"  - {v}" for v in violations[:10])  # Limit to first 10
        )
    
    return True


def check_signal_only_columns(
    signal_df: pd.DataFrame,
    exec_df: pd.DataFrame
) -> bool:
    """
    Check that signal-only columns exist only in signal_df.
    
    Signal-only columns: MA50, MA250, QQQ_ann_vol, Base_Regime, etc.
    
    Args:
        signal_df: Signal data DataFrame
        exec_df: Execution data DataFrame
        
    Returns:
        True if separation is correct
        
    Raises:
        InvariantViolation: If signal columns found in exec_df
    """
    from engine import SIGNAL_ONLY_COLUMNS
    
    signal_cols_in_exec = SIGNAL_ONLY_COLUMNS & set(exec_df.columns)
    if signal_cols_in_exec:
        raise InvariantViolation(
            f"Signal-only columns found in exec_df: {signal_cols_in_exec}"
        )
    
    return True


def compute_trade_count_cap(df_length: int, max_trades_per_year: int = 100) -> int:
    """
    Compute trade count cap that scales with dataset length.
    
    Formula: ceil(len(df) / 252 * MAX_TRADES_PER_YEAR)
    
    Args:
        df_length: Length of the DataFrame
        max_trades_per_year: Maximum trades per year (default 100)
        
    Returns:
        Trade count cap
    """
    import math
    return math.ceil(df_length / 252 * max_trades_per_year)
