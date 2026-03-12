"""Metrics computation for trading strategy (Phase 46).

This module provides functions to compute baseline metrics:
- Max drawdown
- Worst 20-day return (compounded)
- Trade count
- Exposure days/percentage
- Final equity
"""
import pandas as pd
import numpy as np
from typing import Dict, Any


def compute_max_drawdown(equity: pd.Series) -> float:
    """
    Compute maximum drawdown from equity series.
    
    Formula: max_drawdown = max((cummax - equity) / cummax)
    
    Args:
        equity: Series of equity values (e.g., Remaining_Portfolio_Amount)
        
    Returns:
        Maximum drawdown as a decimal (e.g., 0.25 for 25%)
    """
    if len(equity) == 0:
        return 0.0
    
    cummax = equity.cummax()
    drawdown = (cummax - equity) / cummax
    # Handle division by zero (equity starts at 0)
    drawdown = drawdown.replace([np.inf, -np.inf], 0.0).fillna(0.0)
    return float(drawdown.max())


def compute_worst_20d_return(equity: pd.Series) -> float:
    """
    Compute worst 20-day return (compounded, not sum of daily returns).
    
    Uses rolling product formula:
    (1 + returns).rolling(20).apply(np.prod, raw=True) - 1
    
    This is the correct equity-based definition, not sum of daily returns.
    
    Args:
        equity: Series of equity values
        
    Returns:
        Worst 20-day return as a decimal (e.g., -0.30 for -30%)
    """
    if len(equity) < 20:
        return 0.0
    
    returns = equity.pct_change()
    # Compound returns over 20-day windows
    rolling_20d = (1 + returns).rolling(20).apply(np.prod, raw=True) - 1
    
    worst = rolling_20d.min()
    return float(worst) if not pd.isna(worst) else 0.0


def compute_trade_count(df: pd.DataFrame, column: str = 'Trade_Flag') -> int:
    """
    Compute total trade count.
    
    Args:
        df: DataFrame with trade flag column
        column: Column name for trade flag (default 'Trade_Flag')
        
    Returns:
        Total number of trades
    """
    if column not in df.columns:
        return 0
    return int(df[column].sum())


def compute_exposure_metrics(
    df: pd.DataFrame,
    weight_column: str = 'Actual_Weight',
    threshold: float = 0.01
) -> Dict[str, Any]:
    """
    Compute exposure metrics.
    
    Args:
        df: DataFrame with weight column
        weight_column: Column name for actual weight
        threshold: Minimum weight to consider as exposed
        
    Returns:
        Dict with exposure_days and exposure_pct
    """
    if weight_column not in df.columns:
        return {'exposure_days': 0, 'exposure_pct': 0.0}
    
    exposed = (df[weight_column] > threshold).sum()
    total = len(df)
    pct = exposed / total if total > 0 else 0.0
    
    return {
        'exposure_days': int(exposed),
        'exposure_pct': float(pct)
    }


def compute_baseline_metrics(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Compute all baseline metrics for a backtest result.
    
    Metrics:
    - max_drawdown_pct: Maximum drawdown from peak
    - worst_20d_return: Worst 20-day compounded return
    - trade_count: Total number of trades
    - exposure_days: Days with exposure > 1%
    - exposure_pct: Percentage of days with exposure
    - final_equity: Final portfolio value
    - initial_equity: Initial portfolio value
    - total_return: (final - initial) / initial
    
    Args:
        df: DataFrame with backtest results
        
    Returns:
        Dict with all metrics
    """
    equity_col = 'Remaining_Portfolio_Amount'
    
    if equity_col not in df.columns:
        raise ValueError(f"Missing required column: {equity_col}")
    
    equity = df[equity_col]
    
    # Core metrics
    max_drawdown = compute_max_drawdown(equity)
    worst_20d = compute_worst_20d_return(equity)
    trade_count = compute_trade_count(df)
    exposure = compute_exposure_metrics(df)
    
    # Equity metrics
    initial_equity = float(equity.iloc[0]) if len(equity) > 0 else 0.0
    final_equity = float(equity.iloc[-1]) if len(equity) > 0 else 0.0
    total_return = (final_equity - initial_equity) / initial_equity if initial_equity > 0 else 0.0
    
    return {
        'max_drawdown_pct': max_drawdown,
        'worst_20d_return': worst_20d,
        'trade_count': trade_count,
        'exposure_days': exposure['exposure_days'],
        'exposure_pct': exposure['exposure_pct'],
        'initial_equity': initial_equity,
        'final_equity': final_equity,
        'total_return': total_return
    }


def format_metrics_report(metrics: Dict[str, Any]) -> str:
    """
    Format metrics as a human-readable report.
    
    Args:
        metrics: Dict from compute_baseline_metrics
        
    Returns:
        Formatted string report
    """
    lines = [
        "=" * 50,
        "BASELINE METRICS REPORT",
        "=" * 50,
        f"Max Drawdown:     {metrics['max_drawdown_pct']:.2%}",
        f"Worst 20-day:     {metrics['worst_20d_return']:.2%}",
        f"Trade Count:      {metrics['trade_count']}",
        f"Exposure Days:    {metrics['exposure_days']}",
        f"Exposure %:       {metrics['exposure_pct']:.1%}",
        "-" * 50,
        f"Initial Equity:   ${metrics['initial_equity']:,.2f}",
        f"Final Equity:     ${metrics['final_equity']:,.2f}",
        f"Total Return:     {metrics['total_return']:.2%}",
        "=" * 50
    ]
    return "\n".join(lines)


def check_viability_gate(
    metrics: Dict[str, Any],
    max_drawdown_ceiling: float = 0.90,
    trade_count_cap: int = None,
    worst_20d_floor: float = -0.40
) -> tuple:
    """
    Check viability gate for a backtest result (Phase 49).
    
    PASS criteria:
    1. final_equity > initial_equity (after costs)
    2. max_drawdown <= ceiling (e.g., 0.90)
    3. trade_count <= trade_count_cap (if provided)
    4. worst_20d_return > floor (e.g., > -0.40)
    
    Args:
        metrics: Dict from compute_baseline_metrics
        max_drawdown_ceiling: Maximum allowed drawdown
        trade_count_cap: Maximum allowed trades (None = no limit)
        worst_20d_floor: Minimum allowed worst 20-day return
        
    Returns:
        tuple (passed: bool, reasons: list of failure reasons)
    """
    reasons = []
    
    # Check final_equity > initial_equity
    if metrics['final_equity'] <= metrics['initial_equity']:
        reasons.append(
            f"Final equity ${metrics['final_equity']:,.2f} <= "
            f"initial ${metrics['initial_equity']:,.2f}"
        )
    
    # Check max drawdown
    if metrics['max_drawdown_pct'] > max_drawdown_ceiling:
        reasons.append(
            f"Max drawdown {metrics['max_drawdown_pct']:.2%} > "
            f"ceiling {max_drawdown_ceiling:.2%}"
        )
    
    # Check trade count
    if trade_count_cap is not None and metrics['trade_count'] > trade_count_cap:
        reasons.append(
            f"Trade count {metrics['trade_count']} > cap {trade_count_cap}"
        )
    
    # Check worst 20-day
    if metrics['worst_20d_return'] < worst_20d_floor:
        reasons.append(
            f"Worst 20-day {metrics['worst_20d_return']:.2%} < "
            f"floor {worst_20d_floor:.2%}"
        )
    
    passed = len(reasons) == 0
    return passed, reasons
