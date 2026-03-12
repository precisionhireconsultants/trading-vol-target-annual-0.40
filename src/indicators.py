"""Technical indicators for trading strategy."""
import pandas as pd
import numpy as np

from config import DEFAULT_CONFIG

# Default MA windows (can be overridden in tests)
MA_LONG = DEFAULT_CONFIG.MA_LONG
MA_SHORT = DEFAULT_CONFIG.MA_SHORT

# Volatility parameters
VOL_WINDOW = DEFAULT_CONFIG.VOL_WINDOW
TRADING_DAYS_PER_YEAR = DEFAULT_CONFIG.TRADING_DAYS_PER_YEAR


def add_ma250(df: pd.DataFrame, window: int = MA_LONG, intraday_mode: bool = False) -> pd.DataFrame:
    """
    Add MA250 (Simple Moving Average) column to DataFrame.
    
    Args:
        df: DataFrame with 'Close' column
        window: Rolling window size (default 250)
        intraday_mode: When True, use Close.shift(1) so MA(D) only
            reflects closes through D-1 (no look-ahead).
        
    Returns:
        DataFrame with new 'MA250' column
    """
    df = df.copy()
    source = df['Close'].shift(1) if intraday_mode else df['Close']
    df['MA250'] = source.rolling(window=window, min_periods=window).mean()
    return df


def add_ma50(df: pd.DataFrame, window: int = MA_SHORT, intraday_mode: bool = False) -> pd.DataFrame:
    """
    Add MA50 (Simple Moving Average) column to DataFrame.
    
    Args:
        df: DataFrame with 'Close' column
        window: Rolling window size (default 50)
        intraday_mode: When True, use Close.shift(1) so MA(D) only
            reflects closes through D-1 (no look-ahead).
        
    Returns:
        DataFrame with new 'MA50' column
    """
    df = df.copy()
    source = df['Close'].shift(1) if intraday_mode else df['Close']
    df['MA50'] = source.rolling(window=window, min_periods=window).mean()
    return df


def add_annualized_volatility(df: pd.DataFrame, window: int = VOL_WINDOW, intraday_mode: bool = False) -> pd.DataFrame:
    """
    Add QQQ_ann_vol (annualized volatility) column to DataFrame.
    
    Calculation:
        1. returns = pct_change(Close)
        2. daily_vol = rolling_std(returns, window)
        3. QQQ_ann_vol = daily_vol * sqrt(252)
    
    Args:
        df: DataFrame with 'Close' column
        window: Rolling window for volatility (default 20)
        intraday_mode: When True, use Close.shift(1) so vol(D) only
            reflects closes through D-1 (no look-ahead).
        
    Returns:
        DataFrame with new 'QQQ_ann_vol' column
    """
    df = df.copy()
    
    source = df['Close'].shift(1) if intraday_mode else df['Close']
    returns = source.pct_change()
    daily_vol = returns.rolling(window=window, min_periods=window).std()
    df['QQQ_ann_vol'] = daily_vol * np.sqrt(TRADING_DAYS_PER_YEAR)
    
    return df
