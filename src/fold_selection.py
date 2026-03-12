"""Fold selection for backtesting."""
import pandas as pd
from datetime import datetime, timedelta
from typing import Tuple, Dict, Any, Optional

from config import DEFAULT_CONFIG


def select_sample_fold(
    df: pd.DataFrame,
    years: int = DEFAULT_CONFIG.FOLD_YEARS,
    start_date: Optional[datetime] = None
) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """
    Select a sample fold for backtesting.
    
    Deterministic date range selection:
        - Start: User-provided date, or first date where MA250 is not NaN
        - End: Start + years (or last available date)
    
    Args:
        df: DataFrame with 'Date' and 'MA250' columns
        years: Number of years for the fold (default from config)
        start_date: Optional start date (default: first valid MA250 date)
        
    Returns:
        Tuple of:
            - fold_df: DataFrame filtered to the fold date range
            - metadata: Dict with Fold_ID, Train_Start, Train_End, Test_Start, Test_End, Phase
            
    Raises:
        ValueError: If no valid MA250 values or start_date is invalid
    """
    df = df.copy()
    
    # Find first date where MA250 is not NaN
    valid_ma250 = df[df['MA250'].notna()]
    if len(valid_ma250) == 0:
        raise ValueError("No valid MA250 values found in DataFrame")
    
    first_valid_date = valid_ma250['Date'].iloc[0]
    
    # Use provided start_date or default to first valid MA250 date
    if start_date is not None:
        # Convert to pandas Timestamp for comparison
        start_date = pd.Timestamp(start_date)
        
        # Validate start_date is within data range
        min_date = df['Date'].min()
        max_date = df['Date'].max()
        if start_date < min_date or start_date > max_date:
            raise ValueError(
                f"start_date {start_date.date()} is outside data range "
                f"[{min_date.date()}, {max_date.date()}]"
            )
        
        # Validate start_date has valid MA250
        if start_date < first_valid_date:
            raise ValueError(
                f"start_date {start_date.date()} is before first valid MA250 date "
                f"{first_valid_date.date()}"
            )
        
        fold_start = start_date
    else:
        fold_start = first_valid_date
    
    # Calculate end date (start + years)
    end_date = fold_start + timedelta(days=years * 365)
    
    # Clamp to available data
    max_date = df['Date'].max()
    if end_date > max_date:
        end_date = max_date
    
    # Filter to fold range
    fold_df = df[(df['Date'] >= fold_start) & (df['Date'] <= end_date)].copy()
    fold_df = fold_df.reset_index(drop=True)
    
    # Add Phase column (all "test" for single fold)
    fold_df['Phase'] = "test"
    
    # Build metadata
    actual_end = fold_df['Date'].max()
    metadata = {
        'Fold_ID': 1,
        'Train_Start': fold_start,  # For single fold, train = test
        'Train_End': actual_end,
        'Test_Start': fold_start,
        'Test_End': actual_end,
        'Phase': "test"
    }
    
    return fold_df, metadata


def add_fold_metadata_columns(df: pd.DataFrame, metadata: Dict[str, Any]) -> pd.DataFrame:
    """
    Add fold metadata columns to every row of the DataFrame.
    
    Columns added:
        - Fold_ID
        - Phase (already added by select_sample_fold, but ensures it exists)
        - Train_Start
        - Train_End
        - Test_Start
        - Test_End
    
    Args:
        df: DataFrame from select_sample_fold
        metadata: Metadata dict from select_sample_fold
        
    Returns:
        DataFrame with fold metadata columns added
    """
    df = df.copy()
    
    df['Fold_ID'] = metadata['Fold_ID']
    df['Train_Start'] = metadata['Train_Start']
    df['Train_End'] = metadata['Train_End']
    df['Test_Start'] = metadata['Test_Start']
    df['Test_End'] = metadata['Test_End']
    
    # Ensure Phase column exists
    if 'Phase' not in df.columns:
        df['Phase'] = metadata['Phase']
    
    return df
