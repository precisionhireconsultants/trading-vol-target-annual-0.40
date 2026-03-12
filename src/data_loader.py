"""Data loader for QQQ CSV files.

Phase 26: Enhanced validation with human-readable error messages
and corporate action (split) detection.
"""
import pandas as pd
import numpy as np
from pathlib import Path
from typing import List, Optional, Set

# Required columns in the CSV
REQUIRED_COLUMNS = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume']

# Numeric columns to convert to float64
NUMERIC_COLUMNS = ['Open', 'High', 'Low', 'Close', 'Volume']

# Corporate action detection thresholds
SPLIT_RATIO_THRESHOLD = 0.30  # >30% overnight move triggers warning/error
KNOWN_SPLIT_DATES: Set[str] = set()  # Add known split dates here, e.g. {"2022-08-25"}


class DataValidationError(Exception):
    """
    Structured exception for data validation failures.
    
    Provides human-readable error messages with context:
    - file_path: Path to the problematic file
    - column_name: Column that failed validation (if applicable)
    - row_index: Row that failed validation (if applicable)
    - expected: What was expected
    - found: What was actually found
    - hint: Suggestion for fixing the issue
    """
    
    def __init__(
        self,
        message: str,
        file_path: Optional[str] = None,
        column_name: Optional[str] = None,
        row_index: Optional[int] = None,
        expected: Optional[str] = None,
        found: Optional[str] = None,
        hint: Optional[str] = None
    ):
        self.message = message
        self.file_path = file_path
        self.column_name = column_name
        self.row_index = row_index
        self.expected = expected
        self.found = found
        self.hint = hint
        
        # Build detailed error message
        parts = [message]
        if file_path:
            parts.append(f"File: {file_path}")
        if column_name:
            parts.append(f"Column: {column_name}")
        if row_index is not None:
            parts.append(f"Row: {row_index}")
        if expected:
            parts.append(f"Expected: {expected}")
        if found:
            parts.append(f"Found: {found}")
        if hint:
            parts.append(f"Hint: {hint}")
        
        self.detailed_message = "\n  ".join(parts)
        super().__init__(self.detailed_message)


def load_qqq_csv(path: str | Path, price_scale: float = 1.0) -> pd.DataFrame:
    """
    Load QQQ data from a CSV file.
    
    Args:
        path: Path to the CSV file
        price_scale: If not 1.0, OHLC columns are divided by this value to convert
            to dollars per share (e.g. 100_000 for SQQQ index-scaled data).
            Volume is never scaled.
        
    Returns:
        DataFrame with columns: Date, Open, High, Low, Close, Volume
        Sorted by Date ascending
        
    Raises:
        DataValidationError: If required columns are missing or data is invalid
        FileNotFoundError: If file doesn't exist
    """
    path = Path(path)
    
    if not path.exists():
        raise FileNotFoundError(f"CSV file not found: {path}")
    
    # Read CSV
    try:
        df = pd.read_csv(path)
    except Exception as e:
        raise DataValidationError(
            message=f"Failed to read CSV file: {e}",
            file_path=str(path),
            hint="Ensure the file is a valid CSV format"
        )
    
    # Validate required columns with helpful error message
    missing_cols = set(REQUIRED_COLUMNS) - set(df.columns)
    if missing_cols:
        found_cols = list(df.columns)
        raise DataValidationError(
            message=f"Missing required columns: {sorted(missing_cols)}",
            file_path=str(path),
            expected=str(REQUIRED_COLUMNS),
            found=str(found_cols),
            hint="Please check CSV format. Required columns are: Date, Open, High, Low, Close, Volume"
        )
    
    # Parse Date column with error handling
    try:
        df['Date'] = pd.to_datetime(df['Date'])
    except Exception as e:
        raise DataValidationError(
            message=f"Failed to parse Date column: {e}",
            file_path=str(path),
            column_name="Date",
            hint="Date values should be in a standard format like YYYY-MM-DD"
        )
    
    # Validate numeric columns
    for col in NUMERIC_COLUMNS:
        if col in df.columns:
            non_numeric = df[col].apply(lambda x: not isinstance(x, (int, float)) and not pd.isna(x) and not _is_numeric_string(x))
            if non_numeric.any():
                bad_row = non_numeric.idxmax()
                bad_value = df.loc[bad_row, col]
                raise DataValidationError(
                    message=f"Non-numeric value in column '{col}'",
                    file_path=str(path),
                    column_name=col,
                    row_index=int(bad_row),
                    expected="numeric value (int or float)",
                    found=f"'{bad_value}' (type: {type(bad_value).__name__})",
                    hint=f"Ensure all values in '{col}' are numbers"
                )
    
    # Sort by Date ascending
    df = df.sort_values('Date', ascending=True).reset_index(drop=True)
    
    # Apply price scale if requested (e.g. SQQQ index-scaled data -> dollars per share)
    if price_scale != 1.0:
        for col in ['Open', 'High', 'Low', 'Close']:
            if col in df.columns:
                df[col] = df[col] / price_scale
    
    return df


def _is_numeric_string(value) -> bool:
    """Check if a value is a string that can be converted to a number."""
    if not isinstance(value, str):
        return False
    try:
        float(value.replace(',', ''))
        return True
    except ValueError:
        return False


def normalize_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize data types and handle missing values.
    
    - Converts OHLCV columns to float64
    - Drops rows with missing Date or Close
    
    Args:
        df: DataFrame from load_qqq_csv
        
    Returns:
        Normalized DataFrame with proper types and no critical missing values
    """
    df = df.copy()
    
    # Convert numeric columns to float64
    for col in NUMERIC_COLUMNS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').astype(np.float64)
    
    # Count rows before dropping
    rows_before = len(df)
    
    # Drop rows with missing Date or Close (critical columns)
    df = df.dropna(subset=['Date', 'Close'])
    
    rows_dropped = rows_before - len(df)
    if rows_dropped > 0:
        print(f"Dropped {rows_dropped} rows with missing Date or Close")
    
    # Reset index after dropping
    df = df.reset_index(drop=True)
    
    return df


def validate_data_integrity(df: pd.DataFrame) -> pd.DataFrame:
    """
    Validate data meets contract requirements.
    
    Checks:
        1. Ensure Date is unique (dedupe keeping last row for duplicates)
        2. Assert dates are ascending after sort
        3. Assert OHLC > 0 (drop rows with non-positive values)
    
    Args:
        df: DataFrame with Date, Open, High, Low, Close columns
        
    Returns:
        Validated DataFrame with invalid rows removed
        
    Raises:
        AssertionError: If dates are not ascending after sort
    """
    df = df.copy()
    
    # 1. Ensure Date is unique (dedupe keeping last)
    if df['Date'].duplicated().any():
        dup_count = df['Date'].duplicated().sum()
        print(f"Warning: Deduped {dup_count} duplicate dates (kept last)")
        df = df.drop_duplicates(subset=['Date'], keep='last')
        df = df.reset_index(drop=True)
    
    # 2. Assert ascending dates after sort
    assert df['Date'].is_monotonic_increasing, "Dates must be ascending after sort"
    
    # 3. Assert OHLC > 0 (drop invalid rows)
    price_cols = ['Open', 'High', 'Low', 'Close']
    invalid_mask = (df[price_cols] <= 0).any(axis=1)
    if invalid_mask.any():
        invalid_count = invalid_mask.sum()
        print(f"Warning: Dropped {invalid_count} rows with non-positive OHLC")
        df = df[~invalid_mask].reset_index(drop=True)
    
    return df


def check_corporate_actions(
    df: pd.DataFrame,
    file_path: Optional[str] = None,
    mode: str = "backtest",
    known_split_dates: Optional[Set[str]] = None,
    split_threshold: float = SPLIT_RATIO_THRESHOLD
) -> List[dict]:
    """
    Check for potential corporate actions (splits) in price data.
    
    Pattern A (locked for v1): Uses unadjusted prices for both signals and execution.
    This function detects large overnight price changes that may indicate splits.
    
    Args:
        df: DataFrame with Date, Open, Close columns (sorted by Date ascending)
        file_path: Path to file for error messages
        mode: "backtest" (warn) or "live" (error)
        known_split_dates: Set of date strings (YYYY-MM-DD) to ignore
        split_threshold: Ratio threshold for flagging (default 0.30 = 30%)
        
    Returns:
        List of detected potential splits as dicts with date, ratio, and action
        
    Raises:
        DataValidationError: In live mode, if unknown split detected
    """
    if known_split_dates is None:
        known_split_dates = KNOWN_SPLIT_DATES
    
    detected = []
    
    if len(df) < 2:
        return detected
    
    # Calculate overnight returns: Open(t) / Close(t-1) - 1
    df = df.copy()
    df['prev_close'] = df['Close'].shift(1)
    df['overnight_ratio'] = abs(df['Open'] / df['prev_close'] - 1)
    
    # Find days with large overnight moves
    suspicious = df[df['overnight_ratio'] > split_threshold].copy()
    
    for idx, row in suspicious.iterrows():
        date_str = row['Date'].strftime('%Y-%m-%d')
        ratio = row['overnight_ratio']
        open_price = row['Open']
        prev_close = row['prev_close']
        
        # Check if this is a known split date
        is_known = date_str in known_split_dates
        
        action = "skipped (known split)" if is_known else "flagged"
        
        detected.append({
            'date': date_str,
            'ratio': ratio,
            'open': open_price,
            'prev_close': prev_close,
            'is_known': is_known,
            'action': action
        })
        
        if not is_known:
            msg = (
                f"Large overnight price change detected: {ratio:.1%} on {date_str} "
                f"(Open={open_price:.2f}, PrevClose={prev_close:.2f})"
            )
            
            if mode == "live":
                raise DataValidationError(
                    message=msg,
                    file_path=file_path,
                    hint=f"If this is a known split, add '{date_str}' to KNOWN_SPLIT_DATES"
                )
            else:
                print(f"Warning: {msg}")
    
    return detected


def validate_ohlc_sanity(
    df: pd.DataFrame,
    file_path: Optional[str] = None
) -> bool:
    """
    Validate OHLC bar sanity (High >= max(Open, Close), Low <= min(Open, Close), etc.).
    
    Args:
        df: DataFrame with Open, High, Low, Close columns
        file_path: Path for error messages
        
    Returns:
        True if all bars are valid
        
    Raises:
        DataValidationError: If any bar fails sanity checks
    """
    # High must be >= max(Open, Close)
    invalid_high = df['High'] < df[['Open', 'Close']].max(axis=1)
    if invalid_high.any():
        bad_row = invalid_high.idxmax()
        raise DataValidationError(
            message="Invalid OHLC bar: High < max(Open, Close)",
            file_path=file_path,
            row_index=int(bad_row),
            found=f"High={df.loc[bad_row, 'High']}, Open={df.loc[bad_row, 'Open']}, Close={df.loc[bad_row, 'Close']}",
            hint="High price must be >= both Open and Close"
        )
    
    # Low must be <= min(Open, Close)
    invalid_low = df['Low'] > df[['Open', 'Close']].min(axis=1)
    if invalid_low.any():
        bad_row = invalid_low.idxmax()
        raise DataValidationError(
            message="Invalid OHLC bar: Low > min(Open, Close)",
            file_path=file_path,
            row_index=int(bad_row),
            found=f"Low={df.loc[bad_row, 'Low']}, Open={df.loc[bad_row, 'Open']}, Close={df.loc[bad_row, 'Close']}",
            hint="Low price must be <= both Open and Close"
        )
    
    # High must be >= Low
    invalid_hl = df['High'] < df['Low']
    if invalid_hl.any():
        bad_row = invalid_hl.idxmax()
        raise DataValidationError(
            message="Invalid OHLC bar: High < Low",
            file_path=file_path,
            row_index=int(bad_row),
            found=f"High={df.loc[bad_row, 'High']}, Low={df.loc[bad_row, 'Low']}",
            hint="High price must be >= Low price"
        )
    
    return True
