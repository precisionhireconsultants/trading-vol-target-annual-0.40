"""Export functionality for trading results."""
import pandas as pd
import numpy as np
from pathlib import Path
from typing import List

from config import DEFAULT_CONFIG, CSV_FLOAT_FORMAT

# Exact column order for consolidated.csv
FINAL_COLUMNS = [
    'Fold_ID', 'Phase', 'Train_Start', 'Train_End', 'Test_Start', 'Test_End', 'Date',
    'QQQ_Open', 'QQQ_High', 'QQQ_Low', 'QQQ_Close', 'QQQ_Adj Close', 'QQQ_Volume',
    'QQQ_ann_vol', 'MA50', 'MA250', 'Base_Regime', 'Confirmed_Regime', 'Final_Trading_Regime',
    'Target_Weight', 'Exec_Target_Weight', 'Portfolio_Value_Open', 'Actual_Weight',
    'Exec_Symbol', 'Exec_Open', 'Exec_Close', 'Target_Shares',
    'Trade_Flag', 'Trade_Made_Type', 'Trade_Count', 'Net_Shares_Change', 'Total_Notional_Abs',
    'Fill_Price_VWAP', 'Rebalance_Reason_Code',
    'Total_Stocks_Owned', 'Cash', 'Remaining_Portfolio_Amount'
]

# Intraday-only columns (present when BACKTEST_MODE == "intraday")
INTRADAY_COLUMNS = [
    'Signal_Price', 'Exec_Price', 'can_trade'
]

# Phase 35/45: Additional columns for slippage/commission reporting
EXECUTION_COLUMNS = [
    'Slippage_Applied',
    'Commission_Applied',
    'Effective_Fill_Price',
    'Reject_Reason'
]

# Phase 45: Risk control columns
RISK_COLUMNS = [
    'halt_flag',
    'kill_reason',
    'peak_equity'
]

# Debug-only columns (Phase 27): Controlled by --debug-columns flag
DEBUG_COLUMNS = [
    'Cash_Open',
    'Shares_Open',
    'Decision_Price',
    'Fill_Price_Source',
    'Fill_Price_Effective',
    'Weight_Raw',
    'Weight_Clamped'
]

# Track whether we've logged the Adj Close proxy warning
_adj_close_proxy_logged = False

# Schema type validation for critical numeric columns
# These columns must be numeric to prevent PowerBI/analytics headaches
NUMERIC_COLUMNS_SCHEMA = {
    'QQQ_Open': np.float64,
    'QQQ_High': np.float64,
    'QQQ_Low': np.float64,
    'QQQ_Close': np.float64,
    'QQQ_Adj Close': np.float64,
    'Exec_Open': np.float64,
    'Exec_Close': np.float64,
    'QQQ_Volume': np.float64,
    'Cash': np.float64,
    'Total_Stocks_Owned': np.int64,
    'Remaining_Portfolio_Amount': np.float64,
    'Portfolio_Value_Open': np.float64,
}


def validate_column_types(df: pd.DataFrame) -> bool:
    """
    Validate critical columns have correct numeric types.
    
    Prevents PowerBI headaches caused by accidental string columns.
    
    Args:
        df: DataFrame to validate
        
    Returns:
        True if all types are valid
        
    Raises:
        TypeError: If any critical column has non-numeric type
    """
    for col, expected_type in NUMERIC_COLUMNS_SCHEMA.items():
        if col in df.columns:
            if not np.issubdtype(df[col].dtype, np.number):
                raise TypeError(
                    f"Column '{col}' has type {df[col].dtype}, expected numeric. "
                    f"This may cause issues in downstream analytics tools."
                )
    return True


def build_final_schema(df: pd.DataFrame, include_debug: bool = False) -> pd.DataFrame:
    """
    Build final DataFrame with exact column order.
    
    Renames columns to match output schema:
        - Open -> QQQ_Open
        - High -> QQQ_High
        - Low -> QQQ_Low
        - Close -> QQQ_Close, QQQ_Adj Close
        - Volume -> QQQ_Volume
    
    Args:
        df: DataFrame with all computed columns
        include_debug: If True, include debug columns (Cash_Open, Shares_Open, etc.)
        
    Returns:
        DataFrame with exact columns in exact order
    """
    df = df.copy()
    
    # Rename OHLCV columns to QQQ_ prefix
    rename_map = {
        'Open': 'QQQ_Open',
        'High': 'QQQ_High',
        'Low': 'QQQ_Low',
        'Close': 'QQQ_Close',
        'Volume': 'QQQ_Volume'
    }
    df = df.rename(columns=rename_map)
    
    # Create QQQ_Adj Close as copy of QQQ_Close (no adj close in source data)
    global _adj_close_proxy_logged
    if 'QQQ_Adj Close' not in df.columns:
        df['QQQ_Adj Close'] = df['QQQ_Close']
        if not _adj_close_proxy_logged:
            print("Info: QQQ_Adj Close proxied from QQQ_Close (source data lacks Adj Close)")
            _adj_close_proxy_logged = True
    
    # Determine which columns to include
    columns_to_include = FINAL_COLUMNS.copy()

    # Append intraday-only columns when present
    for col in INTRADAY_COLUMNS:
        if col in df.columns:
            columns_to_include.append(col)
    
    # Phase 27: Add debug columns if requested
    if include_debug:
        for debug_col in DEBUG_COLUMNS:
            if debug_col in df.columns:
                columns_to_include.append(debug_col)
    
    # Select and order columns (only those that exist)
    available_columns = [c for c in columns_to_include if c in df.columns]
    result = df[available_columns].copy()
    
    return result


def validate_schema(df: pd.DataFrame) -> bool:
    """
    Validate that DataFrame has exact expected columns in order.
    
    Args:
        df: DataFrame to validate
        
    Returns:
        True if schema matches, False otherwise
    """
    return list(df.columns) == FINAL_COLUMNS


def get_expected_columns() -> List[str]:
    """Return the expected column list."""
    return FINAL_COLUMNS.copy()


# Default output path
DEFAULT_OUTPUT_PATH = DEFAULT_CONFIG.DEFAULT_OUTPUT_PATH


def export_to_csv(df: pd.DataFrame, path: str = DEFAULT_OUTPUT_PATH) -> Path:
    """
    Export DataFrame to CSV file.
    
    Creates directories if they don't exist.
    Validates column types before export.
    
    Deterministic Contract (Phase 27):
    - Uses fixed float format (CSV_FLOAT_FORMAT = "%.10f") for determinism
    - Fixed column order from FINAL_COLUMNS
    - Consistent across platforms
    
    Args:
        df: DataFrame to export (should be from build_final_schema)
        path: Output file path (default: output/for_graphs/consolidated.csv)
        
    Returns:
        Path to the created file
        
    Raises:
        TypeError: If critical columns have non-numeric types
    """
    # Validate column types before export
    validate_column_types(df)
    
    output_path = Path(path)
    
    # Create parent directories if needed
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Export to CSV with fixed float format for determinism (Phase 27)
    df.to_csv(output_path, index=False, float_format=CSV_FLOAT_FORMAT)
    
    return output_path


def verify_export(path: str) -> bool:
    """
    Verify that exported CSV exists and has correct headers.
    
    Args:
        path: Path to CSV file
        
    Returns:
        True if file exists and has correct headers
    """
    file_path = Path(path)
    
    if not file_path.exists():
        return False
    
    # Read just the header
    df = pd.read_csv(file_path, nrows=0)
    
    return list(df.columns) == FINAL_COLUMNS
