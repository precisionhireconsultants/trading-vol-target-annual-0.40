"""Tests for export module."""
import pytest
import pandas as pd
import numpy as np
from datetime import datetime

from export import (
    build_final_schema, validate_schema, FINAL_COLUMNS, get_expected_columns,
    export_to_csv, verify_export, DEFAULT_OUTPUT_PATH
)


def create_test_dataframe():
    """Create a test DataFrame with all required columns."""
    return pd.DataFrame({
        'Fold_ID': [1],
        'Phase': ['test'],
        'Train_Start': [datetime(2020, 1, 1)],
        'Train_End': [datetime(2020, 12, 31)],
        'Test_Start': [datetime(2020, 1, 1)],
        'Test_End': [datetime(2020, 12, 31)],
        'Date': [datetime(2020, 6, 1)],
        'Open': [100.0],
        'High': [105.0],
        'Low': [99.0],
        'Close': [103.0],
        'Volume': [1000000],
        'QQQ_ann_vol': [0.15],
        'MA50': [100.0],
        'MA250': [98.0],
        'Base_Regime': ['bull'],
        'Confirmed_Regime': ['bull'],
        'Final_Trading_Regime': ['bull'],
        'Target_Weight': [1.0],
        'Exec_Target_Weight': [1.0],
        'Portfolio_Value_Open': [10000.0],
        'Actual_Weight': [0.95],
        'Exec_Symbol': ['QQQ'],
        'Exec_Open': [100.0],
        'Exec_Close': [103.0],
        'Target_Shares': [100],
        'Trade_Flag': [1],
        'Trade_Made_Type': ['BUY'],
        'Trade_Count': [1],
        'Net_Shares_Change': [100],
        'Total_Notional_Abs': [10000.0],
        'Fill_Price_VWAP': [100.0],
        'Rebalance_Reason_Code': ['REGIME_SWITCH'],
        'Total_Stocks_Owned': [100],
        'Cash': [0.0],
        'Remaining_Portfolio_Amount': [10300.0]
    })


class TestBuildFinalSchema:
    """Tests for build_final_schema function."""
    
    def test_renames_ohlcv_columns(self):
        """Test that OHLCV columns are renamed with QQQ_ prefix."""
        df = create_test_dataframe()
        result = build_final_schema(df)
        
        assert 'QQQ_Open' in result.columns
        assert 'QQQ_High' in result.columns
        assert 'QQQ_Low' in result.columns
        assert 'QQQ_Close' in result.columns
        assert 'QQQ_Volume' in result.columns
    
    def test_adds_adj_close(self):
        """Test that QQQ_Adj Close is added."""
        df = create_test_dataframe()
        result = build_final_schema(df)
        
        assert 'QQQ_Adj Close' in result.columns
        assert result['QQQ_Adj Close'].iloc[0] == result['QQQ_Close'].iloc[0]
    
    def test_exact_column_count(self):
        """Test that result has exactly 37 columns."""
        df = create_test_dataframe()
        result = build_final_schema(df)
        
        assert len(result.columns) == 37
    
    def test_column_order_matches(self):
        """Test that column order matches FINAL_COLUMNS exactly."""
        df = create_test_dataframe()
        result = build_final_schema(df)
        
        assert list(result.columns) == FINAL_COLUMNS


class TestValidateSchema:
    """Tests for validate_schema function."""
    
    def test_valid_schema(self):
        """Test that valid schema returns True."""
        df = create_test_dataframe()
        result = build_final_schema(df)
        
        assert validate_schema(result) is True
    
    def test_invalid_schema_wrong_columns(self):
        """Test that wrong columns return False."""
        df = pd.DataFrame({'wrong': [1], 'columns': [2]})
        
        assert validate_schema(df) is False
    
    def test_invalid_schema_wrong_order(self):
        """Test that wrong order returns False."""
        df = create_test_dataframe()
        result = build_final_schema(df)
        # Swap first two columns
        cols = list(result.columns)
        cols[0], cols[1] = cols[1], cols[0]
        result = result[cols]
        
        assert validate_schema(result) is False


class TestGetExpectedColumns:
    """Tests for get_expected_columns function."""
    
    def test_returns_list(self):
        """Test that function returns a list."""
        result = get_expected_columns()
        assert isinstance(result, list)
    
    def test_returns_37_columns(self):
        """Test that function returns 37 columns."""
        result = get_expected_columns()
        assert len(result) == 37
    
    def test_first_column_is_fold_id(self):
        """Test first column is Fold_ID."""
        result = get_expected_columns()
        assert result[0] == 'Fold_ID'
    
    def test_last_column_is_remaining_portfolio(self):
        """Test last column is Remaining_Portfolio_Amount."""
        result = get_expected_columns()
        assert result[-1] == 'Remaining_Portfolio_Amount'


class TestExportToCsv:
    """Tests for export_to_csv function."""
    
    def test_creates_file(self, tmp_path):
        """Test that CSV file is created."""
        df = create_test_dataframe()
        final_df = build_final_schema(df)
        
        output_path = tmp_path / "test_output.csv"
        result = export_to_csv(final_df, str(output_path))
        
        assert result.exists()
    
    def test_creates_directories(self, tmp_path):
        """Test that parent directories are created."""
        df = create_test_dataframe()
        final_df = build_final_schema(df)
        
        output_path = tmp_path / "nested" / "dirs" / "output.csv"
        result = export_to_csv(final_df, str(output_path))
        
        assert result.exists()
    
    def test_file_has_correct_headers(self, tmp_path):
        """Test that exported CSV has correct headers."""
        df = create_test_dataframe()
        final_df = build_final_schema(df)
        
        output_path = tmp_path / "test_output.csv"
        export_to_csv(final_df, str(output_path))
        
        # Read back and check
        loaded_df = pd.read_csv(output_path)
        assert list(loaded_df.columns) == FINAL_COLUMNS
    
    def test_file_has_data(self, tmp_path):
        """Test that exported CSV has data rows."""
        df = create_test_dataframe()
        final_df = build_final_schema(df)
        
        output_path = tmp_path / "test_output.csv"
        export_to_csv(final_df, str(output_path))
        
        loaded_df = pd.read_csv(output_path)
        assert len(loaded_df) == 1


class TestVerifyExport:
    """Tests for verify_export function."""
    
    def test_valid_export(self, tmp_path):
        """Test verification of valid export."""
        df = create_test_dataframe()
        final_df = build_final_schema(df)
        
        output_path = tmp_path / "test_output.csv"
        export_to_csv(final_df, str(output_path))
        
        assert verify_export(str(output_path)) is True
    
    def test_missing_file(self, tmp_path):
        """Test verification of missing file."""
        missing_path = tmp_path / "does_not_exist.csv"
        
        assert verify_export(str(missing_path)) is False
    
    def test_wrong_headers(self, tmp_path):
        """Test verification of file with wrong headers."""
        wrong_df = pd.DataFrame({'wrong': [1], 'headers': [2]})
        output_path = tmp_path / "wrong_headers.csv"
        wrong_df.to_csv(output_path, index=False)
        
        assert verify_export(str(output_path)) is False
