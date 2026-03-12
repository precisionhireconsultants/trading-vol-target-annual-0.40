"""Tests for data loader module."""
import pytest
import pandas as pd
import numpy as np
from pathlib import Path

from data_loader import (
    load_qqq_csv, normalize_data, validate_data_integrity,
    REQUIRED_COLUMNS, NUMERIC_COLUMNS,
    DataValidationError, check_corporate_actions, validate_ohlc_sanity
)


# Path to test fixtures
FIXTURES_DIR = Path(__file__).parent / "fixtures"
SAMPLE_QQQ_PATH = FIXTURES_DIR / "sample_qqq.csv"


class TestLoadQqqCsv:
    """Tests for load_qqq_csv function."""
    
    def test_load_returns_dataframe(self):
        """Test that load_qqq_csv returns a DataFrame."""
        df = load_qqq_csv(SAMPLE_QQQ_PATH)
        assert isinstance(df, pd.DataFrame)
    
    def test_load_has_required_columns(self):
        """Test that loaded DataFrame has all required columns."""
        df = load_qqq_csv(SAMPLE_QQQ_PATH)
        for col in REQUIRED_COLUMNS:
            assert col in df.columns, f"Missing column: {col}"
    
    def test_load_correct_row_count(self):
        """Test that fixture has expected number of rows."""
        df = load_qqq_csv(SAMPLE_QQQ_PATH)
        assert len(df) == 15, f"Expected 15 rows, got {len(df)}"
    
    def test_date_parsed_as_datetime(self):
        """Test that Date column is parsed as datetime."""
        df = load_qqq_csv(SAMPLE_QQQ_PATH)
        assert pd.api.types.is_datetime64_any_dtype(df['Date'])
    
    def test_sorted_by_date_ascending(self):
        """Test that data is sorted by Date ascending."""
        df = load_qqq_csv(SAMPLE_QQQ_PATH)
        dates = df['Date'].tolist()
        assert dates == sorted(dates), "Data not sorted by Date ascending"
    
    def test_file_not_found_raises(self):
        """Test that missing file raises FileNotFoundError."""
        with pytest.raises(FileNotFoundError):
            load_qqq_csv("nonexistent.csv")
    
    def test_missing_columns_raises(self, tmp_path):
        """Test that missing required columns raises DataValidationError."""
        # Create CSV with missing columns
        bad_csv = tmp_path / "bad.csv"
        bad_csv.write_text("Date,Open,High\n2024-01-01,100,101\n")
        
        with pytest.raises(DataValidationError, match="Missing required columns"):
            load_qqq_csv(bad_csv)


class TestNormalizeData:
    """Tests for normalize_data function."""
    
    def test_numeric_columns_are_float64(self):
        """Test that OHLCV columns are converted to float64."""
        df = load_qqq_csv(SAMPLE_QQQ_PATH)
        df = normalize_data(df)
        
        for col in NUMERIC_COLUMNS:
            assert df[col].dtype == np.float64, f"{col} should be float64"
    
    def test_drops_rows_with_missing_close(self):
        """Test that rows with missing Close are dropped."""
        df = pd.DataFrame({
            'Date': pd.to_datetime(['2024-01-01', '2024-01-02', '2024-01-03']),
            'Open': [100.0, 101.0, 102.0],
            'High': [105.0, 106.0, 107.0],
            'Low': [99.0, 100.0, 101.0],
            'Close': [103.0, np.nan, 105.0],  # Middle row has missing Close
            'Volume': [1000, 1100, 1200]
        })
        
        result = normalize_data(df)
        assert len(result) == 2, "Should drop row with missing Close"
        assert result['Close'].isna().sum() == 0, "No NaN in Close"
    
    def test_drops_rows_with_missing_date(self):
        """Test that rows with missing Date are dropped."""
        df = pd.DataFrame({
            'Date': pd.to_datetime(['2024-01-01', pd.NaT, '2024-01-03']),
            'Open': [100.0, 101.0, 102.0],
            'High': [105.0, 106.0, 107.0],
            'Low': [99.0, 100.0, 101.0],
            'Close': [103.0, 104.0, 105.0],
            'Volume': [1000, 1100, 1200]
        })
        
        result = normalize_data(df)
        assert len(result) == 2, "Should drop row with missing Date"
    
    def test_handles_string_numbers(self):
        """Test that string numbers are converted properly."""
        df = pd.DataFrame({
            'Date': pd.to_datetime(['2024-01-01', '2024-01-02']),
            'Open': ['100.5', '101.5'],  # Strings
            'High': ['105.5', '106.5'],
            'Low': ['99.5', '100.5'],
            'Close': ['103.5', '104.5'],
            'Volume': ['1000', '1100']
        })
        
        result = normalize_data(df)
        assert result['Open'].dtype == np.float64
        assert result['Close'].iloc[0] == 103.5
    
    def test_preserves_valid_data(self):
        """Test that valid data is preserved unchanged."""
        df = load_qqq_csv(SAMPLE_QQQ_PATH)
        original_len = len(df)
        
        result = normalize_data(df)
        assert len(result) == original_len, "Should not drop valid rows"
    
    def test_resets_index_after_drop(self):
        """Test that index is reset after dropping rows."""
        df = pd.DataFrame({
            'Date': pd.to_datetime(['2024-01-01', '2024-01-02', '2024-01-03']),
            'Open': [100.0, 101.0, 102.0],
            'High': [105.0, 106.0, 107.0],
            'Low': [99.0, 100.0, 101.0],
            'Close': [103.0, np.nan, 105.0],
            'Volume': [1000, 1100, 1200]
        })
        
        result = normalize_data(df)
        assert list(result.index) == [0, 1], "Index should be reset"


# =============================================================================
# Phase 26: Human-Readable Error Messages
# =============================================================================

class TestDataValidationError:
    """Tests for DataValidationError class."""
    
    def test_error_message_is_readable(self):
        """Test that error message is human-readable."""
        err = DataValidationError(
            message="Missing required columns: ['Close', 'Open']",
            file_path="/path/to/data.csv",
            expected="['Date', 'Open', 'High', 'Low', 'Close', 'Volume']",
            found="['Date', 'High', 'Low', 'Volume']",
            hint="Please check CSV format"
        )
        
        msg = str(err)
        assert "Missing required columns" in msg
        assert "/path/to/data.csv" in msg
        assert "Hint:" in msg
    
    def test_missing_column_error_message_is_readable(self, tmp_path):
        """Test that missing column error message is readable and helpful."""
        bad_csv = tmp_path / "bad.csv"
        bad_csv.write_text("Date,High,Low\n2024-01-01,101,99\n")
        
        with pytest.raises(DataValidationError) as exc_info:
            load_qqq_csv(bad_csv)
        
        err = exc_info.value
        # Check that error message contains helpful info
        assert "Missing required columns" in err.message
        assert str(bad_csv) in err.file_path
        assert err.hint is not None
        assert "Date" in str(err.found)  # Shows what was found
    
    def test_wrong_type_error_message_is_readable(self, tmp_path):
        """Test that wrong type error message is readable."""
        bad_csv = tmp_path / "bad_types.csv"
        bad_csv.write_text("Date,Open,High,Low,Close,Volume\n2024-01-01,abc,101,99,100,1000\n")
        
        with pytest.raises(DataValidationError) as exc_info:
            load_qqq_csv(bad_csv)
        
        err = exc_info.value
        assert "Non-numeric" in err.message or "parse" in err.message.lower()
        assert str(bad_csv) in err.file_path
    
    def test_error_contains_column_names(self, tmp_path):
        """Test that error messages contain specific column names."""
        bad_csv = tmp_path / "missing.csv"
        bad_csv.write_text("Date,Volume\n2024-01-01,1000\n")
        
        with pytest.raises(DataValidationError) as exc_info:
            load_qqq_csv(bad_csv)
        
        err = exc_info.value
        # Should mention the missing columns
        assert "Open" in str(err) or "Close" in str(err)
    
    def test_error_contains_helpful_hints(self, tmp_path):
        """Test that error messages contain helpful hints."""
        bad_csv = tmp_path / "missing.csv"
        bad_csv.write_text("Date,Volume\n2024-01-01,1000\n")
        
        with pytest.raises(DataValidationError) as exc_info:
            load_qqq_csv(bad_csv)
        
        err = exc_info.value
        assert err.hint is not None
        assert len(err.hint) > 0


class TestCorporateActionDetection:
    """Tests for corporate action (split) detection."""
    
    def test_detects_large_overnight_move(self):
        """Test that large overnight moves are detected."""
        df = pd.DataFrame({
            'Date': pd.to_datetime(['2024-01-01', '2024-01-02', '2024-01-03']),
            'Open': [100.0, 101.0, 50.0],  # 50% drop overnight on day 3
            'High': [105.0, 106.0, 55.0],
            'Low': [99.0, 100.0, 49.0],
            'Close': [103.0, 104.0, 52.0],
            'Volume': [1000, 1100, 1200]
        })
        
        detected = check_corporate_actions(df, mode="backtest")
        assert len(detected) == 1
        assert detected[0]['date'] == '2024-01-03'
        assert detected[0]['ratio'] > 0.30
    
    def test_ignores_known_split_dates(self):
        """Test that known split dates are skipped."""
        df = pd.DataFrame({
            'Date': pd.to_datetime(['2024-01-01', '2024-01-02', '2024-01-03']),
            'Open': [100.0, 101.0, 50.0],  # Would normally flag
            'High': [105.0, 106.0, 55.0],
            'Low': [99.0, 100.0, 49.0],
            'Close': [103.0, 104.0, 52.0],
            'Volume': [1000, 1100, 1200]
        })
        
        known_splits = {'2024-01-03'}
        detected = check_corporate_actions(df, known_split_dates=known_splits)
        
        assert len(detected) == 1
        assert detected[0]['is_known'] is True
        assert detected[0]['action'] == "skipped (known split)"
    
    def test_live_mode_raises_on_unknown_split(self):
        """Test that live mode raises error on unknown split."""
        df = pd.DataFrame({
            'Date': pd.to_datetime(['2024-01-01', '2024-01-02', '2024-01-03']),
            'Open': [100.0, 101.0, 50.0],  # 50% drop
            'High': [105.0, 106.0, 55.0],
            'Low': [99.0, 100.0, 49.0],
            'Close': [103.0, 104.0, 52.0],
            'Volume': [1000, 1100, 1200]
        })
        
        with pytest.raises(DataValidationError) as exc_info:
            check_corporate_actions(df, mode="live")
        
        assert "overnight" in str(exc_info.value).lower()
    
    def test_normal_moves_not_flagged(self):
        """Test that normal overnight moves are not flagged."""
        df = pd.DataFrame({
            'Date': pd.to_datetime(['2024-01-01', '2024-01-02', '2024-01-03']),
            'Open': [100.0, 101.0, 102.0],  # Normal moves < 30%
            'High': [105.0, 106.0, 107.0],
            'Low': [99.0, 100.0, 101.0],
            'Close': [103.0, 104.0, 105.0],
            'Volume': [1000, 1100, 1200]
        })
        
        detected = check_corporate_actions(df, mode="backtest")
        assert len(detected) == 0


class TestOhlcSanity:
    """Tests for OHLC bar sanity validation."""
    
    def test_valid_bars_pass(self):
        """Test that valid OHLC bars pass validation."""
        df = pd.DataFrame({
            'Open': [100.0, 101.0],
            'High': [105.0, 106.0],
            'Low': [99.0, 100.0],
            'Close': [103.0, 104.0]
        })
        
        assert validate_ohlc_sanity(df) is True
    
    def test_high_below_open_fails(self):
        """Test that High < Open fails validation."""
        df = pd.DataFrame({
            'Open': [100.0],
            'High': [99.0],  # Invalid: High < Open
            'Low': [98.0],
            'Close': [99.5]
        })
        
        with pytest.raises(DataValidationError) as exc_info:
            validate_ohlc_sanity(df)
        
        assert "High < max(Open, Close)" in str(exc_info.value)
    
    def test_low_above_close_fails(self):
        """Test that Low > Close fails validation."""
        df = pd.DataFrame({
            'Open': [100.0],
            'High': [105.0],
            'Low': [101.0],  # Invalid: Low > Close
            'Close': [99.0]
        })
        
        with pytest.raises(DataValidationError) as exc_info:
            validate_ohlc_sanity(df)
        
        assert "Low > min(Open, Close)" in str(exc_info.value)
    
    def test_high_below_low_fails(self):
        """Test that High < Low fails validation."""
        df = pd.DataFrame({
            'Open': [100.0],
            'High': [98.0],  # Invalid: High < Low
            'Low': [99.0],
            'Close': [99.5]
        })
        
        with pytest.raises(DataValidationError) as exc_info:
            validate_ohlc_sanity(df)
        
        # Could trigger either High < max or High < Low
        assert "High" in str(exc_info.value)
