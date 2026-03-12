"""Tests for fold selection module."""
import pytest
import pandas as pd
import numpy as np
from datetime import timedelta

from fold_selection import select_sample_fold, add_fold_metadata_columns


class TestSelectSampleFold:
    """Tests for select_sample_fold function."""
    
    def test_returns_tuple(self):
        """Test that function returns a tuple of DataFrame and dict."""
        df = pd.DataFrame({
            'Date': pd.date_range('2020-01-01', periods=300),
            'Close': [100.0] * 300,
            'MA250': [np.nan] * 249 + [100.0] * 51
        })
        result = select_sample_fold(df, years=1)
        
        assert isinstance(result, tuple)
        assert len(result) == 2
        assert isinstance(result[0], pd.DataFrame)
        assert isinstance(result[1], dict)
    
    def test_fold_starts_at_first_valid_ma250(self):
        """Test that fold starts at first non-NaN MA250."""
        df = pd.DataFrame({
            'Date': pd.date_range('2020-01-01', periods=300),
            'Close': [100.0] * 300,
            'MA250': [np.nan] * 249 + [100.0] * 51
        })
        fold_df, metadata = select_sample_fold(df, years=1)
        
        # First valid MA250 is at index 249
        expected_start = df['Date'].iloc[249]
        assert fold_df['Date'].iloc[0] == expected_start
        assert metadata['Test_Start'] == expected_start
    
    def test_fold_non_empty(self):
        """Test that fold DataFrame is not empty."""
        df = pd.DataFrame({
            'Date': pd.date_range('2020-01-01', periods=300),
            'Close': [100.0] * 300,
            'MA250': [np.nan] * 249 + [100.0] * 51
        })
        fold_df, _ = select_sample_fold(df, years=1)
        
        assert len(fold_df) > 0
    
    def test_metadata_has_required_keys(self):
        """Test that metadata has all required keys."""
        df = pd.DataFrame({
            'Date': pd.date_range('2020-01-01', periods=300),
            'Close': [100.0] * 300,
            'MA250': [np.nan] * 249 + [100.0] * 51
        })
        _, metadata = select_sample_fold(df, years=1)
        
        required_keys = ['Fold_ID', 'Train_Start', 'Train_End', 
                         'Test_Start', 'Test_End', 'Phase']
        for key in required_keys:
            assert key in metadata, f"Missing key: {key}"
    
    def test_fold_id_is_one(self):
        """Test that Fold_ID is 1 for single fold."""
        df = pd.DataFrame({
            'Date': pd.date_range('2020-01-01', periods=300),
            'Close': [100.0] * 300,
            'MA250': [np.nan] * 249 + [100.0] * 51
        })
        _, metadata = select_sample_fold(df, years=1)
        
        assert metadata['Fold_ID'] == 1
    
    def test_phase_column_all_test(self):
        """Test that Phase column is all 'test'."""
        df = pd.DataFrame({
            'Date': pd.date_range('2020-01-01', periods=300),
            'Close': [100.0] * 300,
            'MA250': [np.nan] * 249 + [100.0] * 51
        })
        fold_df, _ = select_sample_fold(df, years=1)
        
        assert 'Phase' in fold_df.columns
        assert all(fold_df['Phase'] == "test")
    
    def test_stable_output(self):
        """Test that output is deterministic (stable)."""
        df = pd.DataFrame({
            'Date': pd.date_range('2020-01-01', periods=300),
            'Close': [100.0] * 300,
            'MA250': [np.nan] * 249 + [100.0] * 51
        })
        
        result1 = select_sample_fold(df, years=1)
        result2 = select_sample_fold(df, years=1)
        
        pd.testing.assert_frame_equal(result1[0], result2[0])
        assert result1[1] == result2[1]
    
    def test_raises_if_no_valid_ma250(self):
        """Test that error is raised if no valid MA250."""
        df = pd.DataFrame({
            'Date': pd.date_range('2020-01-01', periods=10),
            'Close': [100.0] * 10,
            'MA250': [np.nan] * 10
        })
        
        with pytest.raises(ValueError, match="No valid MA250"):
            select_sample_fold(df, years=1)
    
    def test_clamps_to_available_data(self):
        """Test that end date is clamped to available data."""
        df = pd.DataFrame({
            'Date': pd.date_range('2020-01-01', periods=260),
            'Close': [100.0] * 260,
            'MA250': [np.nan] * 249 + [100.0] * 11
        })
        fold_df, metadata = select_sample_fold(df, years=10)  # Request 10 years
        
        # Should clamp to last available date
        assert metadata['Test_End'] == df['Date'].max()


class TestAddFoldMetadataColumns:
    """Tests for add_fold_metadata_columns function."""
    
    def test_adds_all_metadata_columns(self):
        """Test that all fold metadata columns are added."""
        df = pd.DataFrame({
            'Date': pd.date_range('2020-01-01', periods=300),
            'Close': [100.0] * 300,
            'MA250': [np.nan] * 249 + [100.0] * 51
        })
        fold_df, metadata = select_sample_fold(df, years=1)
        result = add_fold_metadata_columns(fold_df, metadata)
        
        expected_cols = ['Fold_ID', 'Phase', 'Train_Start', 'Train_End', 
                         'Test_Start', 'Test_End']
        for col in expected_cols:
            assert col in result.columns, f"Missing column: {col}"
    
    def test_fold_id_in_every_row(self):
        """Test that Fold_ID is in every row."""
        df = pd.DataFrame({
            'Date': pd.date_range('2020-01-01', periods=300),
            'Close': [100.0] * 300,
            'MA250': [np.nan] * 249 + [100.0] * 51
        })
        fold_df, metadata = select_sample_fold(df, years=1)
        result = add_fold_metadata_columns(fold_df, metadata)
        
        assert all(result['Fold_ID'] == 1)
    
    def test_train_test_dates_in_every_row(self):
        """Test that train/test dates are in every row."""
        df = pd.DataFrame({
            'Date': pd.date_range('2020-01-01', periods=300),
            'Close': [100.0] * 300,
            'MA250': [np.nan] * 249 + [100.0] * 51
        })
        fold_df, metadata = select_sample_fold(df, years=1)
        result = add_fold_metadata_columns(fold_df, metadata)
        
        assert all(result['Train_Start'] == metadata['Train_Start'])
        assert all(result['Train_End'] == metadata['Train_End'])
        assert all(result['Test_Start'] == metadata['Test_Start'])
        assert all(result['Test_End'] == metadata['Test_End'])
    
    def test_does_not_modify_original(self):
        """Test that original DataFrame is not modified."""
        df = pd.DataFrame({
            'Date': pd.date_range('2020-01-01', periods=300),
            'Close': [100.0] * 300,
            'MA250': [np.nan] * 249 + [100.0] * 51
        })
        fold_df, metadata = select_sample_fold(df, years=1)
        original_columns = list(fold_df.columns)
        
        add_fold_metadata_columns(fold_df, metadata)
        
        assert 'Fold_ID' not in fold_df.columns or 'Fold_ID' in original_columns
