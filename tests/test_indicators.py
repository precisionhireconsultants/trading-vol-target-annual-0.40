"""Tests for indicators module."""
import pytest
import pandas as pd
import numpy as np

from indicators import (
    add_ma250, add_ma50, add_annualized_volatility,
    MA_LONG, MA_SHORT, VOL_WINDOW, TRADING_DAYS_PER_YEAR
)


class TestAddMA250:
    """Tests for add_ma250 function."""
    
    def test_adds_ma250_column(self):
        """Test that MA250 column is added."""
        df = pd.DataFrame({
            'Date': pd.date_range('2024-01-01', periods=10),
            'Close': [100.0] * 10
        })
        result = add_ma250(df, window=5)
        assert 'MA250' in result.columns
    
    def test_nan_before_window(self):
        """Test that MA250 is NaN before enough data points."""
        df = pd.DataFrame({
            'Date': pd.date_range('2024-01-01', periods=10),
            'Close': list(range(1, 11))  # 1, 2, 3, ..., 10
        })
        result = add_ma250(df, window=5)
        
        # First 4 rows should be NaN (need 5 points for first value)
        assert result['MA250'].iloc[0:4].isna().all()
        # 5th row onwards should have values
        assert not result['MA250'].iloc[4:].isna().any()
    
    def test_correct_sma_calculation(self):
        """Test that SMA is calculated correctly."""
        df = pd.DataFrame({
            'Date': pd.date_range('2024-01-01', periods=10),
            'Close': [10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 70.0, 80.0, 90.0, 100.0]
        })
        result = add_ma250(df, window=5)
        
        # First valid MA at index 4: mean(10,20,30,40,50) = 30
        assert result['MA250'].iloc[4] == 30.0
        # At index 5: mean(20,30,40,50,60) = 40
        assert result['MA250'].iloc[5] == 40.0
        # At index 9: mean(60,70,80,90,100) = 80
        assert result['MA250'].iloc[9] == 80.0
    
    def test_uses_default_window(self):
        """Test that default window is MA_LONG (250)."""
        # Create 300 rows of data
        df = pd.DataFrame({
            'Date': pd.date_range('2024-01-01', periods=300),
            'Close': [100.0] * 300
        })
        result = add_ma250(df)  # Use default window
        
        # First 249 rows should be NaN
        assert result['MA250'].iloc[0:249].isna().all()
        # Row 250 onwards should have values
        assert not result['MA250'].iloc[249:].isna().any()
    
    def test_does_not_modify_original(self):
        """Test that original DataFrame is not modified."""
        df = pd.DataFrame({
            'Date': pd.date_range('2024-01-01', periods=10),
            'Close': [100.0] * 10
        })
        original_columns = list(df.columns)
        
        add_ma250(df, window=5)
        
        assert list(df.columns) == original_columns
        assert 'MA250' not in df.columns
    
    def test_with_varying_prices(self):
        """Test MA with realistic varying prices."""
        np.random.seed(42)
        prices = 100 + np.cumsum(np.random.randn(20))
        
        df = pd.DataFrame({
            'Date': pd.date_range('2024-01-01', periods=20),
            'Close': prices
        })
        result = add_ma250(df, window=5)
        
        # Verify MA is between min and max of window
        for i in range(4, 20):
            window_prices = prices[i-4:i+1]
            assert result['MA250'].iloc[i] >= window_prices.min()
            assert result['MA250'].iloc[i] <= window_prices.max()


class TestAddMA50:
    """Tests for add_ma50 function."""
    
    def test_adds_ma50_column(self):
        """Test that MA50 column is added."""
        df = pd.DataFrame({
            'Date': pd.date_range('2024-01-01', periods=10),
            'Close': [100.0] * 10
        })
        result = add_ma50(df, window=3)
        assert 'MA50' in result.columns
    
    def test_nan_before_window(self):
        """Test that MA50 is NaN before enough data points."""
        df = pd.DataFrame({
            'Date': pd.date_range('2024-01-01', periods=10),
            'Close': list(range(1, 11))
        })
        result = add_ma50(df, window=3)
        
        # First 2 rows should be NaN (need 3 points for first value)
        assert result['MA50'].iloc[0:2].isna().all()
        # 3rd row onwards should have values
        assert not result['MA50'].iloc[2:].isna().any()
    
    def test_correct_sma_calculation(self):
        """Test that SMA is calculated correctly."""
        df = pd.DataFrame({
            'Date': pd.date_range('2024-01-01', periods=6),
            'Close': [10.0, 20.0, 30.0, 40.0, 50.0, 60.0]
        })
        result = add_ma50(df, window=3)
        
        # First valid MA at index 2: mean(10,20,30) = 20
        assert result['MA50'].iloc[2] == 20.0
        # At index 3: mean(20,30,40) = 30
        assert result['MA50'].iloc[3] == 30.0
    
    def test_uses_default_window(self):
        """Test that default window is MA_SHORT (50)."""
        df = pd.DataFrame({
            'Date': pd.date_range('2024-01-01', periods=60),
            'Close': [100.0] * 60
        })
        result = add_ma50(df)  # Use default window
        
        # First 49 rows should be NaN
        assert result['MA50'].iloc[0:49].isna().all()
        # Row 50 onwards should have values
        assert not result['MA50'].iloc[49:].isna().any()
    
    def test_does_not_modify_original(self):
        """Test that original DataFrame is not modified."""
        df = pd.DataFrame({
            'Date': pd.date_range('2024-01-01', periods=10),
            'Close': [100.0] * 10
        })
        original_columns = list(df.columns)
        
        add_ma50(df, window=3)
        
        assert list(df.columns) == original_columns
        assert 'MA50' not in df.columns


class TestAddAnnualizedVolatility:
    """Tests for add_annualized_volatility function."""
    
    def test_adds_qqq_ann_vol_column(self):
        """Test that QQQ_ann_vol column is added."""
        df = pd.DataFrame({
            'Date': pd.date_range('2024-01-01', periods=30),
            'Close': [100.0 + i for i in range(30)]
        })
        result = add_annualized_volatility(df, window=5)
        assert 'QQQ_ann_vol' in result.columns
    
    def test_nan_before_window(self):
        """Test that QQQ_ann_vol is NaN before enough data points."""
        df = pd.DataFrame({
            'Date': pd.date_range('2024-01-01', periods=10),
            'Close': [100.0 + i for i in range(10)]
        })
        result = add_annualized_volatility(df, window=5)
        
        # First row is NaN due to pct_change, plus need window for rolling std
        # So first 5 rows should be NaN (index 0-4)
        assert result['QQQ_ann_vol'].iloc[0:5].isna().all()
        # 6th row onwards should have values
        assert not result['QQQ_ann_vol'].iloc[5:].isna().any()
    
    def test_non_negative_values(self):
        """Test that volatility values are non-negative."""
        np.random.seed(42)
        prices = 100 + np.cumsum(np.random.randn(50))
        
        df = pd.DataFrame({
            'Date': pd.date_range('2024-01-01', periods=50),
            'Close': prices
        })
        result = add_annualized_volatility(df, window=10)
        
        # All non-NaN values should be >= 0
        valid_vol = result['QQQ_ann_vol'].dropna()
        assert (valid_vol >= 0).all()
    
    def test_annualization_factor(self):
        """Test that volatility is properly annualized."""
        # Create data with known daily volatility
        np.random.seed(123)
        daily_returns = np.random.randn(100) * 0.01  # 1% daily std
        prices = 100 * np.cumprod(1 + daily_returns)
        
        df = pd.DataFrame({
            'Date': pd.date_range('2024-01-01', periods=100),
            'Close': prices
        })
        result = add_annualized_volatility(df, window=20)
        
        # Get last value
        ann_vol = result['QQQ_ann_vol'].iloc[-1]
        
        # Should be roughly 0.01 * sqrt(252) ≈ 0.159
        # Allow tolerance due to randomness
        assert 0.05 < ann_vol < 0.30
    
    def test_zero_volatility_for_flat_prices(self):
        """Test that flat prices give zero volatility."""
        df = pd.DataFrame({
            'Date': pd.date_range('2024-01-01', periods=30),
            'Close': [100.0] * 30  # Flat prices
        })
        result = add_annualized_volatility(df, window=5)
        
        # Valid values should be 0 (or very close)
        valid_vol = result['QQQ_ann_vol'].dropna()
        assert (valid_vol < 1e-10).all()
    
    def test_does_not_modify_original(self):
        """Test that original DataFrame is not modified."""
        df = pd.DataFrame({
            'Date': pd.date_range('2024-01-01', periods=30),
            'Close': [100.0 + i for i in range(30)]
        })
        original_columns = list(df.columns)
        
        add_annualized_volatility(df, window=5)
        
        assert list(df.columns) == original_columns
        assert 'QQQ_ann_vol' not in df.columns


# =============================================================================
# Phase 28: Volatility Formula Verification Tests
# =============================================================================

class TestVolatilityFormula:
    """
    Tests to verify the volatility formula: rolling_std(returns, window) * sqrt(252)
    
    Phase 28: These tests ensure the formula is correct and scales properly.
    """
    
    def test_annualized_volatility_formula_with_known_std(self):
        """
        Test annualized volatility formula with synthetic data having known daily std.
        
        Formula: QQQ_ann_vol = rolling_std(returns, window) * sqrt(252)
        
        We create returns with known std and verify the output.
        """
        # Create prices with exactly known daily returns
        # If returns = [0.01, 0.01, 0.01, ...], std = 0 (constant returns)
        # If returns = [0.01, -0.01, 0.01, -0.01, ...], std = 0.01 (alternating)
        
        # Use a simple case: constant percentage change
        window = 5
        n_periods = 20
        daily_return = 0.01  # 1% daily
        
        prices = [100.0]
        for _ in range(n_periods - 1):
            prices.append(prices[-1] * (1 + daily_return))
        
        df = pd.DataFrame({
            'Date': pd.date_range('2024-01-01', periods=n_periods),
            'Close': prices
        })
        
        result = add_annualized_volatility(df, window=window)
        
        # With constant returns, std should be ~0
        # Get last valid volatility value
        valid_vols = result['QQQ_ann_vol'].dropna()
        assert len(valid_vols) > 0
        
        # Should be very close to 0 (constant returns have 0 std)
        for vol in valid_vols:
            assert vol < 0.001, f"Expected near-zero volatility, got {vol}"
    
    def test_volatility_scales_with_sqrt_252(self):
        """
        Test that volatility properly scales with sqrt(252).
        
        Verify: ann_vol = daily_std * sqrt(252)
        """
        # Create data with known daily volatility pattern
        window = 10
        n_periods = 30
        
        # Create alternating returns (+1%, -1%) to get known std
        prices = [100.0]
        for i in range(n_periods - 1):
            factor = 1.01 if i % 2 == 0 else 0.99
            prices.append(prices[-1] * factor)
        
        df = pd.DataFrame({
            'Date': pd.date_range('2024-01-01', periods=n_periods),
            'Close': prices
        })
        
        result = add_annualized_volatility(df, window=window)
        
        # Compute expected: returns have std of ~0.01 (alternating +1%, -1%)
        returns = df['Close'].pct_change()
        daily_std_expected = returns.rolling(window=window).std()
        ann_vol_expected = daily_std_expected * np.sqrt(252)
        
        # Compare
        valid_indices = result['QQQ_ann_vol'].dropna().index
        for idx in valid_indices:
            computed = result.loc[idx, 'QQQ_ann_vol']
            expected = ann_vol_expected.loc[idx]
            assert abs(computed - expected) < 1e-10, \
                f"Volatility mismatch at index {idx}: {computed} != {expected}"
    
    def test_high_volatility_prices(self):
        """Test with high volatility prices."""
        np.random.seed(42)
        # High volatility: 5% daily moves
        daily_returns = np.random.randn(50) * 0.05
        prices = 100 * np.cumprod(1 + daily_returns)
        
        df = pd.DataFrame({
            'Date': pd.date_range('2024-01-01', periods=50),
            'Close': prices
        })
        
        result = add_annualized_volatility(df, window=10)
        
        # Get last value - should be roughly 0.05 * sqrt(252) ≈ 0.79
        last_vol = result['QQQ_ann_vol'].iloc[-1]
        
        # Should be in reasonable range for 5% daily vol
        assert 0.30 < last_vol < 1.50, f"Unexpected high vol: {last_vol}"
    
    def test_low_volatility_prices(self):
        """Test with low volatility prices."""
        np.random.seed(42)
        # Low volatility: 0.1% daily moves
        daily_returns = np.random.randn(50) * 0.001
        prices = 100 * np.cumprod(1 + daily_returns)
        
        df = pd.DataFrame({
            'Date': pd.date_range('2024-01-01', periods=50),
            'Close': prices
        })
        
        result = add_annualized_volatility(df, window=10)
        
        # Get last value - should be roughly 0.001 * sqrt(252) ≈ 0.016
        last_vol = result['QQQ_ann_vol'].iloc[-1]
        
        # Should be in reasonable range for 0.1% daily vol
        assert 0.005 < last_vol < 0.05, f"Unexpected low vol: {last_vol}"
    
    def test_trading_days_constant_is_252(self):
        """Verify that TRADING_DAYS_PER_YEAR is 252."""
        assert TRADING_DAYS_PER_YEAR == 252, \
            f"Expected 252 trading days, got {TRADING_DAYS_PER_YEAR}"
