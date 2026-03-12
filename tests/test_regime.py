"""Tests for regime module."""
import pytest
import pandas as pd
import numpy as np

from regime import (
    add_base_regime, add_confirmed_regime, add_final_trading_regime, add_target_weight
)


class TestAddBaseRegime:
    """Tests for add_base_regime function."""
    
    def test_adds_base_regime_column(self):
        """Test that Base_Regime column is added."""
        df = pd.DataFrame({
            'Date': pd.date_range('2024-01-01', periods=5),
            'Close': [100.0, 101.0, 102.0, 103.0, 104.0],
            'MA250': [99.0, 100.0, 101.0, 102.0, 103.0]
        })
        result = add_base_regime(df)
        assert 'Base_Regime' in result.columns
    
    def test_cash_when_ma250_is_nan(self):
        """Test that regime is 'cash' when MA250 is NaN."""
        df = pd.DataFrame({
            'Date': pd.date_range('2024-01-01', periods=3),
            'Close': [100.0, 101.0, 102.0],
            'MA250': [np.nan, np.nan, 100.0]
        })
        result = add_base_regime(df)
        
        assert result['Base_Regime'].iloc[0] == "cash"
        assert result['Base_Regime'].iloc[1] == "cash"
    
    def test_bull_when_close_above_ma250(self):
        """Test that regime is 'bull' when Close >= MA250."""
        df = pd.DataFrame({
            'Date': pd.date_range('2024-01-01', periods=3),
            'Close': [110.0, 100.0, 105.0],
            'MA250': [100.0, 100.0, 100.0]
        })
        result = add_base_regime(df)
        
        # 110 >= 100 → bull
        assert result['Base_Regime'].iloc[0] == "bull"
        # 100 >= 100 → bull (equal is bull)
        assert result['Base_Regime'].iloc[1] == "bull"
        # 105 >= 100 → bull
        assert result['Base_Regime'].iloc[2] == "bull"
    
    def test_bear_when_close_below_ma250(self):
        """Test that regime is 'bear' when Close < MA250."""
        df = pd.DataFrame({
            'Date': pd.date_range('2024-01-01', periods=3),
            'Close': [90.0, 95.0, 99.9],
            'MA250': [100.0, 100.0, 100.0]
        })
        result = add_base_regime(df)
        
        assert result['Base_Regime'].iloc[0] == "bear"
        assert result['Base_Regime'].iloc[1] == "bear"
        assert result['Base_Regime'].iloc[2] == "bear"
    
    def test_mixed_regimes(self):
        """Test correct regime assignment with mixed conditions."""
        df = pd.DataFrame({
            'Date': pd.date_range('2024-01-01', periods=5),
            'Close': [100.0, 90.0, 110.0, 95.0, 105.0],
            'MA250': [np.nan, 100.0, 100.0, 100.0, 100.0]
        })
        result = add_base_regime(df)
        
        expected = ["cash", "bear", "bull", "bear", "bull"]
        assert list(result['Base_Regime']) == expected
    
    def test_does_not_modify_original(self):
        """Test that original DataFrame is not modified."""
        df = pd.DataFrame({
            'Date': pd.date_range('2024-01-01', periods=3),
            'Close': [100.0, 101.0, 102.0],
            'MA250': [99.0, 100.0, 101.0]
        })
        original_columns = list(df.columns)
        
        add_base_regime(df)
        
        assert list(df.columns) == original_columns
        assert 'Base_Regime' not in df.columns
    
    def test_regime_values_are_strings(self):
        """Test that regime values are strings."""
        df = pd.DataFrame({
            'Date': pd.date_range('2024-01-01', periods=3),
            'Close': [100.0, 90.0, np.nan],
            'MA250': [100.0, 100.0, np.nan]
        })
        result = add_base_regime(df)
        
        # All values should be strings
        for val in result['Base_Regime']:
            assert isinstance(val, str)
    
    def test_only_valid_regime_values(self):
        """Test that only cash, bull, bear values are used."""
        df = pd.DataFrame({
            'Date': pd.date_range('2024-01-01', periods=10),
            'Close': [100.0 + i * (-1)**i for i in range(10)],
            'MA250': [np.nan] * 3 + [100.0] * 7
        })
        result = add_base_regime(df)
        
        valid_regimes = {"cash", "bull", "bear"}
        assert set(result['Base_Regime'].unique()).issubset(valid_regimes)


class TestAddConfirmedRegime:
    """Tests for add_confirmed_regime function."""
    
    def test_adds_confirmed_regime_column(self):
        """Test that Confirmed_Regime column is added."""
        df = pd.DataFrame({
            'Date': pd.date_range('2024-01-01', periods=3),
            'Base_Regime': ["cash", "bull", "bear"]
        })
        result = add_confirmed_regime(df)
        assert 'Confirmed_Regime' in result.columns
    
    def test_equals_base_regime(self):
        """Test that Confirmed_Regime equals Base_Regime (placeholder)."""
        df = pd.DataFrame({
            'Date': pd.date_range('2024-01-01', periods=5),
            'Base_Regime': ["cash", "bull", "bear", "bull", "cash"]
        })
        result = add_confirmed_regime(df)
        
        assert list(result['Confirmed_Regime']) == list(result['Base_Regime'])
    
    def test_all_values_match(self):
        """Test that every row matches between Base and Confirmed."""
        df = pd.DataFrame({
            'Date': pd.date_range('2024-01-01', periods=10),
            'Base_Regime': ["cash"] * 3 + ["bull"] * 4 + ["bear"] * 3
        })
        result = add_confirmed_regime(df)
        
        for i in range(len(result)):
            assert result['Confirmed_Regime'].iloc[i] == result['Base_Regime'].iloc[i]
    
    def test_does_not_modify_original(self):
        """Test that original DataFrame is not modified."""
        df = pd.DataFrame({
            'Date': pd.date_range('2024-01-01', periods=3),
            'Base_Regime': ["cash", "bull", "bear"]
        })
        original_columns = list(df.columns)
        
        add_confirmed_regime(df)
        
        assert list(df.columns) == original_columns
        assert 'Confirmed_Regime' not in df.columns


class TestAddFinalTradingRegime:
    """Tests for add_final_trading_regime function."""
    
    def test_adds_final_trading_regime_column(self):
        """Test that Final_Trading_Regime column is added."""
        df = pd.DataFrame({
            'Date': pd.date_range('2024-01-01', periods=3),
            'Confirmed_Regime': ["cash", "bull", "bear"]
        })
        result = add_final_trading_regime(df)
        assert 'Final_Trading_Regime' in result.columns
    
    def test_bull_stays_bull(self):
        """Test that bull regime stays bull."""
        df = pd.DataFrame({
            'Date': pd.date_range('2024-01-01', periods=3),
            'Confirmed_Regime': ["bull", "bull", "bull"]
        })
        result = add_final_trading_regime(df)
        
        assert all(result['Final_Trading_Regime'] == "bull")
    
    def test_bear_becomes_cash(self):
        """Test that bear regime becomes cash."""
        df = pd.DataFrame({
            'Date': pd.date_range('2024-01-01', periods=3),
            'Confirmed_Regime': ["bear", "bear", "bear"]
        })
        result = add_final_trading_regime(df)
        
        assert all(result['Final_Trading_Regime'] == "cash")
    
    def test_cash_stays_cash(self):
        """Test that cash regime stays cash."""
        df = pd.DataFrame({
            'Date': pd.date_range('2024-01-01', periods=3),
            'Confirmed_Regime': ["cash", "cash", "cash"]
        })
        result = add_final_trading_regime(df)
        
        assert all(result['Final_Trading_Regime'] == "cash")
    
    def test_no_bear_in_output(self):
        """Test that Final_Trading_Regime never contains 'bear'."""
        df = pd.DataFrame({
            'Date': pd.date_range('2024-01-01', periods=10),
            'Confirmed_Regime': ["cash", "bull", "bear", "bull", "bear", 
                                  "cash", "bear", "bull", "bear", "cash"]
        })
        result = add_final_trading_regime(df)
        
        assert "bear" not in result['Final_Trading_Regime'].values
    
    def test_only_bull_or_cash(self):
        """Test that only bull or cash values are present."""
        df = pd.DataFrame({
            'Date': pd.date_range('2024-01-01', periods=10),
            'Confirmed_Regime': ["cash", "bull", "bear", "bull", "bear", 
                                  "cash", "bear", "bull", "bear", "cash"]
        })
        result = add_final_trading_regime(df)
        
        valid_regimes = {"bull", "cash"}
        assert set(result['Final_Trading_Regime'].unique()) == valid_regimes
    
    def test_does_not_modify_original(self):
        """Test that original DataFrame is not modified."""
        df = pd.DataFrame({
            'Date': pd.date_range('2024-01-01', periods=3),
            'Confirmed_Regime': ["cash", "bull", "bear"]
        })
        original_columns = list(df.columns)
        
        add_final_trading_regime(df)
        
        assert list(df.columns) == original_columns
        assert 'Final_Trading_Regime' not in df.columns


class TestAddTargetWeight:
    """Tests for add_target_weight function."""
    
    def test_adds_target_weight_column(self):
        """Test that Target_Weight column is added."""
        df = pd.DataFrame({
            'Date': pd.date_range('2024-01-01', periods=3),
            'Final_Trading_Regime': ["cash", "bull", "cash"]
        })
        result = add_target_weight(df)
        assert 'Target_Weight' in result.columns
    
    def test_bull_gives_weight_one(self):
        """Test that bull regime gives weight of 1.0."""
        df = pd.DataFrame({
            'Date': pd.date_range('2024-01-01', periods=3),
            'Final_Trading_Regime': ["bull", "bull", "bull"]
        })
        result = add_target_weight(df)
        
        assert all(result['Target_Weight'] == 1.0)
    
    def test_cash_gives_weight_zero(self):
        """Test that cash regime gives weight of 0.0."""
        df = pd.DataFrame({
            'Date': pd.date_range('2024-01-01', periods=3),
            'Final_Trading_Regime': ["cash", "cash", "cash"]
        })
        result = add_target_weight(df)
        
        assert all(result['Target_Weight'] == 0.0)
    
    def test_mixed_weights(self):
        """Test correct weight assignment with mixed regimes."""
        df = pd.DataFrame({
            'Date': pd.date_range('2024-01-01', periods=5),
            'Final_Trading_Regime': ["cash", "bull", "cash", "bull", "cash"]
        })
        result = add_target_weight(df)
        
        expected = [0.0, 1.0, 0.0, 1.0, 0.0]
        assert list(result['Target_Weight']) == expected
    
    def test_weight_is_float(self):
        """Test that Target_Weight values are floats."""
        df = pd.DataFrame({
            'Date': pd.date_range('2024-01-01', periods=3),
            'Final_Trading_Regime': ["cash", "bull", "cash"]
        })
        result = add_target_weight(df)
        
        assert result['Target_Weight'].dtype in [float, 'float64']
    
    def test_only_zero_or_one(self):
        """Test that only 0.0 or 1.0 values are present."""
        df = pd.DataFrame({
            'Date': pd.date_range('2024-01-01', periods=10),
            'Final_Trading_Regime': ["cash", "bull"] * 5
        })
        result = add_target_weight(df)
        
        valid_weights = {0.0, 1.0}
        assert set(result['Target_Weight'].unique()) == valid_weights
    
    def test_does_not_modify_original(self):
        """Test that original DataFrame is not modified."""
        df = pd.DataFrame({
            'Date': pd.date_range('2024-01-01', periods=3),
            'Final_Trading_Regime': ["cash", "bull", "cash"]
        })
        original_columns = list(df.columns)
        
        add_target_weight(df)
        
        assert list(df.columns) == original_columns
        assert 'Target_Weight' not in df.columns


class TestMA50Confirmation:
    """Tests for MA50 confirmation logic (Phase 26)."""
    
    def test_flag_off_copies_base_regime(self):
        """When flag is OFF, Confirmed_Regime = Base_Regime."""
        df = pd.DataFrame({
            'Base_Regime': ["bull", "bear", "cash"],
            'MA50': [110.0, 90.0, np.nan],
            'MA250': [100.0, 100.0, np.nan]
        })
        result = add_confirmed_regime(df, use_ma_confirmation=False)
        assert list(result['Confirmed_Regime']) == ["bull", "bear", "cash"]
    
    def test_bull_with_weak_ma50_becomes_cash(self):
        """When bull but MA50 <= MA250, force cash (weak rally)."""
        df = pd.DataFrame({
            'Base_Regime': ["bull", "bull", "bull"],
            'MA50': [95.0, 100.0, 100.0],  # <= MA250
            'MA250': [100.0, 100.0, 105.0]
        })
        result = add_confirmed_regime(df, use_ma_confirmation=True)
        assert list(result['Confirmed_Regime']) == ["cash", "cash", "cash"]
    
    def test_bull_with_strong_ma50_stays_bull(self):
        """When bull and MA50 > MA250, keep bull (confirmed trend)."""
        df = pd.DataFrame({
            'Base_Regime': ["bull", "bull"],
            'MA50': [110.0, 105.0],  # > MA250
            'MA250': [100.0, 100.0]
        })
        result = add_confirmed_regime(df, use_ma_confirmation=True)
        assert list(result['Confirmed_Regime']) == ["bull", "bull"]
    
    def test_bear_unaffected_by_confirmation(self):
        """Bear regime is not changed by MA50 confirmation."""
        df = pd.DataFrame({
            'Base_Regime': ["bear", "bear"],
            'MA50': [110.0, 90.0],
            'MA250': [100.0, 100.0]
        })
        result = add_confirmed_regime(df, use_ma_confirmation=True)
        assert list(result['Confirmed_Regime']) == ["bear", "bear"]
    
    def test_cash_unaffected_by_confirmation(self):
        """Cash regime is not changed by MA50 confirmation."""
        df = pd.DataFrame({
            'Base_Regime': ["cash", "cash"],
            'MA50': [np.nan, 110.0],
            'MA250': [np.nan, 100.0]
        })
        result = add_confirmed_regime(df, use_ma_confirmation=True)
        assert list(result['Confirmed_Regime']) == ["cash", "cash"]
    
    def test_bull_with_nan_ma50_becomes_cash(self):
        """Bull with NaN MA50 should become cash (no confirmation possible)."""
        df = pd.DataFrame({
            'Base_Regime': ["bull"],
            'MA50': [np.nan],
            'MA250': [100.0]
        })
        result = add_confirmed_regime(df, use_ma_confirmation=True)
        assert result['Confirmed_Regime'].iloc[0] == "cash"
    
    def test_target_weight_zero_when_confirmation_fails(self):
        """End-to-end: weak rally should result in Target_Weight = 0."""
        df = pd.DataFrame({
            'Close': [105.0],
            'MA50': [95.0],
            'MA250': [100.0]
        })
        df = add_base_regime(df)  # bull (Close > MA250)
        df = add_confirmed_regime(df, use_ma_confirmation=True)  # cash (MA50 <= MA250)
        df = add_final_trading_regime(df)
        df = add_target_weight(df)
        
        assert df['Base_Regime'].iloc[0] == "bull"
        assert df['Confirmed_Regime'].iloc[0] == "cash"
        assert df['Target_Weight'].iloc[0] == 0.0
    
    def test_default_flag_uses_config(self):
        """When use_ma_confirmation is None, should use config default (False)."""
        df = pd.DataFrame({
            'Base_Regime': ["bull"],
            'MA50': [90.0],  # Would fail confirmation
            'MA250': [100.0]
        })
        # Default config has USE_MA_CONFIRMATION=False, so should just copy
        result = add_confirmed_regime(df)  # No parameter = use config
        assert result['Confirmed_Regime'].iloc[0] == "bull"


class TestVolatilityTargeting:
    """Tests for volatility targeting (Phase 28)."""
    
    def test_vol_targeting_off_gives_binary_weights(self):
        """Without vol targeting, weights are 0 or 1."""
        df = pd.DataFrame({
            'Final_Trading_Regime': ["bull", "cash", "bull"],
            'QQQ_ann_vol': [0.20, 0.20, 0.30]
        })
        result = add_target_weight(df, use_vol_targeting=False)
        assert list(result['Target_Weight']) == [1.0, 0.0, 1.0]
    
    def test_vol_targeting_scales_position(self):
        """Vol targeting scales position based on volatility."""
        df = pd.DataFrame({
            'Final_Trading_Regime': ["bull", "bull"],
            'QQQ_ann_vol': [0.25, 0.50]  # 25% and 50% vol
        })
        result = add_target_weight(
            df, 
            use_vol_targeting=True, 
            vol_target=0.25,
            max_position=1.0
        )
        # At 25% vol, target=25%/25% = 1.0
        assert abs(result['Target_Weight'].iloc[0] - 1.0) < 0.01
        # At 50% vol, target=25%/50% = 0.5
        assert abs(result['Target_Weight'].iloc[1] - 0.5) < 0.01
    
    def test_vol_targeting_respects_max_position(self):
        """Vol targeting caps at max_position."""
        df = pd.DataFrame({
            'Final_Trading_Regime': ["bull"],
            'QQQ_ann_vol': [0.10]  # Very low vol
        })
        result = add_target_weight(
            df,
            use_vol_targeting=True,
            vol_target=0.25,  # Would give 2.5x
            max_position=1.0  # Capped at 1.0
        )
        assert result['Target_Weight'].iloc[0] == 1.0
    
    def test_vol_targeting_cash_regime_is_zero(self):
        """Cash regime always has 0 weight, even with vol targeting."""
        df = pd.DataFrame({
            'Final_Trading_Regime': ["cash", "cash"],
            'QQQ_ann_vol': [0.10, 0.50]
        })
        result = add_target_weight(df, use_vol_targeting=True)
        assert all(result['Target_Weight'] == 0.0)
    
    def test_vol_doubling_halves_weight(self):
        """If vol doubles, target weight roughly halves."""
        df = pd.DataFrame({
            'Final_Trading_Regime': ["bull", "bull"],
            'QQQ_ann_vol': [0.20, 0.40]
        })
        result = add_target_weight(
            df,
            use_vol_targeting=True,
            vol_target=0.20,
            max_position=1.0
        )
        weight1 = result['Target_Weight'].iloc[0]
        weight2 = result['Target_Weight'].iloc[1]
        # weight1 should be roughly 2x weight2
        assert abs(weight1 / weight2 - 2.0) < 0.01
    
    def test_nan_vol_uses_max_position(self):
        """NaN volatility uses max position as fallback."""
        df = pd.DataFrame({
            'Final_Trading_Regime': ["bull"],
            'QQQ_ann_vol': [np.nan]
        })
        result = add_target_weight(
            df,
            use_vol_targeting=True,
            max_position=0.8
        )
        assert result['Target_Weight'].iloc[0] == 0.8
