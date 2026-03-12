"""Tests for portfolio module."""
import pytest
import pandas as pd
import numpy as np

from portfolio import (
    add_exec_target_weight, init_portfolio, PortfolioState, 
    DEFAULT_INITIAL_CAPITAL, compute_target_shares, execute_trade, TradeResult,
    determine_rebalance_reason, compute_trade_fields, DailyTradeFields,
    compute_holdings, DailyHoldings, compute_eod_valuation, compute_actual_weight,
    apply_trade_throttling, is_near_zero, should_rebalance
)


class TestAddExecTargetWeight:
    """Tests for add_exec_target_weight function."""
    
    def test_adds_exec_target_weight_column(self):
        """Test that Exec_Target_Weight column is added."""
        df = pd.DataFrame({
            'Date': pd.date_range('2024-01-01', periods=5),
            'Target_Weight': [0.0, 1.0, 1.0, 0.0, 1.0]
        })
        result = add_exec_target_weight(df)
        assert 'Exec_Target_Weight' in result.columns
    
    def test_first_row_is_zero(self):
        """Test that first row Exec_Target_Weight is 0."""
        df = pd.DataFrame({
            'Date': pd.date_range('2024-01-01', periods=5),
            'Target_Weight': [1.0, 1.0, 0.0, 1.0, 0.0]
        })
        result = add_exec_target_weight(df)
        
        assert result['Exec_Target_Weight'].iloc[0] == 0.0
    
    def test_shifted_by_one(self):
        """Test that Exec_Target_Weight is shifted by one day."""
        df = pd.DataFrame({
            'Date': pd.date_range('2024-01-01', periods=5),
            'Target_Weight': [0.0, 1.0, 1.0, 0.0, 1.0]
        })
        result = add_exec_target_weight(df)
        
        # Row 1 should have yesterday's (row 0) target weight
        assert result['Exec_Target_Weight'].iloc[1] == 0.0
        # Row 2 should have row 1's target weight
        assert result['Exec_Target_Weight'].iloc[2] == 1.0
        # Row 3 should have row 2's target weight
        assert result['Exec_Target_Weight'].iloc[3] == 1.0
        # Row 4 should have row 3's target weight
        assert result['Exec_Target_Weight'].iloc[4] == 0.0
    
    def test_no_look_ahead(self):
        """Test that execution weight uses previous day's signal."""
        df = pd.DataFrame({
            'Date': pd.date_range('2024-01-01', periods=3),
            'Target_Weight': [0.0, 1.0, 0.0]
        })
        result = add_exec_target_weight(df)
        
        # Day 0: Target=0.0, but Exec should be 0.0 (no prior signal)
        # Day 1: Target=1.0, but Exec should be 0.0 (yesterday's was 0.0)
        # Day 2: Target=0.0, but Exec should be 1.0 (yesterday's was 1.0)
        expected = [0.0, 0.0, 1.0]
        assert list(result['Exec_Target_Weight']) == expected
    
    def test_does_not_modify_original(self):
        """Test that original DataFrame is not modified."""
        df = pd.DataFrame({
            'Date': pd.date_range('2024-01-01', periods=5),
            'Target_Weight': [0.0, 1.0, 1.0, 0.0, 1.0]
        })
        original_columns = list(df.columns)
        
        add_exec_target_weight(df)
        
        assert list(df.columns) == original_columns
        assert 'Exec_Target_Weight' not in df.columns


class TestPortfolioState:
    """Tests for PortfolioState dataclass."""
    
    def test_default_values(self):
        """Test default PortfolioState values."""
        state = PortfolioState()
        
        assert state.cash == DEFAULT_INITIAL_CAPITAL
        assert state.shares == 0
    
    def test_custom_values(self):
        """Test PortfolioState with custom values."""
        state = PortfolioState(cash=5000.0, shares=100)
        
        assert state.cash == 5000.0
        assert state.shares == 100
    
    def test_copy(self):
        """Test that copy creates independent state."""
        original = PortfolioState(cash=5000.0, shares=100)
        copied = original.copy()
        
        # Modify copy
        copied.cash = 3000.0
        copied.shares = 50
        
        # Original should be unchanged
        assert original.cash == 5000.0
        assert original.shares == 100


class TestInitPortfolio:
    """Tests for init_portfolio function."""
    
    def test_default_initial_capital(self):
        """Test that default initial capital is used."""
        state = init_portfolio()
        
        assert state.cash == DEFAULT_INITIAL_CAPITAL
        assert state.shares == 0
    
    def test_custom_initial_capital(self):
        """Test custom initial capital."""
        state = init_portfolio(initial_capital=50000.0)
        
        assert state.cash == 50000.0
        assert state.shares == 0
    
    def test_default_is_10000(self):
        """Test that default capital is $10,000."""
        assert DEFAULT_INITIAL_CAPITAL == 10000.0
    
    def test_returns_portfolio_state(self):
        """Test that function returns PortfolioState."""
        state = init_portfolio()
        
        assert isinstance(state, PortfolioState)


class TestComputeTargetShares:
    """Tests for compute_target_shares function."""
    
    def test_full_weight_basic(self):
        """Test 100% weight calculation."""
        # $10,000 portfolio, 100% weight, $100 stock
        # Should buy 100 shares
        result = compute_target_shares(
            portfolio_value_open=10000.0,
            exec_target_weight=1.0,
            open_price=100.0
        )
        assert result == 100
    
    def test_zero_weight(self):
        """Test 0% weight gives 0 shares."""
        result = compute_target_shares(
            portfolio_value_open=10000.0,
            exec_target_weight=0.0,
            open_price=100.0
        )
        assert result == 0
    
    def test_floors_to_integer(self):
        """Test that fractional shares are floored."""
        # $10,000 portfolio, 100% weight, $33 stock
        # 10000 / 33 = 303.03... → 303 shares
        result = compute_target_shares(
            portfolio_value_open=10000.0,
            exec_target_weight=1.0,
            open_price=33.0
        )
        assert result == 303
    
    def test_partial_weight(self):
        """Test 50% weight calculation."""
        # $10,000 portfolio, 50% weight, $100 stock
        # target = 5000 / 100 = 50 shares
        result = compute_target_shares(
            portfolio_value_open=10000.0,
            exec_target_weight=0.5,
            open_price=100.0
        )
        assert result == 50
    
    def test_never_negative(self):
        """Test that shares are never negative."""
        result = compute_target_shares(
            portfolio_value_open=10000.0,
            exec_target_weight=-0.5,  # Invalid but handled
            open_price=100.0
        )
        assert result >= 0
    
    def test_zero_price_returns_zero(self):
        """Test that zero price returns 0 shares."""
        result = compute_target_shares(
            portfolio_value_open=10000.0,
            exec_target_weight=1.0,
            open_price=0.0
        )
        assert result == 0
    
    def test_realistic_scenario(self):
        """Test with realistic QQQ scenario."""
        # $10,000 portfolio, 100% weight, $420 QQQ
        # 10000 / 420 = 23.8 → 23 shares
        result = compute_target_shares(
            portfolio_value_open=10000.0,
            exec_target_weight=1.0,
            open_price=420.0
        )
        assert result == 23


class TestExecuteTrade:
    """Tests for execute_trade function."""
    
    def test_buy_trade(self):
        """Test buying shares."""
        state = PortfolioState(cash=10000.0, shares=0)
        result = execute_trade(state, target_shares=100, open_price=50.0)
        
        assert result.shares_diff == 100
        assert result.trade_type == "BUY"
        assert result.fill_price == 50.0
        assert result.notional == 5000.0
        assert result.new_cash == 5000.0
        assert result.new_shares == 100
    
    def test_sell_trade(self):
        """Test selling shares."""
        state = PortfolioState(cash=5000.0, shares=100)
        result = execute_trade(state, target_shares=0, open_price=50.0)
        
        assert result.shares_diff == -100
        assert result.trade_type == "SELL"
        assert result.fill_price == 50.0
        assert result.notional == 5000.0
        assert result.new_cash == 10000.0
        assert result.new_shares == 0
    
    def test_no_trade(self):
        """Test when no trade is needed."""
        state = PortfolioState(cash=5000.0, shares=100)
        result = execute_trade(state, target_shares=100, open_price=50.0)
        
        assert result.shares_diff == 0
        assert result.trade_type == ""
        assert np.isnan(result.fill_price)
        assert result.notional == 0.0
        assert result.new_cash == 5000.0
        assert result.new_shares == 100
    
    def test_partial_sell(self):
        """Test selling some shares."""
        state = PortfolioState(cash=1000.0, shares=100)
        result = execute_trade(state, target_shares=50, open_price=100.0)
        
        assert result.shares_diff == -50
        assert result.trade_type == "SELL"
        assert result.notional == 5000.0
        assert result.new_cash == 6000.0
        assert result.new_shares == 50
    
    def test_additional_buy(self):
        """Test buying additional shares."""
        state = PortfolioState(cash=5000.0, shares=50)
        result = execute_trade(state, target_shares=100, open_price=100.0)
        
        assert result.shares_diff == 50
        assert result.trade_type == "BUY"
        assert result.notional == 5000.0
        assert result.new_cash == 0.0
        assert result.new_shares == 100
    
    def test_returns_trade_result(self):
        """Test that function returns TradeResult."""
        state = PortfolioState(cash=10000.0, shares=0)
        result = execute_trade(state, target_shares=10, open_price=100.0)
        
        assert isinstance(result, TradeResult)


class TestDetermineRebalanceReason:
    """Tests for determine_rebalance_reason function."""
    
    def test_regime_switch_0_to_1(self):
        """Test regime switch from cash to invested."""
        result = determine_rebalance_reason(0.0, 1.0, True)
        assert result == "REGIME_SWITCH"
    
    def test_regime_switch_1_to_0(self):
        """Test regime switch from invested to cash."""
        result = determine_rebalance_reason(1.0, 0.0, True)
        assert result == "REGIME_SWITCH"
    
    def test_no_trade(self):
        """Test no trade gives NO_TRADE."""
        result = determine_rebalance_reason(1.0, 1.0, False)
        assert result == "NO_TRADE"
    
    def test_rebalance_same_regime(self):
        """Test trade within same regime gives REBALANCE."""
        result = determine_rebalance_reason(1.0, 1.0, True)
        assert result == "REBALANCE"


class TestComputeTradeFields:
    """Tests for compute_trade_fields function."""
    
    def test_buy_trade_fields(self):
        """Test trade fields for a buy."""
        trade_result = TradeResult(
            shares_diff=100, trade_type="BUY", fill_price=50.0,
            notional=5000.0, new_cash=5000.0, new_shares=100
        )
        fields = compute_trade_fields(trade_result, 0.0, 1.0)
        
        assert fields.Trade_Flag == 1
        assert fields.Trade_Count == 1
        assert fields.Net_Shares_Change == 100
        assert fields.Trade_Made_Type == "BUY"
        assert fields.Fill_Price_VWAP == 50.0
        assert fields.Total_Notional_Abs == 5000.0
        assert fields.Rebalance_Reason_Code == "REGIME_SWITCH"
    
    def test_no_trade_fields(self):
        """Test trade fields when no trade."""
        trade_result = TradeResult(
            shares_diff=0, trade_type="", fill_price=np.nan,
            notional=0.0, new_cash=5000.0, new_shares=100
        )
        fields = compute_trade_fields(trade_result, 1.0, 1.0)
        
        assert fields.Trade_Flag == 0
        assert fields.Trade_Count == 0
        assert fields.Net_Shares_Change == 0
        assert fields.Trade_Made_Type == ""
        assert fields.Total_Notional_Abs == 0.0
        assert fields.Rebalance_Reason_Code == "NO_TRADE"
    
    def test_returns_daily_trade_fields(self):
        """Test that function returns DailyTradeFields."""
        trade_result = TradeResult(
            shares_diff=10, trade_type="BUY", fill_price=100.0,
            notional=1000.0, new_cash=9000.0, new_shares=10
        )
        fields = compute_trade_fields(trade_result, 0.0, 1.0)
        
        assert isinstance(fields, DailyTradeFields)


class TestComputeHoldings:
    """Tests for compute_holdings function."""
    
    def test_after_buy(self):
        """Test holdings after a buy trade."""
        trade_result = TradeResult(
            shares_diff=100, trade_type="BUY", fill_price=50.0,
            notional=5000.0, new_cash=5000.0, new_shares=100
        )
        holdings = compute_holdings(trade_result)
        
        assert holdings.Total_Stocks_Owned == 100
        assert holdings.Cash == 5000.0
    
    def test_after_sell(self):
        """Test holdings after a sell trade."""
        trade_result = TradeResult(
            shares_diff=-100, trade_type="SELL", fill_price=50.0,
            notional=5000.0, new_cash=10000.0, new_shares=0
        )
        holdings = compute_holdings(trade_result)
        
        assert holdings.Total_Stocks_Owned == 0
        assert holdings.Cash == 10000.0
    
    def test_no_trade(self):
        """Test holdings when no trade."""
        trade_result = TradeResult(
            shares_diff=0, trade_type="", fill_price=np.nan,
            notional=0.0, new_cash=5000.0, new_shares=100
        )
        holdings = compute_holdings(trade_result)
        
        assert holdings.Total_Stocks_Owned == 100
        assert holdings.Cash == 5000.0
    
    def test_returns_daily_holdings(self):
        """Test that function returns DailyHoldings."""
        trade_result = TradeResult(
            shares_diff=10, trade_type="BUY", fill_price=100.0,
            notional=1000.0, new_cash=9000.0, new_shares=10
        )
        holdings = compute_holdings(trade_result)
        
        assert isinstance(holdings, DailyHoldings)


class TestComputeEodValuation:
    """Tests for compute_eod_valuation function."""
    
    def test_all_cash(self):
        """Test valuation when fully in cash."""
        result = compute_eod_valuation(shares=0, cash=10000.0, close_price=100.0)
        assert result == 10000.0
    
    def test_all_invested(self):
        """Test valuation when fully invested."""
        result = compute_eod_valuation(shares=100, cash=0.0, close_price=100.0)
        assert result == 10000.0
    
    def test_mixed_portfolio(self):
        """Test valuation with mixed portfolio."""
        result = compute_eod_valuation(shares=50, cash=5000.0, close_price=100.0)
        assert result == 10000.0
    
    def test_price_change_affects_value(self):
        """Test that price change affects valuation."""
        # Bought 100 shares at $100, price now $110
        result = compute_eod_valuation(shares=100, cash=0.0, close_price=110.0)
        assert result == 11000.0
    
    def test_accounting_identity(self):
        """Test that cash + stock value = total."""
        shares = 50
        cash = 5000.0
        close_price = 120.0
        
        result = compute_eod_valuation(shares, cash, close_price)
        expected = cash + (shares * close_price)
        
        assert result == expected


class TestComputeActualWeight:
    """Tests for compute_actual_weight function."""
    
    def test_fully_invested(self):
        """Test weight when 100% in stock."""
        result = compute_actual_weight(shares=100, close_price=100.0, portfolio_value=10000.0)
        assert result == 1.0
    
    def test_all_cash(self):
        """Test weight when 100% in cash."""
        result = compute_actual_weight(shares=0, close_price=100.0, portfolio_value=10000.0)
        assert result == 0.0
    
    def test_half_invested(self):
        """Test weight when 50% invested."""
        result = compute_actual_weight(shares=50, close_price=100.0, portfolio_value=10000.0)
        assert result == 0.5
    
    def test_divide_by_zero_protection(self):
        """Test that zero portfolio value returns 0."""
        result = compute_actual_weight(shares=100, close_price=100.0, portfolio_value=0.0)
        assert result == 0.0
    
    def test_weight_between_zero_and_one(self):
        """Test that weight is always between 0 and 1."""
        result = compute_actual_weight(shares=23, close_price=420.0, portfolio_value=10000.0)
        assert 0.0 <= result <= 1.0
    
    def test_realistic_scenario(self):
        """Test with realistic QQQ scenario."""
        # 23 shares at $420 = $9,660 stock value
        # Portfolio = $10,000
        # Weight = 9660/10000 = 0.966
        result = compute_actual_weight(shares=23, close_price=420.0, portfolio_value=10000.0)
        assert abs(result - 0.966) < 0.001


class TestExecutionMode:
    """Tests for execution mode (Phase 27)."""
    
    def test_next_open_mode_shifts_weight(self):
        """NEXT_OPEN mode shifts Target_Weight by one day."""
        df = pd.DataFrame({
            'Target_Weight': [1.0, 1.0, 0.0, 1.0]
        })
        result = add_exec_target_weight(df, execution_mode="NEXT_OPEN")
        
        # First row is 0, then shifted
        expected = [0.0, 1.0, 1.0, 0.0]
        assert list(result['Exec_Target_Weight']) == expected
    
    def test_same_day_close_no_shift(self):
        """SAME_DAY_CLOSE mode uses same-day Target_Weight (no shift)."""
        df = pd.DataFrame({
            'Target_Weight': [1.0, 1.0, 0.0, 1.0]
        })
        result = add_exec_target_weight(df, execution_mode="SAME_DAY_CLOSE")
        
        # No shift - same as Target_Weight
        expected = [1.0, 1.0, 0.0, 1.0]
        assert list(result['Exec_Target_Weight']) == expected
    
    def test_default_mode_is_next_open(self):
        """Default execution mode is NEXT_OPEN."""
        df = pd.DataFrame({
            'Target_Weight': [1.0, 0.0, 1.0]
        })
        result = add_exec_target_weight(df)  # No mode specified
        
        # Should behave like NEXT_OPEN
        assert result['Exec_Target_Weight'].iloc[0] == 0.0
        assert result['Exec_Target_Weight'].iloc[1] == 1.0
    
    def test_same_day_close_deterministic(self):
        """SAME_DAY_CLOSE uses close price as signal and fill (same bar)."""
        df = pd.DataFrame({
            'Target_Weight': [0.0, 1.0, 1.0, 0.0]
        })
        result = add_exec_target_weight(df, execution_mode="SAME_DAY_CLOSE")
        
        # Output should be deterministic and match input
        assert list(result['Exec_Target_Weight']) == list(df['Target_Weight'])


class TestTradeThrottling:
    """Tests for trade throttling (Phase 29)."""
    
    def test_no_throttling_when_disabled(self):
        """When min_weight_change=0, no throttling applied."""
        weights = pd.Series([0.0, 1.0, 0.98, 0.95, 0.0])
        result = apply_trade_throttling(weights, min_change=0.0)
        assert list(result) == list(weights)
    
    def test_small_changes_suppressed(self):
        """Small weight changes are suppressed."""
        weights = pd.Series([0.0, 1.0, 0.98, 0.96, 0.94])
        result = apply_trade_throttling(weights, min_change=0.05)
        
        # 0->1 is regime switch, allowed
        # 1->0.98 is 2% change, suppressed -> stays 1.0
        # 1->0.96 is 4% change, suppressed -> stays 1.0
        # 1->0.94 is 6% change, allowed -> becomes 0.94
        expected = [0.0, 1.0, 1.0, 1.0, 0.94]
        assert list(result) == expected
    
    def test_regime_switch_always_allowed(self):
        """Changes to/from 0 (regime switches) always allowed."""
        weights = pd.Series([1.0, 0.0, 0.02, 0.0])
        result = apply_trade_throttling(weights, min_change=0.10)
        
        # 1->0 is regime switch, allowed
        # 0->0.02 is regime switch (from 0), allowed
        # 0.02->0 is regime switch (to 0), allowed
        expected = [1.0, 0.0, 0.02, 0.0]
        assert list(result) == expected
    
    def test_large_changes_allowed(self):
        """Large weight changes are allowed."""
        weights = pd.Series([0.0, 1.0, 0.80, 0.60, 0.40])
        result = apply_trade_throttling(weights, min_change=0.05)
        
        # All changes > 5%, all allowed
        assert list(result) == list(weights)
    
    def test_throttling_integrated_with_exec_weight(self):
        """Trade throttling integrates with add_exec_target_weight."""
        df = pd.DataFrame({
            'Target_Weight': [1.0, 0.98, 0.96, 0.90, 0.0]
        })
        result = add_exec_target_weight(
            df,
            execution_mode="SAME_DAY_CLOSE",
            min_weight_change=0.05
        )
        
        # 1.0 -> 0.98 (2%) suppressed
        # 0.98 -> 0.96 (2%) suppressed (but we're at 1.0)
        # 0.96 -> 0.90 - cumulative 10% from 1.0, allowed
        # 0.90 -> 0.0 regime switch, allowed
        expected = [1.0, 1.0, 1.0, 0.90, 0.0]
        assert list(result['Exec_Target_Weight']) == expected


class TestIsNearZero:
    """Tests for is_near_zero function (Phase 41)."""
    
    def test_zero_is_near_zero(self):
        """Test that zero is near zero."""
        assert is_near_zero(0.0) is True
    
    def test_tiny_positive_is_near_zero(self):
        """Test that tiny positive values are near zero."""
        assert is_near_zero(1e-9) is True
        assert is_near_zero(1e-7) is True
    
    def test_tiny_negative_is_near_zero(self):
        """Test that tiny negative values are near zero."""
        assert is_near_zero(-1e-9) is True
        assert is_near_zero(-1e-7) is True
    
    def test_significant_value_not_near_zero(self):
        """Test that significant values are not near zero."""
        assert is_near_zero(0.01) is False
        assert is_near_zero(-0.01) is False
        assert is_near_zero(1e-5) is False


class TestRebalanceBand:
    """Tests for should_rebalance function (Phase 41)."""
    
    def test_tiny_values_treated_as_zero(self):
        """Both effectively zero - no trade needed."""
        assert should_rebalance(1e-9, 0.0, 0.05) is False
    
    def test_within_band_no_trade(self):
        """Test that small drift within band does not trigger trade."""
        assert should_rebalance(0.3333, 0.30, 0.05) is False
    
    def test_outside_band_trades(self):
        """Test that drift outside band triggers trade."""
        assert should_rebalance(0.40, 0.30, 0.05) is True
    
    def test_zero_to_nonzero_always_trades(self):
        """Test that regime switch from 0 to non-zero always trades."""
        assert should_rebalance(0.5, 0.0, 0.05) is True
        assert should_rebalance(0.0001, 0.0, 0.05) is True  # Even tiny target
    
    def test_nonzero_to_zero_always_trades(self):
        """Test that regime switch from non-zero to 0 always trades."""
        assert should_rebalance(0.0, 0.5, 0.05) is True
        assert should_rebalance(0.0, 0.3, 0.05) is True
    
    def test_both_nonzero_within_band(self):
        """Test that both non-zero within band does not trade."""
        assert should_rebalance(0.32, 0.30, 0.05) is False
        assert should_rebalance(0.28, 0.30, 0.05) is False
    
    def test_exact_boundary(self):
        """Test behavior at exact band boundary."""
        # Drift of exactly 0.05 should NOT trigger (must EXCEED band)
        assert should_rebalance(0.35, 0.30, 0.05) is False
        # Drift of 0.0501 should trigger
        assert should_rebalance(0.3501, 0.30, 0.05) is True
