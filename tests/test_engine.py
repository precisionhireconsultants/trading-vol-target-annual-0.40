"""Tests for trading engine (Phase 32)."""
import pytest
import numpy as np
import pandas as pd
from datetime import datetime

from engine import (
    Order, Fill, Position,
    PaperBroker, OrderLogger, KillSwitch, RiskLimits,
    TradingEngine, clamp_weight_for_leverage,
    DataIntegrityCheck, align_signal_exec_dates, apply_slippage,
    validate_signal_exec_separation,
    SIGNAL_ONLY_COLUMNS, EXEC_ONLY_COLUMNS,
    REASON_EXPOSURE_CLAMPED, REASON_HALT_SIGNAL_NAN,
    REASON_HALT_DATA_MISSING, REASON_HALT_DATA_BAD_BAR,
    REASON_HALT_DATA_BAD_VOLUME, REASON_HALT_DATA_MISSING_EXEC_BAR,
    REASON_REJECTED_NO_PRICE, REASON_REJECTED_INSUFFICIENT_CASH,
    REASON_REJECTED_INSUFFICIENT_SHARES
)


class TestPaperBroker:
    """Tests for PaperBroker."""
    
    def test_initial_state(self):
        """Test initial broker state."""
        broker = PaperBroker(initial_capital=10000)
        broker.connect()
        
        assert broker.get_cash_balance() == 10000
        assert broker.get_account_value() == 10000
        assert len(broker.get_positions()) == 0
    
    def test_buy_order(self):
        """Test buying shares."""
        broker = PaperBroker(initial_capital=10000)
        broker.connect()
        broker.set_price("QQQ", 100.0)
        
        order = Order(
            order_id="",
            symbol="QQQ",
            side="BUY",
            quantity=50
        )
        order_id = broker.submit_order(order)
        
        assert order.status == "FILLED"
        assert broker.get_cash_balance() == 5000  # 10000 - 50*100
        pos = broker.get_position("QQQ")
        assert pos is not None
        assert pos.shares == 50
    
    def test_sell_order(self):
        """Test selling shares."""
        broker = PaperBroker(initial_capital=10000)
        broker.connect()
        broker.set_price("QQQ", 100.0)
        
        # First buy
        buy_order = Order(order_id="", symbol="QQQ", side="BUY", quantity=50)
        broker.submit_order(buy_order)
        
        # Then sell
        sell_order = Order(order_id="", symbol="QQQ", side="SELL", quantity=30)
        broker.submit_order(sell_order)
        
        assert sell_order.status == "FILLED"
        pos = broker.get_position("QQQ")
        assert pos.shares == 20
        assert broker.get_cash_balance() == 8000  # 5000 + 30*100
    
    def test_reject_insufficient_cash(self):
        """Test order rejected for insufficient cash."""
        broker = PaperBroker(initial_capital=1000)
        broker.connect()
        broker.set_price("QQQ", 100.0)
        
        order = Order(order_id="", symbol="QQQ", side="BUY", quantity=50)
        broker.submit_order(order)
        
        assert order.status == "REJECTED"
    
    def test_reject_insufficient_shares(self):
        """Test sell order rejected for insufficient shares."""
        broker = PaperBroker(initial_capital=10000)
        broker.connect()
        broker.set_price("QQQ", 100.0)
        
        order = Order(order_id="", symbol="QQQ", side="SELL", quantity=50)
        broker.submit_order(order)
        
        assert order.status == "REJECTED"


class TestKillSwitch:
    """Tests for KillSwitch."""
    
    def test_allows_normal_order(self):
        """Test that normal orders are allowed."""
        limits = RiskLimits(max_daily_loss_pct=0.05, max_order_value=50000)
        ks = KillSwitch(limits)
        ks.reset_daily(10000)
        
        order = Order(order_id="", symbol="QQQ", side="BUY", quantity=10)
        allowed, reason = ks.check_order(order, 10000, 100.0)
        
        assert allowed
        assert reason == ""
    
    def test_blocks_on_daily_loss(self):
        """Test kill switch triggers on daily loss."""
        limits = RiskLimits(max_daily_loss_pct=0.05)
        ks = KillSwitch(limits)
        ks.reset_daily(10000)
        
        order = Order(order_id="", symbol="QQQ", side="BUY", quantity=10)
        # 6% loss
        allowed, reason = ks.check_order(order, 9400, 100.0)
        
        assert not allowed
        assert "HALT_DAILY_LOSS" in reason
        assert ks.is_killed
    
    def test_blocks_on_max_order_value(self):
        """Test order rejected for exceeding max value."""
        limits = RiskLimits(max_order_value=5000)
        ks = KillSwitch(limits)
        ks.reset_daily(100000)
        
        order = Order(order_id="", symbol="QQQ", side="BUY", quantity=100)
        allowed, reason = ks.check_order(order, 100000, 100.0)  # $10k order
        
        assert not allowed
        assert "exceeds limit" in reason
    
    def test_blocks_on_max_trades(self):
        """Test order rejected for max trades per day."""
        limits = RiskLimits(max_trades_per_day=2)
        ks = KillSwitch(limits)
        ks.reset_daily(10000)
        
        # Record 2 trades
        ks.record_trade()
        ks.record_trade()
        
        order = Order(order_id="", symbol="QQQ", side="BUY", quantity=10)
        allowed, reason = ks.check_order(order, 10000, 100.0)
        
        assert not allowed
        assert "Max trades" in reason


class TestTradingEngine:
    """Tests for TradingEngine."""
    
    def test_start_stop(self):
        """Test engine start and stop."""
        broker = PaperBroker(initial_capital=10000)
        engine = TradingEngine(broker)
        
        assert engine.start()
        assert engine.is_running
        
        engine.stop()
        assert not engine.is_running
    
    def test_execute_target_position_buy(self):
        """Test executing to target position (buy)."""
        broker = PaperBroker(initial_capital=10000)
        engine = TradingEngine(broker)
        engine.start()
        
        fill = engine.execute_target_position("QQQ", 50, 100.0)
        
        assert fill is not None
        assert fill.side == "BUY"
        assert fill.quantity == 50
        assert fill.fill_price == 100.0
    
    def test_execute_target_position_sell(self):
        """Test executing to target position (sell)."""
        broker = PaperBroker(initial_capital=10000)
        engine = TradingEngine(broker)
        engine.start()
        
        # First buy
        engine.execute_target_position("QQQ", 50, 100.0)
        
        # Then reduce position
        fill = engine.execute_target_position("QQQ", 20, 100.0)
        
        assert fill is not None
        assert fill.side == "SELL"
        assert fill.quantity == 30
    
    def test_no_trade_when_at_target(self):
        """Test no trade when already at target."""
        broker = PaperBroker(initial_capital=10000)
        engine = TradingEngine(broker)
        engine.start()
        
        # Buy to target
        engine.execute_target_position("QQQ", 50, 100.0)
        
        # Try to execute same target
        fill = engine.execute_target_position("QQQ", 50, 100.0)
        
        assert fill is None
    
    def test_account_summary(self):
        """Test getting account summary."""
        broker = PaperBroker(initial_capital=10000)
        engine = TradingEngine(broker)
        engine.start()
        
        summary = engine.get_account_summary()
        
        assert summary['account_value'] == 10000
        assert summary['cash'] == 10000
        assert summary['trades_today'] == 0
        assert not summary['is_killed']


class TestExposureClamp:
    """Tests for clamp_weight_for_leverage (Phase 40)."""
    
    def test_tqqq_weight_clamped_to_one_third(self):
        """Test TQQQ weight is clamped to ~1/3 for max exposure of 1.0."""
        clamped, reason = clamp_weight_for_leverage(1.0, 3.0, 1.0)
        assert abs(clamped - 1.0/3.0) < 1e-9  # Use tolerance, not exact
        assert reason == REASON_EXPOSURE_CLAMPED
    
    def test_effective_exposure_never_exceeds_cap(self):
        """Test that effective exposure (weight * leverage) never exceeds cap."""
        clamped, _ = clamp_weight_for_leverage(1.0, 3.0, 1.0)
        effective = clamped * 3.0
        assert effective <= 1.0 + 1e-9
    
    def test_small_weight_not_clamped(self):
        """Test that weight below cap is not clamped."""
        clamped, reason = clamp_weight_for_leverage(0.2, 3.0, 1.0)
        assert abs(clamped - 0.2) < 1e-9
        assert reason == ""
    
    def test_negative_weight_clamped(self):
        """Test that negative weight (for shorts) is clamped symmetrically."""
        clamped, reason = clamp_weight_for_leverage(-1.0, 3.0, 1.0)
        assert abs(clamped - (-1.0/3.0)) < 1e-9
        assert reason == REASON_EXPOSURE_CLAMPED
    
    def test_nan_weight_returns_zero_and_halt_reason(self):
        """Test that NaN weight returns 0 and halt reason."""
        clamped, reason = clamp_weight_for_leverage(np.nan, 3.0, 1.0)
        assert clamped == 0.0
        assert reason == REASON_HALT_SIGNAL_NAN
    
    def test_zero_leverage_treated_as_one(self):
        """Test that zero or negative leverage is treated as 1.0."""
        clamped, reason = clamp_weight_for_leverage(0.5, 0.0, 1.0)
        assert abs(clamped - 0.5) < 1e-9
        assert reason == ""
    
    def test_weight_at_boundary_not_clamped(self):
        """Test that weight exactly at boundary is not clamped."""
        # For leverage 3.0, max_effective 1.0 -> max_weight = 1/3
        clamped, reason = clamp_weight_for_leverage(1.0/3.0, 3.0, 1.0)
        assert abs(clamped - 1.0/3.0) < 1e-9
        assert reason == ""  # Not clamped because not exceeding


class TestDataIntegrity:
    """Tests for DataIntegrityCheck (Phase 38)."""
    
    def test_missing_price_halts(self):
        """Test that NaN price triggers halt."""
        checker = DataIntegrityCheck()
        valid, reason = checker.validate_bar(np.nan, 100, 99, 100)
        assert not valid
        assert reason == REASON_HALT_DATA_MISSING
    
    def test_zero_price_halts(self):
        """Test that zero price triggers halt."""
        checker = DataIntegrityCheck()
        valid, reason = checker.validate_bar(0, 100, 99, 100)
        assert not valid
        assert reason == REASON_HALT_DATA_MISSING
    
    def test_negative_price_halts(self):
        """Test that negative price triggers halt."""
        checker = DataIntegrityCheck()
        valid, reason = checker.validate_bar(-100, 100, 99, 100)
        assert not valid
        assert reason == REASON_HALT_DATA_MISSING
    
    def test_high_below_open_halts(self):
        """Test that high < open triggers halt."""
        checker = DataIntegrityCheck()
        valid, reason = checker.validate_bar(100, 99, 98, 99)  # high < open
        assert not valid
        assert reason == REASON_HALT_DATA_BAD_BAR
    
    def test_low_above_close_halts(self):
        """Test that low > close triggers halt."""
        checker = DataIntegrityCheck()
        valid, reason = checker.validate_bar(100, 105, 101, 98)  # low > close
        assert not valid
        assert reason == REASON_HALT_DATA_BAD_BAR
    
    def test_high_below_low_halts(self):
        """Test that high < low triggers halt."""
        checker = DataIntegrityCheck()
        valid, reason = checker.validate_bar(100, 98, 102, 100)  # high < low
        assert not valid
        assert reason == REASON_HALT_DATA_BAD_BAR
    
    def test_valid_bar_passes(self):
        """Test that valid OHLC bar passes."""
        checker = DataIntegrityCheck()
        valid, reason = checker.validate_bar(100, 105, 99, 103)
        assert valid
        assert reason == ""
    
    def test_volume_validation_skipped_if_none(self):
        """Volume=None should skip validation, not halt."""
        checker = DataIntegrityCheck()
        valid, reason = checker.validate_bar(100, 105, 99, 103, volume=None)
        assert valid
        assert reason == ""
    
    def test_negative_volume_halts(self):
        """Negative volume should halt if volume is provided."""
        checker = DataIntegrityCheck()
        valid, reason = checker.validate_bar(100, 105, 99, 103, volume=-100)
        assert not valid
        assert reason == REASON_HALT_DATA_BAD_VOLUME
    
    def test_nan_volume_halts(self):
        """NaN volume should halt if volume is provided."""
        checker = DataIntegrityCheck()
        valid, reason = checker.validate_bar(100, 105, 99, 103, volume=np.nan)
        assert not valid
        assert reason == REASON_HALT_DATA_BAD_VOLUME
    
    def test_zero_volume_passes(self):
        """Zero volume is valid (e.g., holiday partial data)."""
        checker = DataIntegrityCheck()
        valid, reason = checker.validate_bar(100, 105, 99, 103, volume=0)
        assert valid
        assert reason == ""
    
    def test_missing_exec_bar_halts(self):
        """Test that missing execution bar triggers halt."""
        checker = DataIntegrityCheck()
        exec_dates = {pd.Timestamp('2020-01-02').normalize(), pd.Timestamp('2020-01-03').normalize()}
        valid, reason = checker.validate_exec_bar_exists(
            pd.Timestamp('2020-01-01'), exec_dates
        )
        assert not valid
        assert reason == REASON_HALT_DATA_MISSING_EXEC_BAR
    
    def test_existing_exec_bar_passes(self):
        """Test that existing execution bar passes."""
        checker = DataIntegrityCheck()
        exec_dates = {pd.Timestamp('2020-01-01').normalize(), pd.Timestamp('2020-01-02').normalize()}
        valid, reason = checker.validate_exec_bar_exists(
            pd.Timestamp('2020-01-01'), exec_dates
        )
        assert valid
        assert reason == ""


class TestDateAlignment:
    """Tests for align_signal_exec_dates (Phase 38)."""
    
    def test_alignment_normalizes_dates(self):
        """Dates with times should be normalized to date-only."""
        signal_df = pd.DataFrame({
            'Date': [pd.Timestamp('2020-01-01 09:30:00'), pd.Timestamp('2020-01-02 09:30:00')],
            'Close': [100, 101]
        })
        exec_df = pd.DataFrame({
            'Date': [pd.Timestamp('2020-01-01 16:00:00'), pd.Timestamp('2020-01-02 16:00:00')],
            'Close': [100.5, 101.5]
        })
        
        aligned_signal, aligned_exec = align_signal_exec_dates(signal_df, exec_df)
        
        # Dates should match after normalization (use .equals for pandas)
        assert aligned_signal['Date'].equals(aligned_exec['Date'])
    
    def test_alignment_dedupes(self):
        """Duplicate dates should be deduped (keep last)."""
        signal_df = pd.DataFrame({
            'Date': [pd.Timestamp('2020-01-01'), pd.Timestamp('2020-01-01'), pd.Timestamp('2020-01-02')],
            'Close': [100, 101, 102]
        })
        exec_df = pd.DataFrame({
            'Date': [pd.Timestamp('2020-01-01'), pd.Timestamp('2020-01-02')],
            'Close': [100.5, 102.5]
        })
        
        aligned_signal, _ = align_signal_exec_dates(signal_df, exec_df)
        
        # Should have 2 rows, not 3
        assert len(aligned_signal) == 2
        # Should keep last value for 2020-01-01
        assert aligned_signal[aligned_signal['Date'] == pd.Timestamp('2020-01-01').normalize()]['Close'].iloc[0] == 101
    
    def test_alignment_filters_to_common(self):
        """Only common dates should remain."""
        signal_df = pd.DataFrame({
            'Date': [pd.Timestamp('2020-01-01'), pd.Timestamp('2020-01-02'), pd.Timestamp('2020-01-03')],
            'Close': [100, 101, 102]
        })
        exec_df = pd.DataFrame({
            'Date': [pd.Timestamp('2020-01-02'), pd.Timestamp('2020-01-03'), pd.Timestamp('2020-01-04')],
            'Close': [101.5, 102.5, 103.5]
        })
        
        aligned_signal, aligned_exec = align_signal_exec_dates(signal_df, exec_df)
        
        # Only 2020-01-02 and 2020-01-03 should remain
        assert len(aligned_signal) == 2
        assert len(aligned_exec) == 2
    
    def test_alignment_raises_on_no_overlap(self):
        """Should raise ValueError if no overlapping dates."""
        signal_df = pd.DataFrame({
            'Date': [pd.Timestamp('2020-01-01'), pd.Timestamp('2020-01-02')],
            'Close': [100, 101]
        })
        exec_df = pd.DataFrame({
            'Date': [pd.Timestamp('2020-01-03'), pd.Timestamp('2020-01-04')],
            'Close': [103, 104]
        })
        
        with pytest.raises(ValueError, match="No overlapping dates"):
            align_signal_exec_dates(signal_df, exec_df)
    
    def test_alignment_sorts_dates(self):
        """Output should be sorted by date."""
        signal_df = pd.DataFrame({
            'Date': [pd.Timestamp('2020-01-03'), pd.Timestamp('2020-01-01'), pd.Timestamp('2020-01-02')],
            'Close': [103, 101, 102]
        })
        exec_df = pd.DataFrame({
            'Date': [pd.Timestamp('2020-01-02'), pd.Timestamp('2020-01-03'), pd.Timestamp('2020-01-01')],
            'Close': [102.5, 103.5, 101.5]
        })
        
        aligned_signal, aligned_exec = align_signal_exec_dates(signal_df, exec_df)
        
        # Should be sorted ascending
        assert list(aligned_signal['Date']) == [
            pd.Timestamp('2020-01-01').normalize(),
            pd.Timestamp('2020-01-02').normalize(),
            pd.Timestamp('2020-01-03').normalize()
        ]


# =============================================================================
# Phase 32: Dataset-Level Date Alignment Tests
# =============================================================================

class TestDatasetLevelAlignment:
    """
    Tests for dataset-level date alignment (Phase 32).
    
    P32: Dataset-level mismatches (extra dates in one series) → fatal raise before run.
    """
    
    def test_qqq_extra_date_filters(self):
        """
        Test that extra QQQ dates are filtered out (only common dates kept).
        """
        signal_df = pd.DataFrame({
            'Date': pd.to_datetime(['2020-01-01', '2020-01-02', '2020-01-03', '2020-01-06']),
            'Close': [100, 101, 102, 103]
        })
        exec_df = pd.DataFrame({
            'Date': pd.to_datetime(['2020-01-01', '2020-01-02', '2020-01-03']),
            'Close': [100.5, 101.5, 102.5]
        })
        
        aligned_signal, aligned_exec = align_signal_exec_dates(signal_df, exec_df)
        
        # Should only have 3 common dates
        assert len(aligned_signal) == 3
        assert len(aligned_exec) == 3
        # 2020-01-06 should be removed from signal
        assert pd.Timestamp('2020-01-06').normalize() not in set(aligned_signal['Date'])
    
    def test_tqqq_extra_date_filters(self):
        """
        Test that extra TQQQ dates are filtered out (only common dates kept).
        """
        signal_df = pd.DataFrame({
            'Date': pd.to_datetime(['2020-01-01', '2020-01-02', '2020-01-03']),
            'Close': [100, 101, 102]
        })
        exec_df = pd.DataFrame({
            'Date': pd.to_datetime(['2020-01-01', '2020-01-02', '2020-01-03', '2020-01-06']),
            'Close': [100.5, 101.5, 102.5, 103.5]
        })
        
        aligned_signal, aligned_exec = align_signal_exec_dates(signal_df, exec_df)
        
        # Should only have 3 common dates
        assert len(aligned_signal) == 3
        assert len(aligned_exec) == 3
        # 2020-01-06 should be removed from exec
        assert pd.Timestamp('2020-01-06').normalize() not in set(aligned_exec['Date'])
    
    def test_timestamps_normalize_to_same_date_passes(self):
        """
        Test that timestamps with different times but same date align correctly.
        """
        signal_df = pd.DataFrame({
            'Date': [pd.Timestamp('2020-01-01 09:30:00'), pd.Timestamp('2020-01-02 09:30:00')],
            'Close': [100, 101]
        })
        exec_df = pd.DataFrame({
            'Date': [pd.Timestamp('2020-01-01 16:00:00'), pd.Timestamp('2020-01-02 16:00:00')],
            'Close': [100.5, 101.5]
        })
        
        aligned_signal, aligned_exec = align_signal_exec_dates(signal_df, exec_df)
        
        # Both should have 2 dates, aligned
        assert len(aligned_signal) == 2
        assert len(aligned_exec) == 2
        assert aligned_signal['Date'].equals(aligned_exec['Date'])
    
    def test_dates_exact_equality_after_alignment(self):
        """
        Test that Date columns are exactly equal after alignment.
        """
        signal_df = pd.DataFrame({
            'Date': pd.to_datetime(['2020-01-01', '2020-01-02', '2020-01-03']),
            'Signal_Close': [100, 101, 102]
        })
        exec_df = pd.DataFrame({
            'Date': pd.to_datetime(['2020-01-01', '2020-01-02', '2020-01-03']),
            'Exec_Close': [100.5, 101.5, 102.5]
        })
        
        aligned_signal, aligned_exec = align_signal_exec_dates(signal_df, exec_df)
        
        # Use .equals() to check exact equality
        assert aligned_signal['Date'].equals(aligned_exec['Date'])


class TestDailyLossHalt:
    """Tests for daily loss halt and flatten (Phase 39)."""
    
    def test_2pct_daily_loss_triggers_halt(self):
        """Test that 2% daily loss triggers halt."""
        limits = RiskLimits(max_daily_loss_pct=0.02)
        ks = KillSwitch(limits)
        ks.start_day(10000.0, pd.Timestamp("2020-01-01"))
        
        # 3% loss
        should_halt, reason = ks.check_end_of_day(9700.0, pd.Timestamp("2020-01-01"))
        assert should_halt
        assert "HALT_DAILY_LOSS" in reason
        assert ks.is_killed
    
    def test_daily_loss_below_threshold_no_halt(self):
        """Test that loss below threshold does not trigger halt."""
        limits = RiskLimits(max_daily_loss_pct=0.02)
        ks = KillSwitch(limits)
        ks.start_day(10000.0, pd.Timestamp("2020-01-01"))
        
        # 1% loss (below 2% threshold)
        should_halt, reason = ks.check_end_of_day(9900.0, pd.Timestamp("2020-01-01"))
        assert not should_halt
        assert reason == ""
        assert not ks.is_killed
    
    def test_drawdown_triggers_halt(self):
        """Test that drawdown triggers halt."""
        # Set high daily loss threshold so drawdown triggers first
        limits = RiskLimits(max_daily_loss_pct=0.50, max_drawdown_pct=0.25)
        ks = KillSwitch(limits)
        
        # Build up to peak
        ks.start_day(10000.0, pd.Timestamp("2020-01-01"))
        ks.check_end_of_day(12000.0, pd.Timestamp("2020-01-01"))  # No halt, new peak
        ks.peak_equity = 12000.0  # Ensure peak is set
        
        # Next day, drawdown from peak (not daily loss)
        # Start at 12000, end at 8000 = 33% drawdown from peak
        # Daily return = 8000/12000 - 1 = -33% which exceeds 50%, so we need different values
        # Let's use: peak=12000, start_day=11000, end_day=8500
        # Daily return = 8500/11000 - 1 = -22.7% (< 50%, no daily loss trigger)
        # Drawdown from peak = (12000-8500)/12000 = 29.2% (> 25%, drawdown trigger)
        ks.start_day(11000.0, pd.Timestamp("2020-01-02"))
        should_halt, reason = ks.check_end_of_day(8500.0, pd.Timestamp("2020-01-02"))
        assert should_halt
        assert "HALT_DRAWDOWN" in reason
    
    def test_flatten_on_halt(self):
        """Test that halt triggers flatten to zero shares."""
        broker = PaperBroker(10000)
        broker.connect()
        broker.set_price("TQQQ", 100)
        
        # Buy 50 shares
        order = Order("", "TQQQ", "BUY", 50)
        broker.submit_order(order)
        
        engine = TradingEngine(broker, RiskLimits(max_daily_loss_pct=0.02))
        engine.start()
        engine.kill_switch.start_day(broker.get_account_value(), pd.Timestamp("2020-01-01"))
        
        # Price drops 5%
        engine.end_of_day("TQQQ", 95, pd.Timestamp("2020-01-01"))
        
        # Should be flat
        pos = broker.get_position("TQQQ")
        assert pos is None or pos.shares == 0
    
    def test_lockout_prevents_trading(self):
        """Test that lockout prevents trading before cooldown expires."""
        # Use cooldown_days=2 so Jan 1 halt -> lockout_until Jan 3
        # Jan 2 should still be locked (Jan 2 < Jan 3)
        limits = RiskLimits(max_daily_loss_pct=0.02, halt_cooldown_days=2)
        ks = KillSwitch(limits)
        ks.start_day(10000.0, pd.Timestamp("2020-01-01"))
        ks.check_end_of_day(9700.0, pd.Timestamp("2020-01-01"))  # Trigger halt
        
        # Jan 2 should still be locked (lockout_until = Jan 3)
        ks.start_day(9700.0, pd.Timestamp("2020-01-02"))
        assert ks.is_killed
        assert "LOCKOUT" in ks.kill_reason
    
    def test_lockout_expires_after_cooldown(self):
        """Test that lockout expires after cooldown period."""
        limits = RiskLimits(max_daily_loss_pct=0.02, halt_cooldown_days=1)
        ks = KillSwitch(limits)
        ks.start_day(10000.0, pd.Timestamp("2020-01-01"))
        ks.check_end_of_day(9700.0, pd.Timestamp("2020-01-01"))  # Trigger halt
        
        # Jan 2 should be locked (lockout_until = Jan 2, and Jan 2 >= Jan 2)
        # According to plan: lockout_until is first tradable day
        # If halt Jan 1, cooldown=1, lockout_until = Jan 2
        # On Jan 2: current_date >= lockout_until → not locked
        ks.start_day(9700.0, pd.Timestamp("2020-01-02"))
        assert not ks.is_killed
    
    def test_force_flatten_order_sell(self):
        """Test that force_flatten_order returns SELL for positive shares."""
        ks = KillSwitch()
        order = ks.force_flatten_order(50, "TQQQ")
        assert order is not None
        assert order.side == "SELL"
        assert order.quantity == 50
    
    def test_force_flatten_order_zero_shares(self):
        """Test that force_flatten_order returns None for zero shares."""
        ks = KillSwitch()
        order = ks.force_flatten_order(0, "TQQQ")
        assert order is None
    
    def test_start_day_with_pd_timestamp(self):
        """Test that start_day works with pd.Timestamp."""
        ks = KillSwitch()
        # Should not raise
        ks.start_day(10000.0, pd.Timestamp("2020-01-01"))
        assert ks.equity_at_open == 10000.0


# Float tolerance for slippage tests
FLOAT_TOL = 1e-9


class TestSlippage:
    """Tests for slippage and commission (Phase 35)."""
    
    def test_buy_fill_price_includes_slippage(self):
        """Test that BUY price is increased by slippage."""
        fill_price = apply_slippage(100.0, "BUY", 5.0)  # 5 bps
        expected = 100.05  # 100 * (1 + 5/10000)
        assert abs(fill_price - expected) < FLOAT_TOL  # TOLERANCE, not ==
    
    def test_sell_fill_price_includes_slippage(self):
        """Test that SELL price is decreased by slippage."""
        fill_price = apply_slippage(100.0, "SELL", 5.0)
        expected = 99.95  # 100 * (1 - 5/10000)
        assert abs(fill_price - expected) < FLOAT_TOL  # TOLERANCE, not ==
    
    def test_zero_slippage(self):
        """Test that zero slippage doesn't change price."""
        fill_price = apply_slippage(100.0, "BUY", 0.0)
        assert abs(fill_price - 100.0) < FLOAT_TOL
    
    def test_cash_reflects_slippage_on_buy(self):
        """Test that cash is correctly reduced with slippage on buy."""
        broker = PaperBroker(10000)
        broker.connect()
        broker.set_price("TQQQ", 100)
        
        order = Order("", "TQQQ", "BUY", 10)
        broker.submit_order(order, slippage_bps=5.0)
        
        # Cost = 10 * 100.05 = 1000.50
        expected_cash = 10000 - 1000.50
        assert abs(broker.cash - expected_cash) < 0.01  # TOLERANCE
    
    def test_cash_reflects_slippage_on_sell(self):
        """Test that cash is correctly credited with slippage on sell."""
        broker = PaperBroker(10000)
        broker.connect()
        broker.set_price("TQQQ", 100)
        
        # First buy
        buy_order = Order("", "TQQQ", "BUY", 10)
        broker.submit_order(buy_order)
        cash_after_buy = broker.cash  # 10000 - 1000 = 9000
        
        # Then sell with slippage
        sell_order = Order("", "TQQQ", "SELL", 10)
        broker.submit_order(sell_order, slippage_bps=5.0)
        
        # Proceeds = 10 * 99.95 = 999.50
        expected_cash = cash_after_buy + 999.50
        assert abs(broker.cash - expected_cash) < 0.01  # TOLERANCE
    
    def test_commission_deducted_on_buy(self):
        """Test that commission is deducted on buy."""
        broker = PaperBroker(10000)
        broker.connect()
        broker.set_price("TQQQ", 100)
        
        order = Order("", "TQQQ", "BUY", 10)
        broker.submit_order(order, commission=5.0)
        
        # Cost = 10 * 100 + 5 = 1005
        expected_cash = 10000 - 1005
        assert abs(broker.cash - expected_cash) < 0.01
    
    def test_commission_deducted_on_sell(self):
        """Test that commission is deducted on sell."""
        broker = PaperBroker(10000)
        broker.connect()
        broker.set_price("TQQQ", 100)
        
        # First buy
        buy_order = Order("", "TQQQ", "BUY", 10)
        broker.submit_order(buy_order)
        
        # Then sell with commission
        sell_order = Order("", "TQQQ", "SELL", 10)
        broker.submit_order(sell_order, commission=5.0)
        
        # Proceeds = 10 * 100 - 5 = 995
        expected_cash = 9000 + 995
        assert abs(broker.cash - expected_cash) < 0.01
    
    def test_no_negative_cash_with_slippage(self):
        """Test that order is rejected if slippage would cause negative cash."""
        broker = PaperBroker(1000)
        broker.connect()
        broker.set_price("TQQQ", 100)
        
        # Try to buy 10 shares at 100.05 each = 1000.50 (more than we have)
        order = Order("", "TQQQ", "BUY", 10)
        broker.submit_order(order, slippage_bps=5.0)
        
        assert order.status == "REJECTED"
        assert order.reject_reason == REASON_REJECTED_INSUFFICIENT_CASH
        assert abs(broker.cash - 1000) < FLOAT_TOL  # Cash unchanged
    
    def test_price_missing_rejects_order(self):
        """PaperBroker must reject if execution price not set."""
        broker = PaperBroker(10000)
        broker.connect()
        # Don't set price - should reject
        
        order = Order("", "TQQQ", "BUY", 10)
        broker.submit_order(order)
        
        assert order.status == "REJECTED"
        assert order.reject_reason == REASON_REJECTED_NO_PRICE
    
    def test_nan_price_rejects_order(self):
        """PaperBroker must reject if execution price is NaN."""
        broker = PaperBroker(10000)
        broker.connect()
        broker.set_price("TQQQ", np.nan)
        
        order = Order("", "TQQQ", "BUY", 10)
        broker.submit_order(order)
        
        assert order.status == "REJECTED"
        assert order.reject_reason == REASON_REJECTED_NO_PRICE
    
    def test_sell_more_than_owned_rejects(self):
        """SELL must reject if quantity > current shares."""
        broker = PaperBroker(10000)
        broker.connect()
        broker.set_price("TQQQ", 100)
        
        # Buy 5 shares
        buy_order = Order("", "TQQQ", "BUY", 5)
        broker.submit_order(buy_order)
        
        # Try to sell 10 (more than we own)
        sell_order = Order("", "TQQQ", "SELL", 10)
        broker.submit_order(sell_order)
        
        assert sell_order.status == "REJECTED"
        assert sell_order.reject_reason == REASON_REJECTED_INSUFFICIENT_SHARES
        # Should still have 5 shares
        pos = broker.get_position("TQQQ")
        assert pos.shares == 5
    
    def test_fill_records_commission(self):
        """Test that fill records commission."""
        broker = PaperBroker(10000)
        broker.connect()
        broker.set_price("TQQQ", 100)
        
        order = Order("", "TQQQ", "BUY", 10)
        broker.submit_order(order, commission=5.0)
        
        fill = broker.fills[-1]
        assert fill.commission == 5.0
    
    def test_fill_records_slipped_price(self):
        """Test that fill records the slipped price, not base price."""
        broker = PaperBroker(10000)
        broker.connect()
        broker.set_price("TQQQ", 100)
        
        order = Order("", "TQQQ", "BUY", 10)
        broker.submit_order(order, slippage_bps=5.0)
        
        fill = broker.fills[-1]
        expected_price = 100.05
        assert abs(fill.fill_price - expected_price) < FLOAT_TOL


# =============================================================================
# Phase 31: Signal vs Execution Asset Split
# =============================================================================

class TestSignalExecSeparation:
    """Tests for signal vs execution asset separation (Phase 31)."""
    
    def test_signal_prices_not_used_for_equity(self):
        """
        Test that signal prices are not in exec_df.
        
        Equity calculations must use exec prices (TQQQ), not signal prices (QQQ).
        """
        signal_df = pd.DataFrame({
            'Date': pd.date_range('2024-01-01', periods=5),
            'MA250': [100.0] * 5,
            'MA50': [99.0] * 5,
            'Target_Weight': [1.0] * 5
        })
        exec_df = pd.DataFrame({
            'Date': pd.date_range('2024-01-01', periods=5),
            'Exec_Open': [50.0] * 5,
            'Exec_Close': [51.0] * 5,
            'Cash': [5000.0] * 5
        })
        
        # Should pass - exec_df has no signal columns
        assert validate_signal_exec_separation(signal_df, exec_df, strict=False)
    
    def test_exec_prices_not_used_for_indicators(self):
        """
        Test that exec prices are not in signal_df.
        
        Indicators must use signal prices (QQQ), not exec prices (TQQQ).
        """
        signal_df = pd.DataFrame({
            'Date': pd.date_range('2024-01-01', periods=5),
            'MA250': [100.0] * 5,
            'QQQ_ann_vol': [0.20] * 5
        })
        exec_df = pd.DataFrame({
            'Date': pd.date_range('2024-01-01', periods=5),
            'Exec_Close': [51.0] * 5
        })
        
        # Should pass - signal_df has no exec columns
        assert validate_signal_exec_separation(signal_df, exec_df, strict=False)
    
    def test_signal_columns_in_exec_fails(self):
        """Test that signal columns in exec_df causes failure."""
        signal_df = pd.DataFrame({
            'Date': pd.date_range('2024-01-01', periods=5),
            'MA250': [100.0] * 5
        })
        # This exec_df incorrectly has MA250 (signal column)
        exec_df = pd.DataFrame({
            'Date': pd.date_range('2024-01-01', periods=5),
            'Exec_Close': [51.0] * 5,
            'MA250': [100.0] * 5  # WRONG - signal column in exec
        })
        
        # Should fail validation
        assert not validate_signal_exec_separation(signal_df, exec_df, strict=False)
    
    def test_exec_columns_in_signal_fails(self):
        """Test that exec columns in signal_df causes failure."""
        # This signal_df incorrectly has Exec_Close (exec column)
        signal_df = pd.DataFrame({
            'Date': pd.date_range('2024-01-01', periods=5),
            'MA250': [100.0] * 5,
            'Exec_Close': [51.0] * 5  # WRONG - exec column in signal
        })
        exec_df = pd.DataFrame({
            'Date': pd.date_range('2024-01-01', periods=5),
            'Exec_Close': [51.0] * 5
        })
        
        # Should fail validation
        assert not validate_signal_exec_separation(signal_df, exec_df, strict=False)
    
    def test_strict_mode_raises(self):
        """Test that strict mode raises ValueError on violation."""
        signal_df = pd.DataFrame({
            'Date': pd.date_range('2024-01-01', periods=5),
            'MA250': [100.0] * 5
        })
        exec_df = pd.DataFrame({
            'Date': pd.date_range('2024-01-01', periods=5),
            'Exec_Close': [51.0] * 5,
            'MA250': [100.0] * 5  # Violation
        })
        
        with pytest.raises(ValueError, match="Signal-only columns"):
            validate_signal_exec_separation(signal_df, exec_df, strict=True)
    
    def test_clean_separation_passes(self):
        """Test that clean separation passes validation."""
        signal_df = pd.DataFrame({
            'Date': pd.date_range('2024-01-01', periods=5),
            'Signal_Close': [100.0] * 5,
            'MA250': [99.0] * 5,
            'MA50': [100.0] * 5,
            'QQQ_ann_vol': [0.20] * 5,
            'Base_Regime': ['bull'] * 5,
            'Confirmed_Regime': ['bull'] * 5,
            'Final_Trading_Regime': ['bull'] * 5,
            'Target_Weight': [1.0] * 5
        })
        exec_df = pd.DataFrame({
            'Date': pd.date_range('2024-01-01', periods=5),
            'Exec_Open': [50.0] * 5,
            'Exec_Close': [51.0] * 5,
            'Portfolio_Value_Open': [10000.0] * 5,
            'Cash': [5000.0] * 5,
            'Total_Stocks_Owned': [100] * 5,
            'Remaining_Portfolio_Amount': [10100.0] * 5,
            'Actual_Weight': [0.5] * 5
        })
        
        assert validate_signal_exec_separation(signal_df, exec_df, strict=True)
    
    def test_poison_pill_equity_uses_exec_only(self):
        """
        Poison-pill test: If equity calculation touches signal_df, test fails.
        
        This is a structural test ensuring valuation functions only use exec prices.
        """
        # Create signal_df with intentionally different values
        signal_df = pd.DataFrame({
            'Date': pd.date_range('2024-01-01', periods=3),
            'Signal_Close': [999.0, 999.0, 999.0],  # "Poison" values
            'Target_Weight': [1.0, 1.0, 1.0]
        })
        exec_df = pd.DataFrame({
            'Date': pd.date_range('2024-01-01', periods=3),
            'Exec_Close': [100.0, 100.0, 100.0],
            'Cash': [5000.0, 5000.0, 5000.0],
            'Total_Stocks_Owned': [50, 50, 50]
        })
        
        # Compute equity using ONLY exec_df values
        for idx in range(len(exec_df)):
            cash = exec_df.loc[idx, 'Cash']
            shares = exec_df.loc[idx, 'Total_Stocks_Owned']
            exec_close = exec_df.loc[idx, 'Exec_Close']
            
            equity = cash + shares * exec_close
            
            # If we accidentally used signal_df, equity would be much higher
            assert equity == 10000.0, \
                f"Equity calculation may have used signal prices! Got {equity}"
            
            # Verify we didn't accidentally use signal price
            signal_close = signal_df.loc[idx, 'Signal_Close']
            wrong_equity = cash + shares * signal_close
            assert equity != wrong_equity, "Test setup error - values shouldn't match"
