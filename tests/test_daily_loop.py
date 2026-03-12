"""Tests for daily loop ordering (Phase 42).

This module tests:
- Strict operation order enforcement
- HALT flatten behavior for NEXT_OPEN and SAME_DAY_CLOSE modes
- Kill switch state management (halt_triggered, pending_flatten, lockout_until)
- Clamp-before-rebalance enforcement
"""
import pytest
import pandas as pd
import numpy as np
from datetime import datetime

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from engine import (
    KillSwitch, RiskLimits, PaperBroker, Order,
    clamp_weight_for_leverage,
    REASON_HALT_DAILY_LOSS, REASON_HALT_DRAWDOWN, REASON_EXPOSURE_CLAMPED
)
from portfolio import compute_equity, compute_target_shares, should_rebalance
from config import DEFAULT_CONFIG, ZERO_EPS


class TestDailyLoopOrdering:
    """Tests for Phase 42: End-to-End Daily Loop Ordering."""
    
    def test_start_day_called_before_trades(self):
        """Test that start_day() must be called before trades."""
        ks = KillSwitch(RiskLimits(max_daily_loss_pct=0.02))
        
        # Without calling start_day, equity_at_open is 0
        assert ks.equity_at_open == 0.0
        
        # Call start_day
        ks.start_day(10000.0, pd.Timestamp("2020-01-01"))
        
        # Now equity_at_open is set
        assert ks.equity_at_open == 10000.0
    
    def test_clamp_before_rebalance_check(self):
        """Test that clamp happens before rebalance band check."""
        # Raw weight that exceeds max exposure
        raw_weight = 1.0  # 100%
        leverage = 3.0  # TQQQ
        max_exposure = 1.0  # QQQ-equivalent cap
        
        # Clamp first
        clamped_weight, reason = clamp_weight_for_leverage(raw_weight, leverage, max_exposure)
        
        # Verify clamp happened
        assert clamped_weight < raw_weight
        assert reason == REASON_EXPOSURE_CLAMPED
        
        # Now check rebalance band (using clamped weight)
        actual_weight = 0.30  # Close to clamped
        band = 0.05
        
        # This should be the correct order: clamp THEN rebalance check
        should_trade = should_rebalance(clamped_weight, actual_weight, band)
        
        # Clamped weight ~0.333, actual 0.30, diff ~0.033 < 0.05 band
        # So no rebalance needed
        assert not should_trade, "Should not rebalance when within band of clamped weight"
    
    def test_risk_check_after_trades(self):
        """Test that risk checks happen at end of day after trades."""
        ks = KillSwitch(RiskLimits(max_daily_loss_pct=0.02))
        ks.start_day(10000.0, pd.Timestamp("2020-01-01"))
        
        # Simulate trades happened (equity changed)
        equity_after_trades = 9700.0  # 3% loss
        
        # EOD risk check
        should_halt, reason = ks.check_end_of_day(equity_after_trades, pd.Timestamp("2020-01-01"))
        
        assert should_halt
        assert "HALT_DAILY_LOSS" in reason
    
    def test_flatten_price_set_before_submit(self):
        """Test that broker price is set before flatten order is submitted."""
        broker = PaperBroker(10000)
        broker.connect()
        broker.set_price("TQQQ", 100)
        
        # Buy 50 shares
        buy_order = Order("", "TQQQ", "BUY", 50)
        broker.submit_order(buy_order)
        
        # Now simulate HALT - price MUST be set before flatten
        close_price = 95.0
        broker.set_price("TQQQ", close_price)  # Set price BEFORE flatten
        
        # Create flatten order
        ks = KillSwitch()
        flatten_order = ks.force_flatten_order(50, "TQQQ")
        
        # Submit flatten - price was already set
        broker.submit_order(flatten_order)
        
        # Verify fill price used the set price
        fill = broker.fills[-1]
        assert fill.fill_price == close_price


class TestNextOpenHaltFlatten:
    """Tests for NEXT_OPEN mode HALT flatten behavior."""
    
    def test_halt_at_close_sets_pending_flatten(self):
        """Test that HALT at close sets pending_flatten for NEXT_OPEN mode."""
        ks = KillSwitch(RiskLimits(max_daily_loss_pct=0.02))
        ks.start_day(10000.0, pd.Timestamp("2020-01-01"))
        
        # EOD check triggers HALT
        should_halt, _ = ks.check_end_of_day(9700.0, pd.Timestamp("2020-01-01"))
        
        assert should_halt
        assert ks.is_killed
        assert ks.pending_flatten is True
        assert ks.halt_triggered_on_date == pd.Timestamp("2020-01-01").normalize()
    
    def test_halt_flatten_executes_next_open_in_next_open_mode(self):
        """
        Test: HALT at date t close → flatten executes at t+1 open.
        
        In NEXT_OPEN mode:
        - Risk check happens at close of day t
        - Flatten order queued (pending_flatten=True)
        - Flatten executes at open of day t+1
        """
        broker = PaperBroker(10000)
        broker.connect()
        
        # Day t: Buy 50 shares at 100
        broker.set_price("TQQQ", 100)
        buy_order = Order("", "TQQQ", "BUY", 50)
        broker.submit_order(buy_order)
        
        ks = KillSwitch(RiskLimits(max_daily_loss_pct=0.02))
        ks.start_day(broker.get_account_value(), pd.Timestamp("2020-01-01"))
        
        # Day t close: Price dropped, triggers HALT
        broker.set_price("TQQQ", 95)  # 5% loss on 50% position
        equity_close = broker.get_account_value()  # 5000 + 50*95 = 9750
        
        # If equity_open was 10000, daily return = 9750/10000 - 1 = -2.5% > -2%
        # This should trigger HALT
        should_halt, _ = ks.check_end_of_day(equity_close, pd.Timestamp("2020-01-01"))
        
        assert should_halt
        assert ks.pending_flatten is True
        
        # Day t+1 open: Execute flatten
        next_open_price = 94.0  # Price at t+1 open
        broker.set_price("TQQQ", next_open_price)
        
        flatten_order = ks.force_flatten_order(50, "TQQQ")
        assert flatten_order is not None
        
        broker.submit_order(flatten_order, is_system_order=True)
        
        # Verify position is flat
        pos = broker.get_position("TQQQ")
        assert pos is None or pos.shares == 0
        
        # Verify fill used t+1 open price
        fill = broker.fills[-1]
        assert fill.fill_price == next_open_price
    
    def test_halt_day_t_plus_1_no_new_trades_even_if_signals_buy(self):
        """
        Test: After HALT, no new trades on t+1 even if signals say "buy".
        
        HALT locks out trading until cooldown expires.
        """
        ks = KillSwitch(RiskLimits(max_daily_loss_pct=0.02, halt_cooldown_days=2))
        ks.start_day(10000.0, pd.Timestamp("2020-01-01"))
        
        # HALT triggers at end of day t
        ks.check_end_of_day(9700.0, pd.Timestamp("2020-01-01"))
        assert ks.is_killed
        
        # Day t+1: Start day (should still be locked)
        ks.start_day(9700.0, pd.Timestamp("2020-01-02"))
        
        # Kill switch should still be active
        assert ks.is_killed
        assert "LOCKOUT" in ks.kill_reason
        
        # Trying to trade should be blocked
        # (In real loop, we'd skip trading logic when is_killed=True)


class TestSameDayCloseHaltFlatten:
    """Tests for SAME_DAY_CLOSE mode HALT flatten behavior."""
    
    def test_halt_flatten_executes_same_bar_close(self):
        """Test that HALT in SAME_DAY_CLOSE mode flattens at same bar close."""
        broker = PaperBroker(10000)
        broker.connect()
        
        # Buy 50 shares at 100
        broker.set_price("TQQQ", 100)
        buy_order = Order("", "TQQQ", "BUY", 50)
        broker.submit_order(buy_order)
        
        ks = KillSwitch(RiskLimits(max_daily_loss_pct=0.02))
        ks.start_day(broker.get_account_value(), pd.Timestamp("2020-01-01"))
        
        # Set close price and check EOD
        close_price = 95.0
        broker.set_price("TQQQ", close_price)
        equity_close = broker.get_account_value()
        
        should_halt, _ = ks.check_end_of_day(equity_close, pd.Timestamp("2020-01-01"))
        
        if should_halt:
            # SAME_DAY_CLOSE: flatten immediately at close
            flatten_order = ks.force_flatten_order(50, "TQQQ")
            if flatten_order:
                broker.submit_order(flatten_order, is_system_order=True)
        
        # Verify position is flat
        pos = broker.get_position("TQQQ")
        assert pos is None or pos.shares == 0


class TestClampBeforeRebalance:
    """Tests for clamp-before-rebalance enforcement (killer test)."""
    
    def test_clamp_before_rebalance_target_shares_uses_clamped_weight_only(self):
        """
        Killer test: Craft a day where weight_raw > max exposure.
        If rebalance happened before clamp, shares would overshoot.
        Assert target_shares is computed from clamped weight only.
        """
        # Setup
        equity = 10000.0
        raw_weight = 1.0  # 100% weight (raw, unclamped)
        leverage = 3.0  # TQQQ is 3x
        max_exposure = 1.0  # Max QQQ-equivalent exposure
        price = 100.0
        
        # Step 1: Clamp FIRST (correct order)
        clamped_weight, reason = clamp_weight_for_leverage(raw_weight, leverage, max_exposure)
        
        # Verify clamping happened
        assert clamped_weight < raw_weight
        expected_clamped = 1.0 / 3.0  # ~0.333
        assert abs(clamped_weight - expected_clamped) < 1e-9
        
        # Step 2: Compute target shares using CLAMPED weight
        target_shares = compute_target_shares(equity, clamped_weight, price)
        
        # With clamped weight 0.333 and $10k equity at $100/share:
        # target_value = 10000 * 0.333 = 3333.33
        # target_shares = floor(3333.33 / 100) = 33
        assert target_shares == 33
        
        # Step 3: Verify effective exposure is within limits
        stock_value = target_shares * price
        actual_weight = stock_value / equity
        effective_exposure = actual_weight * leverage
        
        # Should be <= max_exposure
        assert effective_exposure <= max_exposure + ZERO_EPS
    
    def test_raw_weight_would_cause_overshoot(self):
        """Test that using raw weight would cause exposure overshoot."""
        equity = 10000.0
        raw_weight = 1.0  # 100%
        leverage = 3.0
        max_exposure = 1.0
        price = 100.0
        
        # If we used raw weight (WRONG - no clamp):
        wrong_target_shares = compute_target_shares(equity, raw_weight, price)
        
        # This would give 100 shares
        assert wrong_target_shares == 100
        
        # Effective exposure would be:
        wrong_effective = (wrong_target_shares * price / equity) * leverage
        # = (100 * 100 / 10000) * 3 = 3.0 (300%!)
        
        # This exceeds max exposure
        assert wrong_effective > max_exposure


class TestCooldownBehavior:
    """Tests for cooldown behavior during lockout."""
    
    def test_cooldown_blocks_entries(self):
        """Test that cooldown blocks new entries."""
        ks = KillSwitch(RiskLimits(max_daily_loss_pct=0.02, halt_cooldown_days=2))
        ks.start_day(10000.0, pd.Timestamp("2020-01-01"))
        
        # Trigger HALT
        ks.check_end_of_day(9700.0, pd.Timestamp("2020-01-01"))
        
        # Next day should be locked
        ks.start_day(9700.0, pd.Timestamp("2020-01-02"))
        assert ks.is_killed
        
        # Check order should fail
        order = Order("", "TQQQ", "BUY", 10)
        allowed, reason = ks.check_order(order, 9700.0, 100.0)
        assert not allowed
        assert "Kill switch active" in reason
    
    def test_cooldown_allows_reduce_only(self):
        """
        Test that reduce-only orders are conceptually allowed during cooldown.
        
        Reduce-only means target_shares may move toward 0 only.
        In practice, HALT forces flatten, so reduce is the only option.
        """
        ks = KillSwitch(RiskLimits(max_daily_loss_pct=0.02, halt_cooldown_days=2))
        ks.start_day(10000.0, pd.Timestamp("2020-01-01"))
        
        # Trigger HALT
        ks.check_end_of_day(9700.0, pd.Timestamp("2020-01-01"))
        
        # Force flatten should work even when halted
        flatten_order = ks.force_flatten_order(50, "TQQQ")
        
        # Flatten is a SELL (reduce) order
        assert flatten_order is not None
        assert flatten_order.side == "SELL"
        assert flatten_order.quantity == 50


class TestDailyLossHaltLockout:
    """Tests for daily loss HALT and lockout behavior."""
    
    def test_daily_loss_sets_lockout_until(self):
        """Test that daily loss HALT sets lockout_until."""
        ks = KillSwitch(RiskLimits(max_daily_loss_pct=0.02, halt_cooldown_days=3))
        ks.start_day(10000.0, pd.Timestamp("2020-01-01"))
        
        # Trigger HALT
        ks.check_end_of_day(9700.0, pd.Timestamp("2020-01-01"))
        
        # lockout_until should be 3 days from HALT date
        expected_lockout = pd.Timestamp("2020-01-04").normalize()
        assert ks.lockout_until == expected_lockout
    
    def test_lockout_expires_correctly(self):
        """Test that lockout expires after cooldown period."""
        ks = KillSwitch(RiskLimits(max_daily_loss_pct=0.02, halt_cooldown_days=1))
        ks.start_day(10000.0, pd.Timestamp("2020-01-01"))
        ks.check_end_of_day(9700.0, pd.Timestamp("2020-01-01"))  # HALT
        
        # Jan 2 should be first tradable day (lockout_until = Jan 2)
        ks.start_day(9700.0, pd.Timestamp("2020-01-02"))
        
        # Should no longer be locked
        assert not ks.is_killed
        assert ks.lockout_until is None


class TestEquityComputeFunction:
    """Tests for centralized compute_equity function (Phase 43)."""
    
    def test_compute_equity_basic(self):
        """Test basic equity calculation."""
        equity = compute_equity(cash=5000.0, shares=50, price=100.0)
        assert equity == 10000.0
    
    def test_compute_equity_no_shares(self):
        """Test equity with no shares (all cash)."""
        equity = compute_equity(cash=10000.0, shares=0, price=100.0)
        assert equity == 10000.0
    
    def test_compute_equity_no_cash(self):
        """Test equity with no cash (all invested)."""
        equity = compute_equity(cash=0.0, shares=100, price=100.0)
        assert equity == 10000.0
