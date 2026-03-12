"""Tests for paper_trade_alpaca rebalance execution."""

from scripts import paper_trade_alpaca as pta


def test_rebalance_trims_target_with_sell_when_over_target_dry_run():
    positions = {"TQQQ": {"qty": 1970}}
    results = pta.rebalance_to_target(
        trading_client=object(),
        target_symbol="TQQQ",
        target_shares=1882,
        current_positions=positions,
        trade_date="2026-03-10",
        timeout_min=3,
        dry_run=True,
    )

    assert len(results) == 1
    assert results[0]["_leg"] == "open"
    assert results[0]["side"] == "SELL"
    assert results[0]["qty"] == 88
    assert results[0]["status"] == "DRY_RUN"


def test_rebalance_buys_target_when_under_target_dry_run():
    positions = {"TQQQ": {"qty": 1500}}
    results = pta.rebalance_to_target(
        trading_client=object(),
        target_symbol="TQQQ",
        target_shares=1530,
        current_positions=positions,
        trade_date="2026-03-10",
        timeout_min=3,
        dry_run=True,
    )

    assert len(results) == 1
    assert results[0]["_leg"] == "open"
    assert results[0]["side"] == "BUY"
    assert results[0]["qty"] == 30
    assert results[0]["status"] == "DRY_RUN"


def test_rebalance_exact_target_submits_no_target_leg():
    positions = {"TQQQ": {"qty": 1882}}
    results = pta.rebalance_to_target(
        trading_client=object(),
        target_symbol="TQQQ",
        target_shares=1882,
        current_positions=positions,
        trade_date="2026-03-10",
        timeout_min=3,
        dry_run=True,
    )

    assert results == []


def test_rebalance_flattens_opposite_symbol_then_trims_target():
    positions = {"TQQQ": {"qty": 120}, "SQQQ": {"qty": 25}}
    results = pta.rebalance_to_target(
        trading_client=object(),
        target_symbol="TQQQ",
        target_shares=100,
        current_positions=positions,
        trade_date="2026-03-10",
        timeout_min=3,
        dry_run=True,
    )

    assert len(results) == 2
    assert results[0]["_leg"] == "flatten"
    assert results[0]["symbol"] == "SQQQ"
    assert results[0]["side"] == "SELL"
    assert results[0]["qty"] == 25
    assert results[1]["_leg"] == "open"
    assert results[1]["symbol"] == "TQQQ"
    assert results[1]["side"] == "SELL"
    assert results[1]["qty"] == 20


def test_rebalance_buy_is_capped_by_buying_power_dry_run():
    positions = {"TQQQ": {"qty": 100}}
    results = pta.rebalance_to_target(
        trading_client=object(),
        target_symbol="TQQQ",
        target_shares=120,
        current_positions=positions,
        trade_date="2026-03-10",
        timeout_min=3,
        est_price=10.0,
        buying_power=50.0,
        dry_run=True,
    )

    assert len(results) == 1
    assert results[0]["side"] == "BUY"
    assert results[0]["qty"] == 5
