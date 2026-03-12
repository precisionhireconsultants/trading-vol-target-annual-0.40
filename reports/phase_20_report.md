# Phase 20 Report: Actual_Weight

## Date: 2026-01-30

## Phase Objective
Compute Actual_Weight of stock holdings.

## Functionalities Implemented

1. **`compute_actual_weight(shares, close_price, portfolio_value)` Function**
   - Formula: `(Total_Stocks_Owned * Close) / Remaining_Portfolio_Amount`
   - Handles divide-by-zero (returns 0.0)

## Code Implementation

```python
def compute_actual_weight(shares: int, close_price: float, portfolio_value: float) -> float:
    if portfolio_value <= 0:
        return 0.0
    stock_value = shares * close_price
    return stock_value / portfolio_value
```

## Test Results

```
pytest -q
117 passed in 1.17s
```

### Tests Added (6 new tests)
| Test Name | Description | Status |
|-----------|-------------|--------|
| test_fully_invested | Weight = 1.0 | PASSED |
| test_all_cash | Weight = 0.0 | PASSED |
| test_half_invested | Weight = 0.5 | PASSED |
| test_divide_by_zero_protection | Zero portfolio | PASSED |
| test_weight_between_zero_and_one | Valid range | PASSED |
| test_realistic_scenario | QQQ example | PASSED |

**Total Tests: 117 passed**

## Weight Examples

| Shares | Close | Portfolio Value | Actual Weight |
|--------|-------|-----------------|---------------|
| 100 | $100 | $10,000 | 1.0 |
| 0 | $100 | $10,000 | 0.0 |
| 50 | $100 | $10,000 | 0.5 |
| 23 | $420 | $10,000 | 0.966 |

---
**Phase 20 Status: COMPLETE**
