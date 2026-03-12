# Phase 15 Report: Target_Shares Computation

## Date: 2026-01-30

## Phase Objective
Compute target shares based on portfolio value and target weight.

## Functionalities Implemented

1. **`compute_target_shares(portfolio_value_open, exec_target_weight, open_price)` Function**
   - Formula: `target_shares = floor((portfolio_value_open * exec_target_weight) / open_price)`
   - Returns integer shares (floored)
   - Handles edge cases (zero price, negative weight)

## Code Implementation

```python
def compute_target_shares(
    portfolio_value_open: float,
    exec_target_weight: float,
    open_price: float
) -> int:
    if open_price <= 0:
        return 0
    
    target_value = portfolio_value_open * exec_target_weight
    target_shares = int(np.floor(target_value / open_price))
    
    return max(0, target_shares)
```

## Example Calculations

| Portfolio Value | Weight | Open Price | Target Shares |
|-----------------|--------|------------|---------------|
| $10,000 | 1.0 | $100 | 100 |
| $10,000 | 0.5 | $100 | 50 |
| $10,000 | 1.0 | $420 | 23 |
| $10,000 | 0.0 | $100 | 0 |
| $10,000 | 1.0 | $33 | 303 |

## Test Results

```
pytest -q
89 passed in 1.20s
```

### Tests Added (7 new tests)
| Test Name | Description | Status |
|-----------|-------------|--------|
| test_full_weight_basic | 100% weight | PASSED |
| test_zero_weight | 0% weight | PASSED |
| test_floors_to_integer | Fractional shares | PASSED |
| test_partial_weight | 50% weight | PASSED |
| test_never_negative | No negative shares | PASSED |
| test_zero_price_returns_zero | Price = 0 | PASSED |
| test_realistic_scenario | QQQ example | PASSED |

**Total Tests: 89 passed**

## Design Decisions

1. **Floor vs Round**: Using floor to be conservative (never overspend)
2. **Integer shares**: No fractional share support
3. **Zero price guard**: Returns 0 to avoid division by zero

---
**Phase 15 Status: COMPLETE**
