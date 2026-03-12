# Phase 10 Report: Target_Weight

## Date: 2026-01-30

## Phase Objective
Implement Target_Weight based on Final_Trading_Regime.

## Functionalities Implemented

1. **`add_target_weight(df)` Function** (`src/regime.py`)
   - Converts regime to numeric weight for position sizing
   - Rules:
     - Final_Trading_Regime == "bull" → 1.0 (100% invested)
     - Final_Trading_Regime == "cash" → 0.0 (0% invested)

## Code Implementation

### src/regime.py (addition)
```python
def add_target_weight(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add Target_Weight column based on Final_Trading_Regime.
    
    Rules:
        - If Final_Trading_Regime == "bull" → 1.0 (fully invested)
        - Else → 0.0 (fully in cash)
    """
    df = df.copy()
    df['Target_Weight'] = df['Final_Trading_Regime'].apply(
        lambda x: 1.0 if x == "bull" else 0.0
    )
    return df
```

## Current App State

### Complete Signal Generation Flow
```
Close, MA250
    │
    ▼
Base_Regime (cash/bull/bear)
    │
    ▼
Confirmed_Regime (cash/bull/bear)
    │
    ▼
Final_Trading_Regime (cash/bull)
    │
    ▼
Target_Weight (0.0/1.0)
```

### Weight Mapping
| Final_Trading_Regime | Target_Weight |
|----------------------|---------------|
| bull | 1.0 |
| cash | 0.0 |

## Test Results

```
pytest -q
.........................................................                [100%]
57 passed in 1.00s
```

### Tests Added (7 new tests)
| Test Name | Description | Status |
|-----------|-------------|--------|
| test_adds_target_weight_column | Column is created | PASSED |
| test_bull_gives_weight_one | bull → 1.0 | PASSED |
| test_cash_gives_weight_zero | cash → 0.0 | PASSED |
| test_mixed_weights | Mixed correct | PASSED |
| test_weight_is_float | Float dtype | PASSED |
| test_only_zero_or_one | Only 0.0 or 1.0 | PASSED |
| test_does_not_modify_original | Original unchanged | PASSED |

### All Tests Summary
| Category | Count | Status |
|----------|-------|--------|
| Phase 1 (trivial) | 1 | PASSED |
| Phase 2 (loader) | 7 | PASSED |
| Phase 3 (normalize) | 6 | PASSED |
| Phase 4 (MA250) | 6 | PASSED |
| Phase 5 (MA50) | 5 | PASSED |
| Phase 6 (volatility) | 6 | PASSED |
| Phase 7 (Base_Regime) | 8 | PASSED |
| Phase 8 (Confirmed_Regime) | 4 | PASSED |
| Phase 9 (Final_Trading_Regime) | 7 | PASSED |
| Phase 10 (Target_Weight) | 7 | PASSED |
| **Total** | **57** | **ALL PASSED** |

## Trading Logic Explanation

### Binary Position Sizing
This simple strategy uses binary weights:
- **1.0**: Fully invested in QQQ
- **0.0**: Fully in cash

Future enhancements could include:
- Fractional weights based on volatility
- Gradual position building/reduction

## Issues Encountered
None - implementation was straightforward.

## Next Phase Preview
Phase 11 will implement sample fold selection for the single-fold backtest.

## Verification Commands
```powershell
.\.venv\Scripts\Activate.ps1
pytest -q
# Expected: 57 passed
```

---
**Phase 10 Status: COMPLETE**
