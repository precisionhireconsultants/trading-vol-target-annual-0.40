# Phase 7 Report: Base_Regime (MA250 only)

## Date: 2026-01-30

## Phase Objective
Implement Base_Regime based on MA250 comparison with Close price.

## Functionalities Implemented

1. **`add_base_regime(df)` Function** (`src/regime.py`)
   - Determines market regime based on MA250
   - Three possible regimes: "cash", "bull", "bear"
   - Decision logic:
     - MA250 is NaN → "cash"
     - Close >= MA250 → "bull"
     - Close < MA250 → "bear"

## Code Implementation

### src/regime.py
```python
def add_base_regime(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    
    def determine_regime(row):
        if pd.isna(row['MA250']):
            return "cash"
        elif row['Close'] >= row['MA250']:
            return "bull"
        else:
            return "bear"
    
    df['Base_Regime'] = df.apply(determine_regime, axis=1)
    
    return df
```

## Current App State

### New File
- `src/regime.py` - Regime detection logic
- `tests/test_regime.py` - Regime tests

### Regime Decision Flow
```
                    ┌─────────────────┐
                    │  Is MA250 NaN?  │
                    └────────┬────────┘
                             │
              ┌──────────────┴──────────────┐
              │ Yes                         │ No
              ▼                             ▼
         ┌────────┐              ┌──────────────────┐
         │ "cash" │              │ Close >= MA250?  │
         └────────┘              └────────┬─────────┘
                                          │
                           ┌──────────────┴──────────────┐
                           │ Yes                         │ No
                           ▼                             ▼
                      ┌────────┐                    ┌────────┐
                      │ "bull" │                    │ "bear" │
                      └────────┘                    └────────┘
```

## Test Results

```
pytest -q
.......................................                                  [100%]
39 passed in 1.36s
```

### Tests Added (8 new tests)
| Test Name | Description | Status |
|-----------|-------------|--------|
| test_adds_base_regime_column | Column is created | PASSED |
| test_cash_when_ma250_is_nan | NaN MA250 → cash | PASSED |
| test_bull_when_close_above_ma250 | Close >= MA250 → bull | PASSED |
| test_bear_when_close_below_ma250 | Close < MA250 → bear | PASSED |
| test_mixed_regimes | Correct mixed assignment | PASSED |
| test_does_not_modify_original | Original unchanged | PASSED |
| test_regime_values_are_strings | Values are strings | PASSED |
| test_only_valid_regime_values | Only cash/bull/bear | PASSED |

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
| **Total** | **39** | **ALL PASSED** |

## Regime Assignment Example

| Date | Close | MA250 | Base_Regime |
|------|-------|-------|-------------|
| 2024-01-01 | 100 | NaN | cash |
| 2024-01-02 | 105 | NaN | cash |
| ... | ... | ... | ... |
| 2024-12-01 | 420 | 400 | bull |
| 2024-12-02 | 395 | 400 | bear |
| 2024-12-03 | 400 | 400 | bull (equal) |

## Key Design Decisions

1. **Equal case (Close == MA250)**: Treated as "bull"
   - Rationale: Stay invested unless clearly below MA

2. **NaN handling**: Explicit "cash" regime
   - Rationale: Don't trade without sufficient data for trend analysis

## Issues Encountered
None - implementation was straightforward.

## Next Phase Preview
Phase 8 will add Confirmed_Regime (placeholder = Base_Regime).

## Verification Commands
```powershell
.\.venv\Scripts\Activate.ps1
pytest -q
# Expected: 39 passed
```

---
**Phase 7 Status: COMPLETE**
