# Phase 5 Report: Add MA50 Indicator

## Date: 2026-01-30

## Phase Objective
Add MA50 (50-day Simple Moving Average) indicator based on Close price.

## Functionalities Implemented

1. **`add_ma50(df, window)` Function** (`src/indicators.py`)
   - Calculates Simple Moving Average of Close price
   - Default window: 50 days (configurable for testing)
   - Returns NaN for rows before window is filled
   - Does not modify original DataFrame

## Code Implementation

### src/indicators.py (addition)
```python
def add_ma50(df: pd.DataFrame, window: int = MA_SHORT) -> pd.DataFrame:
    df = df.copy()
    df['MA50'] = df['Close'].rolling(window=window, min_periods=window).mean()
    return df
```

## Current App State

### Available Indicators
| Indicator | Function | Default Window |
|-----------|----------|----------------|
| MA250 | add_ma250() | 250 days |
| MA50 | add_ma50() | 50 days |

### Data Processing Pipeline
```
load_qqq_csv(path) → normalize_data(df) → add_ma250(df) → add_ma50(df) → DataFrame
```

## Test Results

```
pytest -q
.........................                                                [100%]
25 passed in 0.95s
```

### Tests Added (5 new tests)
| Test Name | Description | Status |
|-----------|-------------|--------|
| test_adds_ma50_column | MA50 column is created | PASSED |
| test_nan_before_window | First N-1 rows are NaN | PASSED |
| test_correct_sma_calculation | SMA values are correct | PASSED |
| test_uses_default_window | Default window is 50 | PASSED |
| test_does_not_modify_original | Original df unchanged | PASSED |

### All Tests Summary
| Category | Count | Status |
|----------|-------|--------|
| Phase 1 (trivial) | 1 | PASSED |
| Phase 2 (loader) | 7 | PASSED |
| Phase 3 (normalize) | 6 | PASSED |
| Phase 4 (MA250) | 6 | PASSED |
| Phase 5 (MA50) | 5 | PASSED |
| **Total** | **25** | **ALL PASSED** |

## MA50 Calculation Example

For window=3 with Close prices [10, 20, 30, 40, 50, 60]:

| Index | Close | MA50 (window=3) |
|-------|-------|-----------------|
| 0 | 10 | NaN |
| 1 | 20 | NaN |
| 2 | 30 | 20.0 (mean of 10,20,30) |
| 3 | 40 | 30.0 (mean of 20,30,40) |
| 4 | 50 | 40.0 (mean of 30,40,50) |
| 5 | 60 | 50.0 (mean of 40,50,60) |

## Issues Encountered
None - implementation followed same pattern as MA250.

## Next Phase Preview
Phase 6 will add QQQ_ann_vol (annualized volatility).

## Verification Commands
```powershell
.\.venv\Scripts\Activate.ps1
pytest -q
# Expected: 25 passed
```

---
**Phase 5 Status: COMPLETE**
