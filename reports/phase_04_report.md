# Phase 4 Report: Add MA250 Indicator

## Date: 2026-01-30

## Phase Objective
Add MA250 (250-day Simple Moving Average) indicator based on Close price.

## Functionalities Implemented

1. **`add_ma250(df, window)` Function** (`src/indicators.py`)
   - Calculates Simple Moving Average of Close price
   - Default window: 250 days (configurable for testing)
   - Returns NaN for rows before window is filled
   - Does not modify original DataFrame

2. **Constants Defined**
   - `MA_LONG = 250` - Default long MA window
   - `MA_SHORT = 50` - Default short MA window (for Phase 5)

## Code Implementation

### src/indicators.py
```python
MA_LONG = 250
MA_SHORT = 50

def add_ma250(df: pd.DataFrame, window: int = MA_LONG) -> pd.DataFrame:
    df = df.copy()
    df['MA250'] = df['Close'].rolling(window=window, min_periods=window).mean()
    return df
```

## Current App State

### File Structure
```
trading/
├── src/
│   ├── __init__.py
│   ├── data_loader.py
│   └── indicators.py       # NEW
├── tests/
│   ├── fixtures/
│   │   └── sample_qqq.csv
│   ├── test_trivial.py
│   ├── test_data_loader.py
│   └── test_indicators.py  # NEW
└── ...
```

### Data Processing Pipeline
```
load_qqq_csv(path) → normalize_data(df) → add_ma250(df) → DataFrame with MA250
```

## Test Results

```
pytest -q
....................                                                     [100%]
20 passed in 0.69s
```

### Tests Added (6 new tests)
| Test Name | Description | Status |
|-----------|-------------|--------|
| test_adds_ma250_column | MA250 column is created | PASSED |
| test_nan_before_window | First N-1 rows are NaN | PASSED |
| test_correct_sma_calculation | SMA values are correct | PASSED |
| test_uses_default_window | Default window is 250 | PASSED |
| test_does_not_modify_original | Original df unchanged | PASSED |
| test_with_varying_prices | MA bounded by min/max | PASSED |

### All Tests Summary
| Category | Count | Status |
|----------|-------|--------|
| Phase 1 (trivial) | 1 | PASSED |
| Phase 2 (loader) | 7 | PASSED |
| Phase 3 (normalize) | 6 | PASSED |
| Phase 4 (MA250) | 6 | PASSED |
| **Total** | **20** | **ALL PASSED** |

## MA250 Calculation Example

For window=5 with Close prices [10, 20, 30, 40, 50, 60, 70, 80, 90, 100]:

| Index | Close | MA250 (window=5) |
|-------|-------|------------------|
| 0 | 10 | NaN |
| 1 | 20 | NaN |
| 2 | 30 | NaN |
| 3 | 40 | NaN |
| 4 | 50 | 30.0 (mean of 10-50) |
| 5 | 60 | 40.0 (mean of 20-60) |
| 6 | 70 | 50.0 (mean of 30-70) |
| 7 | 80 | 60.0 (mean of 40-80) |
| 8 | 90 | 70.0 (mean of 50-90) |
| 9 | 100 | 80.0 (mean of 60-100) |

## Test Strategy Note

Tests use small windows (5) for quick verification. Production uses MA_LONG=250.

## Issues Encountered
None - implementation was straightforward.

## Next Phase Preview
Phase 5 will add MA50 (50-day Simple Moving Average of Close).

## Verification Commands
```powershell
.\.venv\Scripts\Activate.ps1
pytest -q
# Expected: 20 passed
```

---
**Phase 4 Status: COMPLETE**
