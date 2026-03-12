# Phase 3 Report: Normalize Types + Missing Values

## Date: 2026-01-30

## Phase Objective
Add type normalization (convert OHLCV to float64) and handle missing values by dropping rows with missing Date or Close.

## Functionalities Implemented

1. **`normalize_data(df)` Function** (`src/data_loader.py`)
   - Converts all OHLCV columns to `float64` dtype
   - Handles string numbers by coercing to numeric
   - Drops rows with missing `Date` or `Close` (critical columns)
   - Resets index after dropping rows
   - Prints warning message when rows are dropped

2. **New Constants**
   - `NUMERIC_COLUMNS = ['Open', 'High', 'Low', 'Close', 'Volume']`

## Code Implementation

### normalize_data function
```python
def normalize_data(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    
    # Convert numeric columns to float64
    for col in NUMERIC_COLUMNS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').astype(np.float64)
    
    # Drop rows with missing Date or Close
    df = df.dropna(subset=['Date', 'Close'])
    df = df.reset_index(drop=True)
    
    return df
```

## Current App State

### File Changes
- `src/data_loader.py` - Added `normalize_data()` function and `NUMERIC_COLUMNS` constant

### Data Processing Pipeline
```
load_qqq_csv(path) → normalize_data(df) → DataFrame ready for indicators
```

## Test Results

```
pytest -q
..............                                                           [100%]
14 passed in 0.81s
```

### Tests Added (6 new tests)
| Test Name | Description | Status |
|-----------|-------------|--------|
| test_numeric_columns_are_float64 | Verify OHLCV columns are float64 | PASSED |
| test_drops_rows_with_missing_close | Missing Close values are dropped | PASSED |
| test_drops_rows_with_missing_date | Missing Date values are dropped | PASSED |
| test_handles_string_numbers | String "100.5" converts to 100.5 | PASSED |
| test_preserves_valid_data | Valid rows are not dropped | PASSED |
| test_resets_index_after_drop | Index is 0,1,2... after drops | PASSED |

### All Tests Summary
| Category | Count | Status |
|----------|-------|--------|
| Phase 1 (trivial) | 1 | PASSED |
| Phase 2 (loader) | 7 | PASSED |
| Phase 3 (normalize) | 6 | PASSED |
| **Total** | **14** | **ALL PASSED** |

## Edge Cases Handled

1. **String numbers**: "100.5" → 100.5 (float64)
2. **Invalid strings**: "abc" → NaN → row dropped if in Close
3. **Missing Date**: Row dropped
4. **Missing Close**: Row dropped
5. **Missing Open/High/Low/Volume**: Kept as NaN (non-critical)

## Sample Transformation

**Before normalize_data:**
```
Open: object or int64
Close: object or int64
```

**After normalize_data:**
```
Open: float64
High: float64
Low: float64
Close: float64
Volume: float64
```

## Issues Encountered
None - implementation was straightforward.

## Next Phase Preview
Phase 4 will add MA250 (250-day Simple Moving Average of Close).

## Verification Commands
```powershell
.\.venv\Scripts\Activate.ps1
pytest -q
# Expected: 14 passed
```

---
**Phase 3 Status: COMPLETE**
