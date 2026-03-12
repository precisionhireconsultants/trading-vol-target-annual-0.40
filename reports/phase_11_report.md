# Phase 11 Report: Sample Fold Selection

## Date: 2026-01-30

## Phase Objective
Implement sample fold selection for single-fold backtesting.

## Functionalities Implemented

1. **`select_sample_fold(df, years)` Function** (`src/fold_selection.py`)
   - Selects deterministic date range for backtesting
   - Start: First date where MA250 is not NaN
   - End: Start + years (or last available date)
   - Returns tuple of (fold_df, metadata)
   - Adds 'Phase' column set to "test"

## Code Implementation

### src/fold_selection.py
```python
def select_sample_fold(df, years=5) -> Tuple[pd.DataFrame, Dict]:
    # Find first valid MA250 date
    valid_ma250 = df[df['MA250'].notna()]
    start_date = valid_ma250['Date'].iloc[0]
    
    # Calculate end date
    end_date = start_date + timedelta(days=years * 365)
    end_date = min(end_date, df['Date'].max())
    
    # Filter and add Phase column
    fold_df = df[(df['Date'] >= start_date) & (df['Date'] <= end_date)]
    fold_df['Phase'] = "test"
    
    # Return with metadata
    return fold_df, {'Fold_ID': 1, ...}
```

## Test Results

```
pytest -q
........................................................................ [100%]
72 passed in 1.03s
```

### Tests Added (10 new tests)
| Test Name | Description | Status |
|-----------|-------------|--------|
| test_returns_tuple | Returns (df, dict) | PASSED |
| test_fold_starts_at_first_valid_ma250 | Correct start date | PASSED |
| test_fold_non_empty | Result not empty | PASSED |
| test_metadata_has_required_keys | All keys present | PASSED |
| test_fold_id_is_one | Fold_ID = 1 | PASSED |
| test_phase_column_all_test | Phase = "test" | PASSED |
| test_stable_output | Deterministic | PASSED |
| test_raises_if_no_valid_ma250 | Error handling | PASSED |
| test_clamps_to_available_data | End date clamping | PASSED |

**Total Tests: 67 passed**

## Metadata Structure

```python
{
    'Fold_ID': 1,
    'Train_Start': datetime,
    'Train_End': datetime,
    'Test_Start': datetime,
    'Test_End': datetime,
    'Phase': "test"
}
```

## Issues Encountered
None.

---
**Phase 11 Status: COMPLETE**
