# Phase 2 Report: Data Loader (QQQ CSV)

## Date: 2026-01-30

## Phase Objective
Implement data loader to read QQQ CSV files, parse dates, and validate columns.

## Functionalities Implemented

1. **`load_qqq_csv(path)` Function** (`src/data_loader.py`)
   - Reads CSV file from given path
   - Validates required columns: Date, Open, High, Low, Close, Volume
   - Parses Date column as datetime
   - Sorts data by Date ascending
   - Returns pandas DataFrame
   - Raises `FileNotFoundError` if file doesn't exist
   - Raises `ValueError` if required columns are missing

2. **Test Fixture** (`tests/fixtures/sample_qqq.csv`)
   - 15 rows of sample QQQ data
   - Date range: 2024-01-02 to 2024-01-23
   - Realistic OHLCV values

## Code Implementation

### src/data_loader.py
```python
REQUIRED_COLUMNS = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume']

def load_qqq_csv(path: str | Path) -> pd.DataFrame:
    # Validates columns, parses Date, sorts ascending
    ...
```

## Current App State

### File Structure
```
trading/
├── src/
│   ├── __init__.py
│   └── data_loader.py        # NEW
├── tests/
│   ├── fixtures/
│   │   └── sample_qqq.csv    # NEW
│   ├── test_trivial.py
│   └── test_data_loader.py   # NEW
├── data/
│   └── qqq_us_d.csv
├── conftest.py
├── pytest.ini
├── requirements.txt
└── reports/
    ├── phase_01_report.md
    └── phase_02_report.md
```

## Test Results

```
pytest -q
........                                                                 [100%]
8 passed in 0.72s
```

### Tests Executed
| Test Name | File | Status |
|-----------|------|--------|
| test_trivial_passes | test_trivial.py | PASSED |
| test_load_returns_dataframe | test_data_loader.py | PASSED |
| test_load_has_required_columns | test_data_loader.py | PASSED |
| test_load_correct_row_count | test_data_loader.py | PASSED |
| test_date_parsed_as_datetime | test_data_loader.py | PASSED |
| test_sorted_by_date_ascending | test_data_loader.py | PASSED |
| test_file_not_found_raises | test_data_loader.py | PASSED |
| test_missing_columns_raises | test_data_loader.py | PASSED |

## Sample Data Loaded

From `tests/fixtures/sample_qqq.csv`:

| Date | Open | High | Low | Close | Volume |
|------|------|------|-----|-------|--------|
| 2024-01-02 | 400.00 | 405.00 | 398.00 | 403.50 | 10,000,000 |
| 2024-01-03 | 403.50 | 408.00 | 402.00 | 407.25 | 11,000,000 |
| 2024-01-04 | 407.25 | 410.00 | 405.00 | 406.00 | 9,500,000 |
| ... | ... | ... | ... | ... | ... |
| 2024-01-23 | 429.00 | 432.00 | 427.00 | 431.00 | 12,200,000 |

**Total rows:** 15

## Real Data Compatibility

The loader is compatible with the actual QQQ data at `data/qqq_us_d.csv`:
- Columns match: Date, Open, High, Low, Close, Volume
- Note: No "Adj Close" column in real data (using Close as discussed)

## Issues Encountered
None - implementation was straightforward.

## Next Phase Preview
Phase 3 will add type normalization (convert OHLCV to float64) and handle missing values.

## Verification Commands
```powershell
.\.venv\Scripts\Activate.ps1
pytest -q
# Expected: 8 passed
```

---
**Phase 2 Status: COMPLETE**
