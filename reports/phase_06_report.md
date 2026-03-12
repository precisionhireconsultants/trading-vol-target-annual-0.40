# Phase 6 Report: Add QQQ_ann_vol

## Date: 2026-01-30

## Phase Objective
Add QQQ_ann_vol (annualized volatility) indicator based on Close price returns.

## Functionalities Implemented

1. **`add_annualized_volatility(df, window)` Function** (`src/indicators.py`)
   - Calculates daily returns: `pct_change(Close)`
   - Computes rolling standard deviation: `rolling_std(returns, window)`
   - Annualizes: `daily_vol * sqrt(252)`
   - Default window: 20 days

2. **New Constants**
   - `VOL_WINDOW = 20` - Default volatility window
   - `TRADING_DAYS_PER_YEAR = 252` - Annualization factor

## Code Implementation

### src/indicators.py (addition)
```python
VOL_WINDOW = 20
TRADING_DAYS_PER_YEAR = 252

def add_annualized_volatility(df: pd.DataFrame, window: int = VOL_WINDOW) -> pd.DataFrame:
    df = df.copy()
    
    # Calculate daily returns
    returns = df['Close'].pct_change()
    
    # Calculate rolling standard deviation of returns
    daily_vol = returns.rolling(window=window, min_periods=window).std()
    
    # Annualize: multiply by sqrt(trading days per year)
    df['QQQ_ann_vol'] = daily_vol * np.sqrt(TRADING_DAYS_PER_YEAR)
    
    return df
```

## Current App State

### Available Indicators
| Indicator | Function | Default Window | Description |
|-----------|----------|----------------|-------------|
| MA250 | add_ma250() | 250 days | Long-term trend |
| MA50 | add_ma50() | 50 days | Short-term trend |
| QQQ_ann_vol | add_annualized_volatility() | 20 days | Annualized volatility |

### Data Processing Pipeline
```
load_qqq_csv(path) 
  → normalize_data(df) 
  → add_ma250(df) 
  → add_ma50(df) 
  → add_annualized_volatility(df)
  → DataFrame with all indicators
```

## Test Results

```
pytest -q
...............................                                          [100%]
31 passed in 0.94s
```

### Tests Added (6 new tests)
| Test Name | Description | Status |
|-----------|-------------|--------|
| test_adds_qqq_ann_vol_column | Column is created | PASSED |
| test_nan_before_window | First N rows are NaN | PASSED |
| test_non_negative_values | Volatility >= 0 | PASSED |
| test_annualization_factor | Proper annualization | PASSED |
| test_zero_volatility_for_flat_prices | Flat = 0 vol | PASSED |
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
| **Total** | **31** | **ALL PASSED** |

## Volatility Calculation Example

For Close prices with 1% daily standard deviation:

```
Daily returns std: 0.01 (1%)
Annualized: 0.01 * sqrt(252) = 0.159 (15.9%)
```

### NaN Behavior
- Row 0: NaN (no prior price for pct_change)
- Rows 1 to window-1: NaN (not enough data for rolling std)
- Row window onwards: Valid volatility values

## Edge Cases Handled

1. **Flat prices**: Returns volatility of 0
2. **Negative returns**: Handled correctly (squared in std)
3. **Large price swings**: Proportional volatility increase

## Issues Encountered
None - implementation was straightforward.

## Next Phase Preview
Phase 7 will implement Base_Regime based on MA250 comparison.

## Verification Commands
```powershell
.\.venv\Scripts\Activate.ps1
pytest -q
# Expected: 31 passed
```

---
**Phase 6 Status: COMPLETE**
