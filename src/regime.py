"""Regime detection for trading strategy."""
import pandas as pd
import numpy as np


def add_base_regime(df: pd.DataFrame, price_col: str = 'Close') -> pd.DataFrame:
    """
    Add Base_Regime column based on MA250 comparison.
    
    Rules:
        - If MA250 is NaN → "cash"
        - If price >= MA250 → "bull"
        - Else → "bear"
    
    Args:
        df: DataFrame with ``price_col`` and 'MA250' columns
        price_col: Column to compare against MA250.  Default ``'Close'``.
            In intraday mode pass ``'Signal_Price'`` so that the regime
            decision uses the near-close signal price, not today's Close.
        
    Returns:
        DataFrame with new 'Base_Regime' column
    """
    df = df.copy()

    col = price_col

    def determine_regime(row):
        if pd.isna(row['MA250']):
            return "cash"
        price = row.get(col)
        if pd.isna(price):
            return "cash"
        if price >= row['MA250']:
            return "bull"
        return "bear"
    
    df['Base_Regime'] = df.apply(determine_regime, axis=1)
    
    return df


def add_confirmed_regime(df: pd.DataFrame, use_ma_confirmation: bool = None) -> pd.DataFrame:
    """
    Add Confirmed_Regime column with optional MA50 confirmation.
    
    When use_ma_confirmation is True (Malik's "don't take weak rallies"):
        - If Base_Regime == "bull" but MA50 <= MA250 -> "cash" (weak rally)
        - Else keep Base_Regime
    
    When use_ma_confirmation is False:
        - Confirmed_Regime = Base_Regime (no filtering)
    
    Args:
        df: DataFrame with 'Base_Regime', 'MA50', 'MA250' columns
        use_ma_confirmation: Override config flag (None uses DEFAULT_CONFIG)
        
    Returns:
        DataFrame with new 'Confirmed_Regime' column
    """
    from config import DEFAULT_CONFIG
    
    if use_ma_confirmation is None:
        use_ma_confirmation = DEFAULT_CONFIG.USE_MA_CONFIRMATION
    
    df = df.copy()
    
    if not use_ma_confirmation:
        # Current behavior: just copy Base_Regime
        df['Confirmed_Regime'] = df['Base_Regime']
    else:
        # MA50 confirmation logic
        def confirm_regime(row):
            if row['Base_Regime'] != "bull":
                return row['Base_Regime']
            # Bull regime - check MA50 vs MA250
            if pd.isna(row['MA50']) or pd.isna(row['MA250']):
                return "cash"  # No data = no confirmation
            if row['MA50'] <= row['MA250']:
                return "cash"  # Weak rally
            return "bull"  # Confirmed bull
        
        df['Confirmed_Regime'] = df.apply(confirm_regime, axis=1)
    
    return df


def add_final_trading_regime(df: pd.DataFrame, use_sqqq_in_bear: bool = None) -> pd.DataFrame:
    """
    Add Final_Trading_Regime column for trading strategy.
    
    Without SQQQ (default):
        - Converts bear to cash since we only go long QQQ (no shorting)
        - bull → "bull", bear/cash → "cash"
    
    With SQQQ enabled:
        - Allows bear regime to pass through for SQQQ trading
        - bull → "bull", bear → "bear", cash → "cash"
    
    Args:
        df: DataFrame with 'Confirmed_Regime' column
        use_sqqq_in_bear: Allow bear regime (None uses config)
        
    Returns:
        DataFrame with new 'Final_Trading_Regime' column
    """
    from config import DEFAULT_CONFIG
    
    if use_sqqq_in_bear is None:
        use_sqqq_in_bear = DEFAULT_CONFIG.USE_SQQQ_IN_BEAR
    
    df = df.copy()
    
    if use_sqqq_in_bear:
        # Allow bear regime through for SQQQ trading
        df['Final_Trading_Regime'] = df['Confirmed_Regime']
    else:
        # Default: convert bear to cash (long-only strategy)
        df['Final_Trading_Regime'] = df['Confirmed_Regime'].apply(
            lambda x: "bull" if x == "bull" else "cash"
        )
    
    return df


def add_target_weight(
    df: pd.DataFrame,
    use_vol_targeting: bool = None,
    vol_target: float = None,
    max_position: float = None
) -> pd.DataFrame:
    """
    Add Target_Weight column based on Final_Trading_Regime.
    
    Without vol targeting:
        - If Final_Trading_Regime == "bull" → 1.0 (fully invested long)
        - If Final_Trading_Regime == "bear" → -1.0 (fully invested short/SQQQ)
        - Else → 0.0 (fully in cash)
    
    With vol targeting (Malik's "how much" component):
        - On bull days: raw_weight = vol_target / max(QQQ_ann_vol, 0.01)
        - On bear days: raw_weight = -vol_target / max(QQQ_ann_vol, 0.01)
        - Target_Weight = clamp(raw_weight, -max_position, max_position)
        - On cash days: Target_Weight = 0.0
    
    Args:
        df: DataFrame with 'Final_Trading_Regime' and optionally 'QQQ_ann_vol'
        use_vol_targeting: Enable volatility targeting (None uses config)
        vol_target: Target annual volatility (None uses config)
        max_position: Maximum position size (None uses config)
        
    Returns:
        DataFrame with new 'Target_Weight' column
    """
    from config import DEFAULT_CONFIG
    
    if use_vol_targeting is None:
        use_vol_targeting = DEFAULT_CONFIG.USE_VOL_TARGETING
    if vol_target is None:
        vol_target = DEFAULT_CONFIG.VOL_TARGET_ANNUAL
    if max_position is None:
        max_position = DEFAULT_CONFIG.MAX_POSITION_PCT
    
    df = df.copy()
    
    if not use_vol_targeting:
        # Simple: 1.0 for bull, -1.0 for bear, 0.0 for cash
        def simple_weight(regime):
            if regime == "bull":
                return 1.0
            elif regime == "bear":
                return -1.0
            else:
                return 0.0
        
        df['Target_Weight'] = df['Final_Trading_Regime'].apply(simple_weight)
    else:
        # Volatility targeting
        def compute_vol_weight(row):
            regime = row['Final_Trading_Regime']
            if regime == "cash":
                return 0.0
            
            # Get current volatility
            ann_vol = row.get('QQQ_ann_vol', np.nan)
            if pd.isna(ann_vol) or ann_vol <= 0:
                # No vol data - use max position
                return max_position if regime == "bull" else -max_position
            
            # Compute target weight
            raw_weight = vol_target / ann_vol
            capped_weight = min(raw_weight, max_position)
            
            return capped_weight if regime == "bull" else -capped_weight
        
        df['Target_Weight'] = df.apply(compute_vol_weight, axis=1)
    
    return df
