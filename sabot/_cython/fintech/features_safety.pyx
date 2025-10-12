# cython: language_level=3, boundscheck=False, wraparound=False, cdivision=True, nonecheck=False
"""
Feature Engineering & Market Safety Kernels.

Lag/diff operators, rolling stats, circuit breakers, guards.
"""

import cython
from libc.math cimport sqrt, fabs, isnan, pow
from libc.stdint cimport int64_t
from libc.time cimport time_t, time
import numpy as np

from sabot import cyarrow as pa
import pyarrow.compute as pc


# ============================================================================
# Feature Engineering Kernels
# ============================================================================

def lag(batch, str column, int k=1):
    """
    Lag operator: x[t-k]
    
    Args:
        batch: RecordBatch
        column: Column to lag
        k: Number of periods to lag
    
    Returns:
        RecordBatch with lagged column
    """
    if batch is None or batch.num_rows == 0:
        return batch
    
    values_np = batch.column(column).to_numpy()
    n = len(values_np)
    
    # Create lagged array (pad with NaN)
    lagged = np.full(n, np.nan)
    if k < n:
        lagged[k:] = values_np[:-k] if k > 0 else values_np
    
    lag_name = f'{column}_lag{k}'
    return batch.append_column(lag_name, pa.array(lagged, type=pa.float64()))


def diff(batch, str column, int k=1):
    """
    Difference operator: x[t] - x[t-k]
    
    Args:
        batch: RecordBatch
        column: Column to difference
        k: Difference period
    
    Returns:
        RecordBatch with differenced column
    """
    if batch is None or batch.num_rows == 0:
        return batch
    
    values_np = batch.column(column).to_numpy()
    n = len(values_np)
    
    # Compute difference
    diff_values = np.full(n, np.nan)
    if k < n:
        diff_values[k:] = values_np[k:] - values_np[:-k]
    
    diff_name = f'{column}_diff{k}'
    return batch.append_column(diff_name, pa.array(diff_values, type=pa.float64()))


def rolling_min(batch, str column, int64_t window=20):
    """
    Rolling minimum.
    
    Args:
        batch: RecordBatch
        column: Column name
        window: Rolling window size
    
    Returns:
        RecordBatch with rolling_min column
    """
    if batch is None or batch.num_rows == 0:
        return batch
    
    values_np = batch.column(column).to_numpy()
    cdef double[:] values = values_np
    cdef int64_t n = len(values)
    cdef double[:] rolling_min_vals = np.zeros(n, dtype=np.float64)
    
    cdef int64_t i, j
    cdef double min_val
    
    with nogil:
        for i in range(n):
            min_val = values[i]
            for j in range(max(0, i - window + 1), i + 1):
                if values[j] < min_val:
                    min_val = values[j]
            rolling_min_vals[i] = min_val
    
    return batch.append_column(f'{column}_min{window}', pa.array(rolling_min_vals, type=pa.float64()))


def rolling_max(batch, str column, int64_t window=20):
    """Rolling maximum."""
    if batch is None or batch.num_rows == 0:
        return batch
    
    values_np = batch.column(column).to_numpy()
    cdef double[:] values = values_np
    cdef int64_t n = len(values)
    cdef double[:] rolling_max_vals = np.zeros(n, dtype=np.float64)
    
    cdef int64_t i, j
    cdef double max_val
    
    with nogil:
        for i in range(n):
            max_val = values[i]
            for j in range(max(0, i - window + 1), i + 1):
                if values[j] > max_val:
                    max_val = values[j]
            rolling_max_vals[i] = max_val
    
    return batch.append_column(f'{column}_max{window}', pa.array(rolling_max_vals, type=pa.float64()))


def rolling_std(batch, str column, int64_t window=20):
    """
    Rolling standard deviation.
    
    Uses online algorithm for numerical stability.
    """
    if batch is None or batch.num_rows == 0:
        return batch
    
    values_np = batch.column(column).to_numpy()
    cdef double[:] values = values_np
    cdef int64_t n = len(values)
    cdef double[:] rolling_std_vals = np.zeros(n, dtype=np.float64)
    
    cdef int64_t i, j, count
    cdef double mean, m2, variance
    
    with nogil:
        for i in range(n):
            mean = 0.0
            m2 = 0.0
            count = 0
            
            # Compute mean
            for j in range(max(0, i - window + 1), i + 1):
                mean += values[j]
                count += 1
            mean /= count
            
            # Compute variance
            for j in range(max(0, i - window + 1), i + 1):
                m2 += (values[j] - mean) * (values[j] - mean)
            
            variance = m2 / count if count > 0 else 0.0
            rolling_std_vals[i] = sqrt(variance)
    
    return batch.append_column(f'{column}_std{window}', pa.array(rolling_std_vals, type=pa.float64()))


def rolling_skew(batch, str column, int64_t window=20):
    """
    Rolling skewness.
    
    Skew = E[(X - μ)³] / σ³
    """
    if batch is None or batch.num_rows == 0:
        return batch
    
    values_np = batch.column(column).to_numpy()
    n = len(values_np)
    skew_vals = np.zeros(n)
    
    for i in range(n):
        window_data = values_np[max(0, i - window + 1):i + 1]
        if len(window_data) > 2:
            mean = np.mean(window_data)
            std = np.std(window_data)
            if std > 1e-10:
                skew_vals[i] = np.mean(((window_data - mean) / std) ** 3)
    
    return batch.append_column(f'{column}_skew{window}', pa.array(skew_vals, type=pa.float64()))


def rolling_kurt(batch, str column, int64_t window=20):
    """
    Rolling kurtosis (excess kurtosis).
    
    Kurt = E[(X - μ)⁴] / σ⁴ - 3
    """
    if batch is None or batch.num_rows == 0:
        return batch
    
    values_np = batch.column(column).to_numpy()
    n = len(values_np)
    kurt_vals = np.zeros(n)
    
    for i in range(n):
        window_data = values_np[max(0, i - window + 1):i + 1]
        if len(window_data) > 3:
            mean = np.mean(window_data)
            std = np.std(window_data)
            if std > 1e-10:
                kurt_vals[i] = np.mean(((window_data - mean) / std) ** 4) - 3.0
    
    return batch.append_column(f'{column}_kurt{window}', pa.array(kurt_vals, type=pa.float64()))


def autocorr(batch, str column, int lag=1, int64_t window=100):
    """
    Rolling autocorrelation at specified lag.
    
    Corr(X_t, X_{t-lag})
    """
    if batch is None or batch.num_rows == 0:
        return batch
    
    values_np = batch.column(column).to_numpy()
    n = len(values_np)
    autocorr_vals = np.zeros(n)
    
    for i in range(n):
        if i >= lag + window - 1:
            x = values_np[i - window + 1:i + 1]
            x_lag = values_np[i - lag - window + 1:i - lag + 1]
            
            if len(x) == len(x_lag) and len(x) > 1:
                autocorr_vals[i] = np.corrcoef(x, x_lag)[0, 1]
    
    return batch.append_column(f'{column}_autocorr{lag}', pa.array(autocorr_vals, type=pa.float64()))


def signed_volume(batch):
    """
    Signed volume: side × size
    
    Args:
        batch: RecordBatch with 'side' (+1/-1) and 'volume'
    
    Returns:
        RecordBatch with 'signed_volume' column
    """
    if batch is None or batch.num_rows == 0:
        return batch
    
    side = batch.column('side')
    volume = batch.column('volume')
    
    signed_vol = pc.multiply(side, volume)
    return batch.append_column('signed_volume', signed_vol)


# ============================================================================
# Market Safety & Controls
# ============================================================================

def stale_quote_detector(batch, int64_t max_age_ms=1000):
    """
    Detect stale quotes (not updated within max_age_ms).
    
    Args:
        batch: RecordBatch with 'timestamp' and 'last_update'
        max_age_ms: Maximum age in milliseconds
    
    Returns:
        RecordBatch with 'is_stale' flag (1=stale, 0=fresh)
    """
    if batch is None or batch.num_rows == 0:
        return batch
    
    timestamp = batch.column('timestamp')
    last_update = batch.column('last_update')
    
    # Age in ms
    age_ms = pc.subtract(timestamp, last_update)
    
    # Flag as stale if age > threshold
    is_stale = pc.cast(pc.greater(age_ms, max_age_ms), pa.int64())
    
    return batch.append_column('is_stale', is_stale)


def price_band_guard(batch, str ref_column='mid', double pct=0.05):
    """
    Price band guard - reject orders outside ±pct% of reference.
    
    Args:
        batch: RecordBatch with 'order_price' and ref_column
        ref_column: Reference price column (e.g., 'mid', 'last')
        pct: Maximum deviation (0.05 = 5%)
    
    Returns:
        RecordBatch with 'price_check' (1=pass, 0=fail)
    """
    if batch is None or batch.num_rows == 0:
        return batch
    
    order_px = batch.column('order_price')
    ref_px = batch.column(ref_column)
    
    # Compute bounds
    lower_bound = pc.multiply(ref_px, 1.0 - pct)
    upper_bound = pc.multiply(ref_px, 1.0 + pct)
    
    # Check if in bounds
    above_lower = pc.greater_equal(order_px, lower_bound)
    below_upper = pc.less_equal(order_px, upper_bound)
    price_check = pc.and_(above_lower, below_upper)
    
    return batch.append_column('price_check', pc.cast(price_check, pa.int64()))


def fat_finger_guard(batch, str ref_column='mid', double pct=0.10, double notional_max=1000000.0):
    """
    Fat finger guard - detect abnormal orders.
    
    Checks:
    1. Price within ±pct% of reference
    2. Notional value < max threshold
    
    Args:
        batch: RecordBatch with 'order_price', 'order_size', ref_column
        ref_column: Reference price
        pct: Max price deviation
        notional_max: Max notional value ($)
    
    Returns:
        RecordBatch with 'fat_finger_check' (1=pass, 0=fail)
    """
    if batch is None or batch.num_rows == 0:
        return batch
    
    order_px = batch.column('order_price')
    order_size = batch.column('order_size')
    ref_px = batch.column(ref_column)
    
    # Price check
    lower = pc.multiply(ref_px, 1.0 - pct)
    upper = pc.multiply(ref_px, 1.0 + pct)
    price_ok = pc.and_(
        pc.greater_equal(order_px, lower),
        pc.less_equal(order_px, upper)
    )
    
    # Notional check
    notional = pc.multiply(order_px, order_size)
    notional_ok = pc.less_equal(notional, notional_max)
    
    # Combined check
    fat_finger_check = pc.and_(price_ok, notional_ok)
    
    return batch.append_column('fat_finger_check', pc.cast(fat_finger_check, pa.int64()))


def circuit_breaker(batch, str column='price', str ref_column='ref_price', double pct=0.05):
    """
    Circuit breaker - halt trading if price moves > pct%.
    
    Args:
        batch: RecordBatch with price columns
        column: Current price column
        ref_column: Reference price column
        pct: Circuit breaker threshold (0.05 = 5%)
    
    Returns:
        RecordBatch with 'circuit_break' flag (1=breaker triggered)
    """
    if batch is None or batch.num_rows == 0:
        return batch
    
    current_px = batch.column(column)
    ref_px = batch.column(ref_column)
    
    # Compute percent change
    pct_change = pc.abs(pc.divide(
        pc.subtract(current_px, ref_px),
        ref_px
    ))
    
    # Trigger if exceeds threshold
    circuit_break = pc.greater(pct_change, pct)
    
    return batch.append_column('circuit_break', pc.cast(circuit_break, pa.int64()))


def throttle_check(batch, double rate_limit=100.0):
    """
    Rate throttle check - limit orders/sec.
    
    Simple token bucket implementation.
    
    Args:
        batch: RecordBatch with 'timestamp' (ms)
        rate_limit: Max orders per second
    
    Returns:
        RecordBatch with 'throttle_pass' (1=pass, 0=reject)
    """
    if batch is None or batch.num_rows == 0:
        return batch
    
    # Simplified: check if more than rate_limit orders in past 1 second
    # For production, use stateful token bucket
    
    n = batch.num_rows
    timestamps = batch.column('timestamp').to_numpy()
    
    throttle_pass = np.ones(n, dtype=np.int64)
    
    # Count orders in rolling 1-second window
    for i in range(n):
        window_start = timestamps[i] - 1000  # 1 second ago
        count = np.sum((timestamps[:i+1] >= window_start) & (timestamps[:i+1] <= timestamps[i]))
        
        if count > rate_limit:
            throttle_pass[i] = 0
    
    return batch.append_column('throttle_pass', pa.array(throttle_pass, type=pa.int64()))


def outlier_flag_mad(batch, str column, int64_t window=100, double k=3.0):
    """
    Outlier detection using MAD (Median Absolute Deviation).
    
    More robust than standard deviation for heavy-tailed data.
    
    MAD = median(|X - median(X)|)
    Outlier if |X - median| > k × MAD
    
    Args:
        batch: RecordBatch
        column: Column to check
        window: Rolling window
        k: Number of MADs (typically 2.5-3.0)
    
    Returns:
        RecordBatch with 'outlier_flag' (1=outlier)
    """
    if batch is None or batch.num_rows == 0:
        return batch
    
    values_np = batch.column(column).to_numpy()
    n = len(values_np)
    outlier_flags = np.zeros(n, dtype=np.int64)
    
    # Constant for consistency with standard deviation (1.4826)
    MAD_SCALE = 1.4826
    
    for i in range(window, n):
        window_data = values_np[i - window:i]
        median = np.median(window_data)
        mad = np.median(np.abs(window_data - median)) * MAD_SCALE
        
        if mad > 1e-10:
            z_score = np.abs(values_np[i] - median) / mad
            if z_score > k:
                outlier_flags[i] = 1
    
    return batch.append_column('outlier_flag', pa.array(outlier_flags, type=pa.int64()))


cdef inline int64_t max(int64_t a, int64_t b) nogil:
    return a if a > b else b

