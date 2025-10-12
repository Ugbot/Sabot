# cython: language_level=3, boundscheck=False, wraparound=False, cdivision=True, nonecheck=False
"""
Cross-Asset Kernels - Asynchronous covariance and correlation.

Hayashi-Yoshida covariance for non-synchronous tick data.
"""

import cython
from libc.math cimport sqrt, fabs
from libc.stdint cimport int64_t
import numpy as np

from sabot import cyarrow as pa


def hayashi_yoshida_cov(batch_a, batch_b, int64_t window_ms=60000):
    """
    Hayashi-Yoshida covariance for asynchronous tick data.
    
    Computes covariance between two tick streams without requiring
    synchronous observations. Key for cross-asset risk when data
    arrives at different times (e.g., different exchanges, assets).
    
    Formula:
    Cov_HY = Σ r_a^i × r_b^j  for all overlapping intervals [t_{i-1}, t_i] ∩ [s_{j-1}, s_j]
    
    Reference:
    Hayashi & Yoshida (2005) "On covariance estimation of non-synchronously
    observed diffusion processes"
    
    Args:
        batch_a: RecordBatch with 'timestamp', 'returns' for asset A
        batch_b: RecordBatch with 'timestamp', 'returns' for asset B
        window_ms: Rolling window in milliseconds
    
    Returns:
        Array of covariance values aligned to batch_a timestamps
    """
    if batch_a is None or batch_b is None:
        return pa.array([], type=pa.float64())
    
    # Extract data
    ts_a = batch_a.column('timestamp').to_numpy()
    returns_a = batch_a.column('returns').to_numpy()
    ts_b = batch_b.column('timestamp').to_numpy()
    returns_b = batch_b.column('returns').to_numpy()
    
    cdef int64_t[:] timestamps_a = ts_a.astype(np.int64)
    cdef int64_t[:] timestamps_b = ts_b.astype(np.int64)
    cdef double[:] r_a = returns_a
    cdef double[:] r_b = returns_b
    
    cdef int64_t n_a = len(timestamps_a)
    cdef int64_t n_b = len(timestamps_b)
    cdef double[:] hy_cov = np.zeros(n_a, dtype=np.float64)
    
    cdef int64_t i, j, k
    cdef int64_t window_start, overlap_start, overlap_end
    cdef double cov_sum
    
    # Compute HY covariance for each timestamp in A
    with nogil:
        for i in range(1, n_a):
            window_start = timestamps_a[i] - window_ms
            cov_sum = 0.0
            
            # Find all returns in A within window
            for j in range(i):
                if timestamps_a[j] < window_start:
                    continue
                
                # For each return in A, find overlapping returns in B
                for k in range(n_b - 1):
                    # Check if intervals [timestamps_a[j-1], timestamps_a[j]] and
                    # [timestamps_b[k], timestamps_b[k+1]] overlap
                    
                    if j == 0:
                        continue  # Need previous timestamp for A
                    
                    # Overlap: max(start_a, start_b) < min(end_a, end_b)
                    overlap_start = max(timestamps_a[j-1], timestamps_b[k])
                    overlap_end = min(timestamps_a[j], timestamps_b[k+1])
                    
                    if overlap_start < overlap_end:
                        # Intervals overlap - add contribution
                        cov_sum += r_a[j] * r_b[k+1]
            
            hy_cov[i] = cov_sum
    
    return pa.array(hy_cov, type=pa.float64())


def hayashi_yoshida_corr(batch_a, batch_b, int64_t window_ms=60000):
    """
    Hayashi-Yoshida correlation for asynchronous data.
    
    Corr_HY = Cov_HY(A, B) / sqrt(Cov_HY(A, A) × Cov_HY(B, B))
    
    Args:
        batch_a: RecordBatch for asset A
        batch_b: RecordBatch for asset B
        window_ms: Window size
    
    Returns:
        Array of correlation values
    """
    # Compute covariances
    cov_ab = hayashi_yoshida_cov(batch_a, batch_b, window_ms).to_numpy()
    cov_aa = hayashi_yoshida_cov(batch_a, batch_a, window_ms).to_numpy()
    cov_bb = hayashi_yoshida_cov(batch_b, batch_b, window_ms).to_numpy()
    
    # Compute correlation
    n = len(cov_ab)
    corr = np.zeros(n)
    
    for i in range(n):
        denom = np.sqrt(cov_aa[i] * cov_bb[i])
        if denom > 1e-10:
            corr[i] = cov_ab[i] / denom
        else:
            corr[i] = 0.0
    
    return pa.array(corr, type=pa.float64())


def xcorr_leadlag(batch_a, batch_b, int max_lag=10, double alpha=0.94):
    """
    Cross-correlation with lead-lag detection.
    
    Finds optimal lag k* that maximizes correlation between A and B.
    Useful for detecting which asset leads/lags.
    
    Returns:
    - lag*: Optimal lag (positive = A leads B, negative = B leads A)
    - rho*: Maximum correlation at lag*
    
    Args:
        batch_a: RecordBatch for asset A
        batch_b: RecordBatch for asset B
        max_lag: Maximum lag to search (±max_lag)
        alpha: EWMA smoothing for returns
    
    Returns:
        Struct with 'optimal_lag' and 'max_corr' columns
    """
    if batch_a is None or batch_b is None:
        return pa.record_batch({
            'optimal_lag': pa.array([], type=pa.int64()),
            'max_corr': pa.array([], type=pa.float64())
        })
    
    returns_a = batch_a.column('returns').to_numpy()
    returns_b = batch_b.column('returns').to_numpy()
    
    n = min(len(returns_a), len(returns_b))
    
    optimal_lags = np.zeros(n, dtype=np.int64)
    max_corrs = np.zeros(n)
    
    # For each point, compute correlation at different lags
    window = 50  # Correlation window
    
    for i in range(window + max_lag, n):
        best_corr = -1.0
        best_lag = 0
        
        for lag in range(-max_lag, max_lag + 1):
            # Extract windows
            if lag >= 0:
                # A leads B
                window_a = returns_a[i - window - lag:i - lag]
                window_b = returns_b[i - window:i]
            else:
                # B leads A
                window_a = returns_a[i - window:i]
                window_b = returns_b[i - window + lag:i + lag]
            
            if len(window_a) == len(window_b) and len(window_a) > 1:
                corr = np.corrcoef(window_a, window_b)[0, 1]
                if not np.isnan(corr) and abs(corr) > abs(best_corr):
                    best_corr = corr
                    best_lag = lag
        
        optimal_lags[i] = best_lag
        max_corrs[i] = best_corr
    
    return pa.record_batch({
        'optimal_lag': pa.array(optimal_lags, type=pa.int64()),
        'max_corr': pa.array(max_corrs, type=pa.float64())
    })


def pairs_spread(batch_a, batch_b, double hedge_ratio=1.0):
    """
    Compute pairs trading spread.
    
    Spread = price_a - hedge_ratio × price_b
    
    Args:
        batch_a: RecordBatch with 'price' for asset A
        batch_b: RecordBatch with 'price' for asset B  
        hedge_ratio: Hedge ratio (can be estimated via OLS/Kalman)
    
    Returns:
        Array of spread values
    """
    if batch_a is None or batch_b is None:
        return pa.array([], type=pa.float64())
    
    price_a = batch_a.column('price')
    price_b = batch_b.column('price')
    
    # Ensure same length
    n = min(len(price_a), len(price_b))
    
    import pyarrow.compute as pc
    spread = pc.subtract(
        price_a[:n],
        pc.multiply(price_b[:n], hedge_ratio)
    )
    
    return spread


def kalman_hedge_ratio(batch_a, batch_b, double q=0.001, double r=0.1):
    """
    Kalman filter for time-varying hedge ratio.
    
    Estimates β_t in: price_a = β_t × price_b + ε_t
    
    Useful for pairs trading with changing relationships.
    
    Args:
        batch_a: RecordBatch for asset A
        batch_b: RecordBatch for asset B
        q: Process noise (how fast β changes)
        r: Measurement noise
    
    Returns:
        Struct with 'hedge_ratio' and 'spread' columns
    """
    if batch_a is None or batch_b is None:
        return pa.record_batch({
            'hedge_ratio': pa.array([], type=pa.float64()),
            'spread': pa.array([], type=pa.float64())
        })
    
    price_a = batch_a.column('price').to_numpy()
    price_b = batch_b.column('price').to_numpy()
    
    n = min(len(price_a), len(price_b))
    
    # Kalman filter state
    beta = 1.0  # Initial hedge ratio
    P = 1.0     # Error covariance
    
    hedge_ratios = np.zeros(n)
    spreads = np.zeros(n)
    
    for i in range(n):
        if price_b[i] > 1e-10:  # Avoid division by zero
            # Predict
            beta_pred = beta
            P_pred = P + q
            
            # Measurement: z = price_a / price_b
            z = price_a[i] / price_b[i]
            
            # Update
            K = P_pred / (P_pred + r)
            beta = beta_pred + K * (z - beta_pred)
            P = (1.0 - K) * P_pred
            
            hedge_ratios[i] = beta
            spreads[i] = price_a[i] - beta * price_b[i]
    
    return pa.record_batch({
        'hedge_ratio': pa.array(hedge_ratios, type=pa.float64()),
        'spread': pa.array(spreads, type=pa.float64())
    })


cdef inline int64_t max(int64_t a, int64_t b) nogil:
    return a if a > b else b


cdef inline int64_t min(int64_t a, int64_t b) nogil:
    return a if a < b else b

