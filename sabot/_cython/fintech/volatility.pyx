# cython: language_level=3, boundscheck=False, wraparound=False, cdivision=True, nonecheck=False
"""
Volatility Kernels - Realized variance and jump-robust estimators.

All estimators use high-frequency returns (log_returns).
"""

import cython
from libc.math cimport sqrt, fabs, pow
from libc.stdint cimport int64_t
import numpy as np

from sabot import cyarrow as pa
import pyarrow.compute as pc


def realized_var(batch, str returns_column='log_return', int64_t window=20):
    """
    Realized variance: sum of squared returns.
    
    RV_t = Σ r²_i over window
    
    Args:
        batch: RecordBatch with returns
        returns_column: Column name for returns
        window: Number of periods
    
    Returns:
        RecordBatch with 'realized_var' column
    """
    if batch is None or batch.num_rows == 0:
        return batch
    
    returns_np = batch.column(returns_column).to_numpy()
    cdef double[:] returns = returns_np
    cdef int64_t n = len(returns)
    cdef double[:] rv = np.zeros(n, dtype=np.float64)
    
    cdef int64_t i, j
    cdef double sum_sq
    
    # Compute rolling RV
    with nogil:
        for i in range(n):
            sum_sq = 0.0
            for j in range(max(0, i - window + 1), i + 1):
                sum_sq += returns[j] * returns[j]
            rv[i] = sum_sq
    
    rv_array = pa.array(rv, type=pa.float64())
    return batch.append_column('realized_var', rv_array)


def bipower_var(batch, str returns_column='log_return', int64_t window=20):
    """
    Bipower variation: jump-robust volatility estimator.
    
    BPV_t = (π/2) × Σ |r_i| |r_{i-1}|
    
    Barndorff-Nielsen & Shephard (2004).
    Robust to price jumps.
    
    Args:
        batch: RecordBatch with returns
        returns_column: Column name
        window: Number of periods
    
    Returns:
        RecordBatch with 'bipower_var' column
    """
    if batch is None or batch.num_rows == 0:
        return batch
    
    returns_np = batch.column(returns_column).to_numpy()
    cdef double[:] returns = returns_np
    cdef int64_t n = len(returns)
    cdef double[:] bpv = np.zeros(n, dtype=np.float64)
    
    cdef double PI_OVER_2 = 1.5707963267948966  # π/2
    cdef int64_t i, j
    cdef double sum_bp
    
    with nogil:
        for i in range(1, n):
            sum_bp = 0.0
            for j in range(max(1, i - window + 1), i + 1):
                sum_bp += fabs(returns[j]) * fabs(returns[j - 1])
            bpv[i] = PI_OVER_2 * sum_bp
    
    bpv_array = pa.array(bpv, type=pa.float64())
    return batch.append_column('bipower_var', bpv_array)


def medrv(batch, str returns_column='log_return', int64_t window=20):
    """
    Median realized variance: HF-robust estimator.
    
    MedRV uses medians of squared returns to avoid microstructure noise.
    
    Args:
        batch: RecordBatch with returns
        returns_column: Column name
        window: Number of periods
    
    Returns:
        RecordBatch with 'medrv' column
    """
    if batch is None or batch.num_rows == 0:
        return batch
    
    returns_np = batch.column(returns_column).to_numpy()
    cdef double[:] returns = returns_np
    cdef int64_t n = len(returns)
    
    # Python-level computation (median requires sorting)
    medrv_values = []
    for i in range(n):
        window_returns = returns_np[max(0, i - window + 1):i + 1]
        squared_returns = window_returns ** 2
        median_rv = np.median(squared_returns) if len(squared_returns) > 0 else 0.0
        medrv_values.append(median_rv * len(squared_returns))  # Scale by count
    
    medrv_array = pa.array(medrv_values, type=pa.float64())
    return batch.append_column('medrv', medrv_array)


def riskmetrics_vol(batch, str returns_column='log_return', double lambda_param=0.94):
    """
    RiskMetrics EWMA volatility.
    
    σ²_t = λ × σ²_{t-1} + (1-λ) × r²_t
    
    J.P. Morgan RiskMetrics (1996).
    λ = 0.94 for daily, 0.97 for monthly.
    
    Args:
        batch: RecordBatch with returns
        returns_column: Column name
        lambda_param: Decay factor (0 < λ < 1)
    
    Returns:
        RecordBatch with 'riskmetrics_vol' column
    """
    if batch is None or batch.num_rows == 0:
        return batch
    
    returns_np = batch.column(returns_column).to_numpy()
    cdef double[:] returns = returns_np
    cdef int64_t n = len(returns)
    cdef double[:] vol = np.zeros(n, dtype=np.float64)
    
    cdef double variance = 0.0
    cdef int64_t i
    cdef cbool initialized = False
    
    with nogil:
        for i in range(n):
            if not initialized:
                variance = returns[i] * returns[i]
                initialized = True
            else:
                variance = lambda_param * variance + (1.0 - lambda_param) * returns[i] * returns[i]
            
            vol[i] = sqrt(variance)
    
    vol_array = pa.array(vol, type=pa.float64())
    return batch.append_column('riskmetrics_vol', vol_array)

