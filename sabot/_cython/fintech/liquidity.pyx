# cython: language_level=3, boundscheck=False, wraparound=False, cdivision=True, nonecheck=False
"""
Liquidity & Market Impact Kernels.

Kyle's lambda, Amihud illiquidity, spread estimators.
"""

import cython
from libc.math cimport sqrt, fabs, log
from libc.stdint cimport int64_t
import numpy as np

from sabot import cyarrow as pa
import pyarrow.compute as pc


def kyle_lambda(batch, str returns_column='log_return', str volume_column='signed_volume', int64_t window=20):
    """
    Kyle's lambda: price impact coefficient.
    
    λ = Cov(Δp, signed_volume) / Var(signed_volume)
    
    Kyle (1985). Measures $ price move per $ of signed volume.
    
    Args:
        batch: RecordBatch with returns and signed volume
        returns_column: Price change column
        volume_column: Signed volume (+ buy, - sell)
        window: Rolling window
    
    Returns:
        RecordBatch with 'kyle_lambda' column
    """
    if batch is None or batch.num_rows == 0:
        return batch
    
    returns_np = batch.column(returns_column).to_numpy()
    volume_np = batch.column(volume_column).to_numpy()
    
    cdef double[:] returns = returns_np
    cdef double[:] volume = volume_np
    cdef int64_t n = len(returns)
    cdef double[:] lambda_values = np.zeros(n, dtype=np.float64)
    
    cdef int64_t i, j, count
    cdef double mean_r, mean_v, cov, var_v, delta_r, delta_v
    
    # Compute rolling Kyle lambda
    with nogil:
        for i in range(window - 1, n):
            # Compute means
            mean_r = 0.0
            mean_v = 0.0
            count = 0
            for j in range(i - window + 1, i + 1):
                mean_r += returns[j]
                mean_v += volume[j]
                count += 1
            mean_r /= count
            mean_v /= count
            
            # Compute covariance and variance
            cov = 0.0
            var_v = 0.0
            for j in range(i - window + 1, i + 1):
                delta_r = returns[j] - mean_r
                delta_v = volume[j] - mean_v
                cov += delta_r * delta_v
                var_v += delta_v * delta_v
            
            # Kyle's lambda
            if var_v > 1e-10:
                lambda_values[i] = cov / var_v
            else:
                lambda_values[i] = 0.0
    
    lambda_array = pa.array(lambda_values, type=pa.float64())
    return batch.append_column('kyle_lambda', lambda_array)


def amihud_illiquidity(batch, str returns_column='log_return', str volume_column='volume', int64_t window=20):
    """
    Amihud illiquidity measure.
    
    ILLIQ_t = E[|r_i| / volume_i]
    
    Amihud (2002). Average price impact per unit volume.
    Higher values = less liquid.
    
    Args:
        batch: RecordBatch with returns and volume
        returns_column: Returns column
        volume_column: Volume column (dollar volume preferred)
        window: Rolling window
    
    Returns:
        RecordBatch with 'amihud_illiquidity' column
    """
    if batch is None or batch.num_rows == 0:
        return batch
    
    returns_np = batch.column(returns_column).to_numpy()
    volume_np = batch.column(volume_column).to_numpy()
    
    cdef double[:] returns = returns_np
    cdef double[:] volume = volume_np
    cdef int64_t n = len(returns)
    cdef double[:] illiq = np.zeros(n, dtype=np.float64)
    
    cdef int64_t i, j, count
    cdef double sum_illiq
    
    with nogil:
        for i in range(window - 1, n):
            sum_illiq = 0.0
            count = 0
            for j in range(i - window + 1, i + 1):
                if volume[j] > 1e-10:
                    sum_illiq += fabs(returns[j]) / volume[j]
                    count += 1
            
            if count > 0:
                illiq[i] = sum_illiq / count
            else:
                illiq[i] = 0.0
    
    illiq_array = pa.array(illiq, type=pa.float64())
    return batch.append_column('amihud_illiquidity', illiq_array)


def roll_spread(batch, str returns_column='log_return', int64_t window=20):
    """
    Roll's spread estimator from serial covariance.
    
    spread = 2 × sqrt(-Cov(r_t, r_{t-1}))
    
    Roll (1984). Estimates bid-ask spread from returns alone.
    
    Args:
        batch: RecordBatch with returns
        returns_column: Returns column
        window: Rolling window
    
    Returns:
        RecordBatch with 'roll_spread' column
    """
    if batch is None or batch.num_rows == 0:
        return batch
    
    returns_np = batch.column(returns_column).to_numpy()
    cdef double[:] returns = returns_np
    cdef int64_t n = len(returns)
    cdef double[:] spread = np.zeros(n, dtype=np.float64)
    
    cdef int64_t i, j, count
    cdef double mean_r, cov, delta_r1, delta_r2
    
    with nogil:
        for i in range(window, n):
            # Compute mean
            mean_r = 0.0
            count = 0
            for j in range(i - window + 1, i + 1):
                mean_r += returns[j]
                count += 1
            mean_r /= count
            
            # Compute serial covariance
            cov = 0.0
            for j in range(i - window + 2, i + 1):
                delta_r1 = returns[j] - mean_r
                delta_r2 = returns[j - 1] - mean_r
                cov += delta_r1 * delta_r2
            cov /= (count - 1)
            
            # Roll spread (handle negative cov)
            if cov < 0.0:
                spread[i] = 2.0 * sqrt(-cov)
            else:
                spread[i] = 0.0
    
    spread_array = pa.array(spread, type=pa.float64())
    return batch.append_column('roll_spread', spread_array)


def corwin_schultz_spread(batch, str high_column='high', str low_column='low', int64_t window=2):
    """
    Corwin-Schultz spread estimator from high/low prices.
    
    Uses high-low range over 1-day and 2-day periods.
    
    Corwin & Schultz (2012).
    
    Args:
        batch: RecordBatch with daily high/low prices
        high_column: High price column
        low_column: Low price column
        window: Window for computation (typically 2)
    
    Returns:
        RecordBatch with 'corwin_schultz_spread' column
    """
    if batch is None or batch.num_rows == 0:
        return batch
    
    high_np = batch.column(high_column).to_numpy()
    low_np = batch.column(low_column).to_numpy()
    
    cdef double[:] high = high_np
    cdef double[:] low = low_np
    cdef int64_t n = len(high)
    cdef double[:] spread = np.zeros(n, dtype=np.float64)
    
    cdef int64_t i
    cdef double beta, gamma, alpha, S
    cdef double high_max_2d, low_min_2d
    cdef double K = 0.30 # Constant from paper
    
    with nogil:
        for i in range(window, n):
            # High-low ratio over 1 day
            beta = log(high[i] / low[i])
            beta = beta * beta
            
            # High-low ratio over 2 days
            high_max_2d = max(high[i], high[i-1])
            low_min_2d = min(low[i], low[i-1])
            gamma = log(high_max_2d / low_min_2d)
            gamma = gamma * gamma
            
            # Compute spread
            alpha = (sqrt(2.0 * beta) - sqrt(beta)) / (3.0 - 2.0 * sqrt(2.0))
            alpha -= sqrt(gamma / (3.0 - 2.0 * sqrt(2.0)))
            
            S = (2.0 * (exp(alpha) - 1.0)) / (1.0 + exp(alpha))
            spread[i] = S
    
    spread_array = pa.array(spread, type=pa.float64())
    return batch.append_column('corwin_schultz_spread', spread_array)


cdef inline double exp(double x) nogil:
    """Fast exp approximation or use <math.h>"""
    from libc.math cimport exp as c_exp
    return c_exp(x)


cdef inline double max(double a, double b) nogil:
    return a if a > b else b


cdef inline double min(double a, double b) nogil:
    return a if a < b else b

