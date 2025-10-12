# cython: language_level=3, boundscheck=False, wraparound=False, cdivision=True, nonecheck=False
"""
Execution & Risk Kernels - TCA, optimal execution, VaR, CUSUM.
"""

import cython
from libc.math cimport sqrt, fabs, log, exp, pow
from libc.stdint cimport int64_t
import numpy as np

from sabot import cyarrow as pa
import pyarrow.compute as pc


# ============================================================================
# Execution & TCA Kernels
# ============================================================================

def vwap(batch, str price_column='price', str volume_column='volume'):
    """
    Volume-weighted average price.
    
    VWAP = Σ (price × volume) / Σ volume
    
    Args:
        batch: RecordBatch with price and volume
        price_column: Price column name
        volume_column: Volume column name
    
    Returns:
        RecordBatch with 'vwap' column
    """
    if batch is None or batch.num_rows == 0:
        return batch
    
    price = batch.column(price_column)
    volume = batch.column(volume_column)
    
    # Weighted price
    weighted = pc.multiply(price, volume)
    
    # Cumulative sums
    cum_weighted = pc.cumulative_sum(weighted)
    cum_volume = pc.cumulative_sum(volume)
    
    # VWAP
    vwap_values = pc.divide(cum_weighted, cum_volume)
    
    return batch.append_column('vwap', vwap_values)


def implementation_shortfall(batch):
    """
    Implementation shortfall (IS) - slippage measure.
    
    IS = (avg_exec_price - arrival_price) / arrival_price
    
    Positive IS = paid more than arrival (bad for buys).
    
    Args:
        batch: RecordBatch with 'exec_price', 'arrival_price', 'volume'
    
    Returns:
        RecordBatch with 'implementation_shortfall_bps' column
    """
    if batch is None or batch.num_rows == 0:
        return batch
    
    exec_price = batch.column('exec_price')
    arrival_price = batch.column('arrival_price')
    
    # IS = (exec - arrival) / arrival
    shortfall = pc.divide(
        pc.subtract(exec_price, arrival_price),
        arrival_price
    )
    
    # Convert to bps
    shortfall_bps = pc.multiply(shortfall, 10000.0)
    
    return batch.append_column('implementation_shortfall_bps', shortfall_bps)


def optimal_ac_schedule(double Q, double T, double sigma, double gamma, double eta):
    """
    Almgren-Chriss optimal execution schedule (closed-form).
    
    Minimizes: E[cost] + γ × Var[cost]
    
    Returns trajectory x(t) = Q × sinh(κ(T-t)) / sinh(κT)
    where κ = sqrt(γ / (2 η σ²))
    
    Args:
        Q: Total quantity to trade
        T: Time horizon (seconds)
        sigma: Price volatility (per sqrt(second))
        gamma: Risk aversion parameter
        eta: Temporary price impact coefficient
    
    Returns:
        List of slice sizes at each time step
    """
    # Compute κ
    kappa = sqrt(gamma / (2.0 * eta * sigma * sigma))
    
    # Generate time steps (10 slices)
    n_slices = 10
    dt = T / n_slices
    
    slices = []
    remaining = Q
    
    for i in range(n_slices):
        t = i * dt
        t_next = (i + 1) * dt
        
        # x(t) = Q × sinh(κ(T-t)) / sinh(κT)
        x_t = Q * np.sinh(kappa * (T - t)) / np.sinh(kappa * T)
        x_next = Q * np.sinh(kappa * (T - t_next)) / np.sinh(kappa * T)
        
        slice_size = x_t - x_next
        slices.append(slice_size)
        remaining -= slice_size
    
    return slices


def arrival_price_impact(batch):
    """
    Arrival price impact (vs mid at order entry).
    
    Impact = (exec_price - arrival_mid) / arrival_mid (in bps)
    
    Args:
        batch: RecordBatch with 'exec_price', 'arrival_mid'
    
    Returns:
        RecordBatch with 'arrival_impact_bps' column
    """
    if batch is None or batch.num_rows == 0:
        return batch
    
    exec_price = batch.column('exec_price')
    arrival_mid = batch.column('arrival_mid')
    
    impact_bps = pc.multiply(
        pc.divide(
            pc.subtract(exec_price, arrival_mid),
            arrival_mid
        ),
        10000.0
    )
    
    return batch.append_column('arrival_impact_bps', impact_bps)


# ============================================================================
# Risk Kernels
# ============================================================================

def cusum(batch, str column='value', double k=0.5, double h=5.0):
    """
    CUSUM change detection (cumulative sum control chart).
    
    S_t = max(0, S_{t-1} + (x_t - μ - k))
    
    Raises alarm when S_t > h.
    
    Args:
        batch: RecordBatch with values
        column: Column to monitor
        k: Slack parameter (allowance for random variation)
        h: Threshold for alarm
    
    Returns:
        RecordBatch with 'cusum_stat' and 'cusum_alarm' columns
    """
    if batch is None or batch.num_rows == 0:
        return batch
    
    values_np = batch.column(column).to_numpy()
    cdef double[:] values = values_np
    cdef int64_t n = len(values)
    cdef double[:] cusum_stat = np.zeros(n, dtype=np.float64)
    cdef int64_t[:] alarm = np.zeros(n, dtype=np.int64)
    
    # Estimate mean from first few values
    cdef double mean = np.mean(values_np[:min(100, n)])
    cdef double S = 0.0
    cdef int64_t i
    
    with nogil:
        for i in range(n):
            S = max(0.0, S + (values[i] - mean - k))
            cusum_stat[i] = S
            if S > h:
                alarm[i] = 1
    
    stat_array = pa.array(cusum_stat, type=pa.float64())
    alarm_array = pa.array(alarm, type=pa.int64())
    
    result = batch.append_column('cusum_stat', stat_array)
    result = result.append_column('cusum_alarm', alarm_array)
    return result


def historical_var(batch, str column='pnl', double alpha=0.05, int64_t window=250):
    """
    Historical Value at Risk.
    
    VaR_α = -quantile(returns, α)
    
    Args:
        batch: RecordBatch with P&L or returns
        column: Column name
        alpha: Confidence level (0.05 = 95% VaR)
        window: Rolling window
    
    Returns:
        RecordBatch with 'var' column
    """
    if batch is None or batch.num_rows == 0:
        return batch
    
    values_np = batch.column(column).to_numpy()
    cdef double[:] values = values_np
    cdef int64_t n = len(values)
    
    # Compute rolling quantile
    var_values = []
    for i in range(n):
        window_data = values_np[max(0, i - window + 1):i + 1]
        if len(window_data) > 0:
            quantile = np.quantile(window_data, alpha)
            var_values.append(-quantile)  # VaR is positive
        else:
            var_values.append(0.0)
    
    var_array = pa.array(var_values, type=pa.float64())
    return batch.append_column('var', var_array)


def historical_cvar(batch, str column='pnl', double alpha=0.05, int64_t window=250):
    """
    Conditional Value at Risk (Expected Shortfall).
    
    CVaR_α = E[loss | loss > VaR_α]
    
    Args:
        batch: RecordBatch with P&L or returns
        column: Column name
        alpha: Confidence level
        window: Rolling window
    
    Returns:
        RecordBatch with 'cvar' column
    """
    if batch is None or batch.num_rows == 0:
        return batch
    
    values_np = batch.column(column).to_numpy()
    cdef int64_t n = len(values_np)
    
    # Compute rolling CVaR
    cvar_values = []
    for i in range(n):
        window_data = values_np[max(0, i - window + 1):i + 1]
        if len(window_data) > 0:
            var_threshold = -np.quantile(window_data, alpha)
            # Mean of losses exceeding VaR
            tail_losses = window_data[window_data <= -var_threshold]
            if len(tail_losses) > 0:
                cvar = -np.mean(tail_losses)
            else:
                cvar = var_threshold
            cvar_values.append(cvar)
        else:
            cvar_values.append(0.0)
    
    cvar_array = pa.array(cvar_values, type=pa.float64())
    return batch.append_column('cvar', cvar_array)


def cornish_fisher_var(batch, double alpha=0.05):
    """
    Cornish-Fisher VaR adjustment for skew/kurtosis.
    
    Adjusts normal VaR for fat tails and asymmetry.
    
    Args:
        batch: RecordBatch with 'mean', 'stddev', 'skew', 'kurt'
        alpha: Confidence level
    
    Returns:
        RecordBatch with 'cf_var' column
    """
    if batch is None or batch.num_rows == 0:
        return batch
    
    from scipy.stats import norm
    
    mean = batch.column('mean').to_numpy()
    std = batch.column('stddev').to_numpy()
    skew = batch.column('skew').to_numpy()
    kurt = batch.column('kurt').to_numpy()
    
    # Standard normal quantile
    z = norm.ppf(alpha)
    
    # Cornish-Fisher expansion
    cf_z = (z +
            (z**2 - 1) * skew / 6.0 +
            (z**3 - 3*z) * kurt / 24.0 -
            (2*z**3 - 5*z) * skew**2 / 36.0)
    
    # VaR = -mean - cf_z × std
    var_values = -(mean + cf_z * std)
    
    var_array = pa.array(var_values, type=pa.float64())
    return batch.append_column('cf_var', var_array)


cdef inline double max(double a, double b) nogil:
    return a if a > b else b


cdef inline int64_t min(int64_t a, int64_t b) nogil:
    return a if a < b else b

