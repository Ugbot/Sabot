# cython: language_level=3, boundscheck=False, wraparound=False, cdivision=True, nonecheck=False
"""
Momentum & Filter Kernels - Technical indicators and regime detection.

All kernels are stateful and maintain running state across batches.
"""

import cython
from libc.math cimport sqrt, fabs, isnan
from libc.stdint cimport int64_t
import numpy as np

from sabot import cyarrow as pa
import pyarrow.compute as pc


cdef class MomentumKernel:
    """Base class."""
    cpdef object process_batch(self, object batch):
        return batch


cdef class EMAKernel(MomentumKernel):
    """
    Exponential Moving Average.
    
    EMA_t = α × x_t + (1-α) × EMA_{t-1}
    
    Common alphas:
    - 0.2 (5-period equivalent)
    - 0.1 (10-period equivalent)
    - 0.05 (20-period equivalent)
    """
    
    def __cinit__(self, double alpha=0.1):
        self._state = EMAState(alpha)
    
    cpdef object process_batch(self, object batch):
        """Compute EMA for batch."""
        if batch is None or batch.num_rows == 0:
            return batch
        
        values_np = batch.column(0).to_numpy()
        cdef double[:] values = values_np
        cdef int64_t n = len(values)
        cdef double[:] ema = np.zeros(n, dtype=np.float64)
        
        cdef int64_t i
        with nogil:
            for i in range(n):
                if not isnan(values[i]):
                    ema[i] = self._state.update(values[i])
        
        return batch.append_column('ema', pa.array(ema, type=pa.float64()))


cdef class SMAKernel(MomentumKernel):
    """
    Simple Moving Average.
    
    SMA_t = (1/n) × Σ x_{t-i}
    """
    
    def __cinit__(self, int64_t window=20):
        self._window = window
        self._sum = 0.0
    
    cpdef object process_batch(self, object batch):
        """Compute SMA for batch."""
        if batch is None or batch.num_rows == 0:
            return batch
        
        values_np = batch.column(0).to_numpy()
        cdef double[:] values = values_np
        cdef int64_t n = len(values)
        cdef double[:] sma = np.zeros(n, dtype=np.float64)
        
        cdef int64_t i
        cdef double current_sum
        cdef size_t count
        
        with nogil:
            for i in range(n):
                if not isnan(values[i]):
                    self._values.push_back(values[i])
                    self._sum += values[i]
                    
                    if self._values.size() > self._window:
                        self._sum -= self._values.front()
                        self._values.pop_front()
                    
                    count = self._values.size()
                    sma[i] = self._sum / count if count > 0 else 0.0
        
        return batch.append_column('sma', pa.array(sma, type=pa.float64()))


cdef class MACDKernel(MomentumKernel):
    """
    MACD - Moving Average Convergence Divergence.
    
    MACD = EMA_fast - EMA_slow
    Signal = EMA(MACD, signal_period)
    Histogram = MACD - Signal
    
    Standard: (12, 26, 9)
    """
    
    def __cinit__(self, int fast=12, int slow=26, int signal=9):
        # Convert periods to alpha: α = 2/(n+1)
        cdef double alpha_fast = 2.0 / (fast + 1)
        cdef double alpha_slow = 2.0 / (slow + 1)
        cdef double alpha_signal = 2.0 / (signal + 1)
        
        self._fast = EMAState(alpha_fast)
        self._slow = EMAState(alpha_slow)
        self._signal = EMAState(alpha_signal)
    
    cpdef object process_batch(self, object batch):
        """Compute MACD for batch."""
        if batch is None or batch.num_rows == 0:
            return batch
        
        values_np = batch.column(0).to_numpy()
        cdef double[:] values = values_np
        cdef int64_t n = len(values)
        cdef double[:] macd = np.zeros(n, dtype=np.float64)
        cdef double[:] signal_line = np.zeros(n, dtype=np.float64)
        cdef double[:] histogram = np.zeros(n, dtype=np.float64)
        
        cdef int64_t i
        cdef double fast_val, slow_val, macd_val
        
        with nogil:
            for i in range(n):
                if not isnan(values[i]):
                    fast_val = self._fast.update(values[i])
                    slow_val = self._slow.update(values[i])
                    macd_val = fast_val - slow_val
                    
                    macd[i] = macd_val
                    signal_line[i] = self._signal.update(macd_val)
                    histogram[i] = macd[i] - signal_line[i]
        
        result = batch.append_column('macd', pa.array(macd, type=pa.float64()))
        result = result.append_column('macd_signal', pa.array(signal_line, type=pa.float64()))
        result = result.append_column('macd_hist', pa.array(histogram, type=pa.float64()))
        return result


cdef class RSIKernel(MomentumKernel):
    """
    Relative Strength Index.
    
    RSI = 100 - (100 / (1 + RS))
    RS = EMA(gains) / EMA(losses)
    
    Wilder smoothing: α = 1/period
    Standard period: 14
    """
    
    def __cinit__(self, int period=14):
        cdef double alpha = 1.0 / period
        self._gain_ema = EMAState(alpha)
        self._loss_ema = EMAState(alpha)
        self._last_price = 0.0
        self._has_last = False
    
    cpdef object process_batch(self, object batch):
        """Compute RSI for batch."""
        if batch is None or batch.num_rows == 0:
            return batch
        
        values_np = batch.column(0).to_numpy()
        cdef double[:] values = values_np
        cdef int64_t n = len(values)
        cdef double[:] rsi = np.zeros(n, dtype=np.float64)
        
        cdef int64_t i
        cdef double change, gain, loss, avg_gain, avg_loss, rs
        
        with nogil:
            for i in range(n):
                if not isnan(values[i]):
                    if self._has_last:
                        change = values[i] - self._last_price
                        gain = change if change > 0 else 0.0
                        loss = -change if change < 0 else 0.0
                        
                        avg_gain = self._gain_ema.update(gain)
                        avg_loss = self._loss_ema.update(loss)
                        
                        if avg_loss > 1e-10:
                            rs = avg_gain / avg_loss
                            rsi[i] = 100.0 - (100.0 / (1.0 + rs))
                        else:
                            rsi[i] = 100.0
                    else:
                        rsi[i] = 50.0  # Neutral
                    
                    self._last_price = values[i]
                    self._has_last = True
        
        return batch.append_column('rsi', pa.array(rsi, type=pa.float64()))


cdef class BollingerKernel(MomentumKernel):
    """
    Bollinger Bands.
    
    Middle = SMA(price, n)
    Upper = Middle + k × StdDev(price, n)
    Lower = Middle - k × StdDev(price, n)
    
    Standard: n=20, k=2
    """
    
    def __cinit__(self, int64_t window=20, double k=2.0):
        self._state = BollingerState(window)
        self._k = k
    
    cpdef object process_batch(self, object batch):
        """Compute Bollinger Bands for batch."""
        if batch is None or batch.num_rows == 0:
            return batch
        
        values_np = batch.column(0).to_numpy()
        cdef double[:] values = values_np
        cdef int64_t n = len(values)
        cdef double[:] mid = np.zeros(n, dtype=np.float64)
        cdef double[:] upper = np.zeros(n, dtype=np.float64)
        cdef double[:] lower = np.zeros(n, dtype=np.float64)
        
        cdef int64_t i
        cdef double mean, std
        
        with nogil:
            for i in range(n):
                if not isnan(values[i]):
                    self._state.push(values[i])
                    mean = self._state.mean()
                    std = self._state.std()
                    
                    mid[i] = mean
                    upper[i] = mean + self._k * std
                    lower[i] = mean - self._k * std
        
        result = batch.append_column('bb_mid', pa.array(mid, type=pa.float64()))
        result = result.append_column('bb_upper', pa.array(upper, type=pa.float64()))
        result = result.append_column('bb_lower', pa.array(lower, type=pa.float64()))
        return result


cdef class Kalman1DKernel(MomentumKernel):
    """
    1D Kalman filter for price/level smoothing.
    
    Predicts next state and updates with measurement.
    Useful for denoising prices or estimating fair value.
    
    Args:
        q: Process noise (variance of state changes)
        r: Measurement noise (variance of observations)
    """
    
    def __cinit__(self, double q=0.001, double r=0.1):
        self._state = Kalman1DState(q, r)
    
    cpdef object process_batch(self, object batch):
        """Apply Kalman filter to batch."""
        if batch is None or batch.num_rows == 0:
            return batch
        
        values_np = batch.column(0).to_numpy()
        cdef double[:] values = values_np
        cdef int64_t n = len(values)
        cdef double[:] filtered = np.zeros(n, dtype=np.float64)
        
        cdef int64_t i
        with nogil:
            for i in range(n):
                if not isnan(values[i]):
                    filtered[i] = self._state.update(values[i])
        
        return batch.append_column('kalman_filtered', pa.array(filtered, type=pa.float64()))


cdef class DonchianKernel(MomentumKernel):
    """
    Donchian Channel - highest high and lowest low.
    
    High = max(price, n)
    Low = min(price, n)
    Mid = (High + Low) / 2
    
    Used for breakout strategies.
    """
    
    def __cinit__(self, int64_t window=20):
        self._window = window
    
    cpdef object process_batch(self, object batch):
        """Compute Donchian channel for batch."""
        if batch is None or batch.num_rows == 0:
            return batch
        
        values_np = batch.column(0).to_numpy()
        cdef double[:] values = values_np
        cdef int64_t n = len(values)
        cdef double[:] high = np.zeros(n, dtype=np.float64)
        cdef double[:] low = np.zeros(n, dtype=np.float64)
        cdef double[:] mid = np.zeros(n, dtype=np.float64)
        
        cdef int64_t i, j
        cdef double max_val, min_val
        
        with nogil:
            for i in range(n):
                if not isnan(values[i]):
                    self._values.push_back(values[i])
                    
                    if self._values.size() > self._window:
                        self._values.pop_front()
                    
                    # Find max and min in window
                    max_val = self._values[0]
                    min_val = self._values[0]
                    for j in range(self._values.size()):
                        if self._values[j] > max_val:
                            max_val = self._values[j]
                        if self._values[j] < min_val:
                            min_val = self._values[j]
                    
                    high[i] = max_val
                    low[i] = min_val
                    mid[i] = (max_val + min_val) / 2.0
        
        result = batch.append_column('donchian_high', pa.array(high, type=pa.float64()))
        result = result.append_column('donchian_low', pa.array(low, type=pa.float64()))
        result = result.append_column('donchian_mid', pa.array(mid, type=pa.float64()))
        return result


# ============================================================================
# Python API Functions
# ============================================================================

def ema(batch, double alpha=0.1):
    """Exponential moving average."""
    kernel = EMAKernel(alpha=alpha)
    return kernel.process_batch(batch)


def sma(batch, int64_t window=20):
    """Simple moving average."""
    kernel = SMAKernel(window=window)
    return kernel.process_batch(batch)


def macd(batch, int fast=12, int slow=26, int signal=9):
    """MACD indicator with signal and histogram."""
    kernel = MACDKernel(fast=fast, slow=slow, signal=signal)
    return kernel.process_batch(batch)


def rsi(batch, int period=14):
    """Relative Strength Index."""
    kernel = RSIKernel(period=period)
    return kernel.process_batch(batch)


def bollinger_bands(batch, int64_t window=20, double k=2.0):
    """Bollinger Bands (middle, upper, lower)."""
    kernel = BollingerKernel(window=window, k=k)
    return kernel.process_batch(batch)


def kalman_1d(batch, double q=0.001, double r=0.1):
    """1D Kalman filter for smoothing."""
    kernel = Kalman1DKernel(q=q, r=r)
    return kernel.process_batch(batch)


def donchian_channel(batch, int64_t window=20):
    """Donchian channel (high, low, mid)."""
    kernel = DonchianKernel(window=window)
    return kernel.process_batch(batch)


def stochastic_osc(batch, int k_period=14, int d_period=3):
    """
    Stochastic Oscillator.
    
    %K = 100 × (Close - Low_n) / (High_n - Low_n)
    %D = SMA(%K, d_period)
    
    Args:
        batch: RecordBatch with 'high', 'low', 'close'
        k_period: Lookback period for %K
        d_period: Smoothing period for %D
    
    Returns:
        RecordBatch with 'stoch_k' and 'stoch_d' columns
    """
    if batch is None or batch.num_rows == 0:
        return batch
    
    # Use Arrow compute for vectorized operations
    high = batch.column('high')
    low = batch.column('low')
    close = batch.column('close')
    
    # Compute rolling high/low (Python-level for now)
    high_np = high.to_numpy()
    low_np = low.to_numpy()
    close_np = close.to_numpy()
    
    n = len(close_np)
    k_values = np.zeros(n)
    
    for i in range(k_period - 1, n):
        window_high = np.max(high_np[i - k_period + 1:i + 1])
        window_low = np.min(low_np[i - k_period + 1:i + 1])
        
        if window_high - window_low > 1e-10:
            k_values[i] = 100.0 * (close_np[i] - window_low) / (window_high - window_low)
        else:
            k_values[i] = 50.0
    
    # Smooth %K to get %D
    d_values = np.convolve(k_values, np.ones(d_period)/d_period, mode='same')
    
    result = batch.append_column('stoch_k', pa.array(k_values, type=pa.float64()))
    result = result.append_column('stoch_d', pa.array(d_values, type=pa.float64()))
    return result

