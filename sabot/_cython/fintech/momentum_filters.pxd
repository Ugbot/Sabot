# cython: language_level=3
"""
Momentum & Filter Kernels - Technical analysis and regime detection.

Moving averages, oscillators, Kalman filters, change detection.
"""

from libc.stdint cimport int64_t, uint64_t
from libcpp cimport bool as cbool
from libcpp.deque cimport deque


# Pure C++ struct for EMA state
cdef extern from * nogil:
    """
    struct EMAState {
        double value;
        double alpha;
        bool initialized;
        
        EMAState(double a) : value(0.0), alpha(a), initialized(false) {}
        
        double update(double x) {
            if (!initialized) {
                value = x;
                initialized = true;
            } else {
                value = alpha * x + (1.0 - alpha) * value;
            }
            return value;
        }
    };
    """
    cdef cppclass EMAState:
        double value
        double alpha
        cbool initialized
        EMAState(double alpha)
        double update(double x) nogil


# Kalman filter 1D state
cdef extern from * nogil:
    """
    struct Kalman1DState {
        double x;      // state estimate
        double P;      // error covariance
        double q;      // process noise
        double r;      // measurement noise
        bool initialized;
        
        Kalman1DState(double q_, double r_) : x(0.0), P(1.0), q(q_), r(r_), initialized(false) {}
        
        double update(double z) {
            if (!initialized) {
                x = z;
                P = r;
                initialized = true;
                return x;
            }
            
            // Predict
            double x_pred = x;
            double P_pred = P + q;
            
            // Update
            double K = P_pred / (P_pred + r);
            x = x_pred + K * (z - x_pred);
            P = (1.0 - K) * P_pred;
            
            return x;
        }
    };
    """
    cdef cppclass Kalman1DState:
        double x
        double P
        double q
        double r
        cbool initialized
        Kalman1DState(double q, double r)
        double update(double z) nogil


# Bollinger bands state
cdef extern from * nogil:
    """
    #include <deque>
    #include <cmath>
    
    struct BollingerState {
        std::deque<double> values;
        size_t window;
        double sum;
        double sum_sq;
        
        BollingerState(size_t w) : window(w), sum(0.0), sum_sq(0.0) {}
        
        void push(double x) {
            values.push_back(x);
            sum += x;
            sum_sq += x * x;
            
            if (values.size() > window) {
                double old = values.front();
                values.pop_front();
                sum -= old;
                sum_sq -= old * old;
            }
        }
        
        double mean() {
            return values.empty() ? 0.0 : sum / values.size();
        }
        
        double std() {
            if (values.size() < 2) return 0.0;
            double m = mean();
            return sqrt((sum_sq / values.size()) - (m * m));
        }
    };
    """
    cdef cppclass BollingerState:
        deque[double] values
        size_t window
        double sum
        double sum_sq
        BollingerState(size_t window)
        void push(double x) nogil
        double mean() nogil
        double std() nogil


cdef class MomentumKernel:
    """Base class for momentum kernels."""
    cpdef object process_batch(self, object batch)


cdef class EMAKernel(MomentumKernel):
    """Exponential moving average."""
    cdef EMAState _state
    cpdef object process_batch(self, object batch)


cdef class SMAKernel(MomentumKernel):
    """Simple moving average."""
    cdef deque[double] _values
    cdef size_t _window
    cdef double _sum
    cpdef object process_batch(self, object batch)


cdef class MACDKernel(MomentumKernel):
    """MACD indicator."""
    cdef EMAState _fast
    cdef EMAState _slow
    cdef EMAState _signal
    cpdef object process_batch(self, object batch)


cdef class RSIKernel(MomentumKernel):
    """Relative Strength Index."""
    cdef EMAState _gain_ema
    cdef EMAState _loss_ema
    cdef double _last_price
    cdef cbool _has_last
    cpdef object process_batch(self, object batch)


cdef class BollingerKernel(MomentumKernel):
    """Bollinger Bands."""
    cdef BollingerState _state
    cdef double _k
    cpdef object process_batch(self, object batch)


cdef class Kalman1DKernel(MomentumKernel):
    """1D Kalman filter for price/level."""
    cdef Kalman1DState _state
    cpdef object process_batch(self, object batch)


cdef class DonchianKernel(MomentumKernel):
    """Donchian channel (high/low breakout)."""
    cdef deque[double] _values
    cdef size_t _window
    cpdef object process_batch(self, object batch)

