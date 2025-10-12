# cython: language_level=3
"""
Cython header for online statistics kernels.

Stateful streaming primitives with O(1) updates and numerically stable algorithms.
"""

from libc.stdint cimport int64_t, uint64_t
from libcpp cimport bool as cbool
from libcpp.vector cimport vector
from libcpp.deque cimport deque


# Pure C++ struct for Welford's online mean/variance
cdef extern from * nogil:
    """
    struct WelfordState {
        uint64_t count;
        double mean;
        double m2;      // sum of squared differences from mean
        double variance;
        double stddev;
        
        WelfordState() : count(0), mean(0.0), m2(0.0), variance(0.0), stddev(0.0) {}
        
        void update(double value) {
            count++;
            double delta = value - mean;
            mean += delta / count;
            double delta2 = value - mean;
            m2 += delta * delta2;
            
            if (count > 1) {
                variance = m2 / (count - 1);
                stddev = sqrt(variance);
            }
        }
        
        void reset() {
            count = 0;
            mean = 0.0;
            m2 = 0.0;
            variance = 0.0;
            stddev = 0.0;
        }
    };
    """
    cdef cppclass WelfordState:
        uint64_t count
        double mean
        double m2
        double variance
        double stddev
        WelfordState()
        void update(double value) nogil
        void reset() nogil


# Pure C++ struct for EWMA state
cdef extern from * nogil:
    """
    struct EWMAState {
        double value;
        double alpha;
        bool initialized;
        
        EWMAState(double alpha_) : value(0.0), alpha(alpha_), initialized(false) {}
        
        double update(double new_value) {
            if (!initialized) {
                value = new_value;
                initialized = true;
            } else {
                value = alpha * new_value + (1.0 - alpha) * value;
            }
            return value;
        }
        
        void reset() {
            value = 0.0;
            initialized = false;
        }
    };
    """
    cdef cppclass EWMAState:
        double value
        double alpha
        cbool initialized
        EWMAState(double alpha)
        double update(double new_value) nogil
        void reset() nogil


# Pure C++ struct for EWCOV state (exponentially weighted covariance)
cdef extern from * nogil:
    """
    struct EWCOVState {
        double mean_x;
        double mean_y;
        double cov;
        double alpha;
        bool initialized;
        
        EWCOVState(double alpha_) : mean_x(0.0), mean_y(0.0), cov(0.0), alpha(alpha_), initialized(false) {}
        
        double update(double x, double y) {
            if (!initialized) {
                mean_x = x;
                mean_y = y;
                cov = 0.0;
                initialized = true;
            } else {
                double dx = x - mean_x;
                mean_x = alpha * x + (1.0 - alpha) * mean_x;
                mean_y = alpha * y + (1.0 - alpha) * mean_y;
                cov = (1.0 - alpha) * (cov + alpha * dx * (y - mean_y));
            }
            return cov;
        }
        
        double get_correlation(double std_x, double std_y) {
            if (std_x > 0.0 && std_y > 0.0) {
                return cov / (std_x * std_y);
            }
            return 0.0;
        }
        
        void reset() {
            mean_x = 0.0;
            mean_y = 0.0;
            cov = 0.0;
            initialized = false;
        }
    };
    """
    cdef cppclass EWCOVState:
        double mean_x
        double mean_y
        double cov
        double alpha
        cbool initialized
        EWCOVState(double alpha)
        double update(double x, double y) nogil
        double get_correlation(double std_x, double std_y) nogil
        void reset() nogil


# Pure C++ struct for rolling window with deque
cdef extern from * nogil:
    """
    #include <deque>
    #include <cmath>
    
    struct RollingWindow {
        std::deque<double> values;
        size_t window_size;
        double sum;
        double sum_sq;
        
        RollingWindow(size_t size) : window_size(size), sum(0.0), sum_sq(0.0) {}
        
        void push(double value) {
            values.push_back(value);
            sum += value;
            sum_sq += value * value;
            
            // Evict old value if window is full
            if (values.size() > window_size) {
                double old = values.front();
                values.pop_front();
                sum -= old;
                sum_sq -= old * old;
            }
        }
        
        double mean() {
            if (values.empty()) return 0.0;
            return sum / values.size();
        }
        
        double variance() {
            if (values.size() < 2) return 0.0;
            double m = mean();
            return (sum_sq / values.size()) - (m * m);
        }
        
        double stddev() {
            return sqrt(variance());
        }
        
        size_t count() {
            return values.size();
        }
        
        void reset() {
            values.clear();
            sum = 0.0;
            sum_sq = 0.0;
        }
    };
    """
    cdef cppclass RollingWindow:
        deque[double] values
        size_t window_size
        double sum
        double sum_sq
        RollingWindow(size_t size)
        void push(double value) nogil
        double mean() nogil
        double variance() nogil
        double stddev() nogil
        size_t count() nogil
        void reset() nogil


# Cython kernel classes
cdef class OnlineStatsKernel:
    """Base class for online statistics kernels."""
    cdef cbool _initialized
    cpdef object process_batch(self, object batch)


cdef class LogReturnsKernel(OnlineStatsKernel):
    """Compute log returns: r_t = log(p_t / p_{t-1})"""
    cdef double _last_price
    cdef cbool _has_last_price
    cpdef object process_batch(self, object batch)


cdef class WelfordKernel(OnlineStatsKernel):
    """Welford's online mean/variance algorithm."""
    cdef WelfordState _state
    cpdef object process_batch(self, object batch)
    cdef void _update_value(self, double value) nogil


cdef class EWMAKernel(OnlineStatsKernel):
    """Exponentially weighted moving average."""
    cdef EWMAState _state
    cpdef object process_batch(self, object batch)
    cdef double _update_value(self, double value) nogil


cdef class EWCOVKernel(OnlineStatsKernel):
    """Exponentially weighted covariance."""
    cdef EWCOVState _state
    cpdef object process_batch(self, object batch)
    cdef double _update_values(self, double x, double y) nogil


cdef class RollingZScoreKernel(OnlineStatsKernel):
    """Rolling z-score with online mean/stddev."""
    cdef RollingWindow _window
    cpdef object process_batch(self, object batch)
    cdef double _compute_zscore(self, double value) nogil

