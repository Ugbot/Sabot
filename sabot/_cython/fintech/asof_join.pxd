# cython: language_level=3
"""
ASOF Join Kernels - Time-series joins for financial data.

Efficiently join on nearest timestamp (within tolerance).
Critical for matching trades to quotes, aligning multi-source data.
"""

from libc.stdint cimport int64_t, uint64_t
from libcpp cimport bool as cbool
from libcpp.vector cimport vector
from libcpp.string cimport string as cpp_string
from libcpp.unordered_map cimport unordered_map


# C++ struct for timestamp-indexed row (zero-copy)
cdef extern from * nogil:
    """
    struct TimestampedRow {
        int64_t timestamp;
        size_t row_index;
        size_t batch_index;
        
        TimestampedRow() : timestamp(0), row_index(0), batch_index(0) {}
        TimestampedRow(int64_t ts, size_t ri, size_t bi) 
            : timestamp(ts), row_index(ri), batch_index(bi) {}
        
        bool operator<(const TimestampedRow& other) const {
            return timestamp < other.timestamp;
        }
    };
    """
    cdef cppclass TimestampedRow:
        int64_t timestamp
        size_t row_index
        size_t batch_index
        TimestampedRow()
        TimestampedRow(int64_t ts, size_t ri, size_t bi)
        cbool operator<(const TimestampedRow& other) nogil


# C++ struct for sorted index (per symbol)
cdef extern from * nogil:
    """
    #include <vector>
    #include <algorithm>
    
    struct SortedIndex {
        std::vector<TimestampedRow> rows;
        int64_t min_timestamp;
        int64_t max_timestamp;
        
        SortedIndex() : min_timestamp(0), max_timestamp(0) {}
        
        void add_row(int64_t ts, size_t row_idx, size_t batch_idx) {
            TimestampedRow row(ts, row_idx, batch_idx);
            
            // Insert in sorted position (binary insert)
            auto it = std::lower_bound(rows.begin(), rows.end(), row);
            rows.insert(it, row);
            
            // Update bounds
            if (rows.size() == 1) {
                min_timestamp = ts;
                max_timestamp = ts;
            } else {
                if (ts < min_timestamp) min_timestamp = ts;
                if (ts > max_timestamp) max_timestamp = ts;
            }
        }
        
        size_t find_asof_backward(int64_t target_ts, int64_t tolerance) {
            // Find latest row where timestamp <= target_ts (within tolerance)
            size_t best_idx = SIZE_MAX;
            int64_t best_ts_diff = tolerance + 1;
            
            // Binary search for first element >= target
            TimestampedRow target(target_ts, 0, 0);
            auto it = std::upper_bound(rows.begin(), rows.end(), target);
            
            // Move backward to find best match
            if (it != rows.begin()) {
                --it;
                int64_t ts_diff = target_ts - it->timestamp;
                if (ts_diff >= 0 && ts_diff <= tolerance) {
                    best_idx = std::distance(rows.begin(), it);
                }
            }
            
            return best_idx;
        }
        
        size_t find_asof_forward(int64_t target_ts, int64_t tolerance) {
            // Find earliest row where timestamp >= target_ts (within tolerance)
            TimestampedRow target(target_ts, 0, 0);
            auto it = std::lower_bound(rows.begin(), rows.end(), target);
            
            if (it != rows.end()) {
                int64_t ts_diff = it->timestamp - target_ts;
                if (ts_diff >= 0 && ts_diff <= tolerance) {
                    return std::distance(rows.begin(), it);
                }
            }
            
            return SIZE_MAX;
        }
        
        void prune_before(int64_t cutoff_ts) {
            // Remove rows older than cutoff
            TimestampedRow cutoff(cutoff_ts, 0, 0);
            auto it = std::lower_bound(rows.begin(), rows.end(), cutoff);
            rows.erase(rows.begin(), it);
            
            if (!rows.empty()) {
                min_timestamp = rows.front().timestamp;
            }
        }
        
        size_t size() const {
            return rows.size();
        }
        
        void clear() {
            rows.clear();
            min_timestamp = 0;
            max_timestamp = 0;
        }
    };
    """
    cdef cppclass SortedIndex:
        vector[TimestampedRow] rows
        int64_t min_timestamp
        int64_t max_timestamp
        SortedIndex()
        void add_row(int64_t ts, size_t row_idx, size_t batch_idx) nogil
        size_t find_asof_backward(int64_t target_ts, int64_t tolerance) nogil
        size_t find_asof_forward(int64_t target_ts, int64_t tolerance) nogil
        void prune_before(int64_t cutoff_ts) nogil
        size_t size() nogil
        void clear() nogil


cdef class AsofJoinKernel:
    """
    ASOF join kernel - match on nearest timestamp.
    
    Maintains sorted index of right-side data per symbol.
    O(log n) lookup using binary search.
    """
    cdef:
        unordered_map[cpp_string, SortedIndex] _indices  # Per-symbol sorted index
        vector[object] _right_batches  # Store right batches for row access
        str _time_column
        str _by_column
        str _direction
        int64_t _tolerance_ms
        int64_t _max_lookback_ms
        cbool _initialized
    
    cpdef void add_right_batch(self, object batch)
    cpdef object process_left_batch(self, object batch)
    cpdef void prune_old_data(self, int64_t cutoff_ts)
    cpdef dict get_stats(self)


cdef class StreamingAsofJoinKernel(AsofJoinKernel):
    """
    Streaming ASOF join with automatic pruning.
    
    Maintains fixed lookback window to bound memory.
    """
    cpdef void auto_prune(self, int64_t current_ts)

