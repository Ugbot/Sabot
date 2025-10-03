# cython: language_level=3
"""
High-performance data loader with Cython threading.

Header file for data_loader.pyx
"""

from libc.stdint cimport int64_t
from libcpp cimport bool as cbool

cdef class DataLoader:
    """
    Cython-accelerated data loader with multi-threading.

    Supports:
    - CSV with parallel parsing
    - Arrow IPC with memory-mapping
    - Auto-format detection
    - Column filtering
    - Row limits
    """
    cdef:
        int64_t block_size
        int num_threads
        cbool use_threads
        str compression
        int compression_level

    cpdef object load(self, str path, int64_t limit=?, list columns=?)
    cpdef object load_csv(self, str path, int64_t limit=?, list columns=?)
    cpdef object load_arrow_ipc(self, str path, int64_t limit=?, list columns=?)
    cdef object _load_csv_streaming(self, str path, int64_t limit, object read_options, object parse_options, object convert_options)
    cdef object _convert_null_columns(self, object table)
