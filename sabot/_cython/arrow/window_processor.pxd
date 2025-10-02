# -*- coding: utf-8 -*-
"""
Arrow Window Processor Cython Header

Header declarations for ArrowWindowProcessor.
"""


cdef class ArrowWindowProcessor:
    cdef:
        str timestamp_column
        str window_column
        object batch_processor

    # Window assignment operations
    cpdef object assign_tumbling_windows(self, object batch, int64_t window_size_ms)
    cpdef object assign_sliding_windows(self, object batch, int64_t window_size_ms,
                                       int64_t slide_ms)
    cpdef object assign_session_windows(self, object batch, int64_t gap_ms)

    # Window aggregation
    cpdef object aggregate_windows(self, object windowed_batch, str group_by_columns,
                                  dict aggregations)
