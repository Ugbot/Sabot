# -*- coding: utf-8 -*-
"""
Arrow Join Processor Cython Header

Header declarations for ArrowJoinProcessor.
"""

from libc.stdint cimport int64_t


cdef class ArrowJoinProcessor:
    cdef:
        object batch_processor

    # Join operations
    cpdef object stream_stream_join(self, object left_batch, object right_batch,
                                   str left_key, str right_key,
                                   str left_time_col, str right_time_col,
                                   int64_t window_size_ms, str join_type=?)
    cpdef object stream_table_join(self, object stream_batch, object table_batch,
                                  str stream_key, str table_key, str join_type=?)
    cpdef object interval_join(self, object left_batch, object right_batch,
                              str left_key, str right_key,
                              str left_time_col, str right_time_col,
                              int64_t interval_ms, str join_type=?)
    cpdef object temporal_join(self, object stream_batch, object table_batch,
                              str stream_key, str table_key, str table_time_col,
                              str join_type=?)
    cpdef object table_table_join(self, object left_batch, object right_batch,
                                 str left_key, str right_key, str join_type=?)

    # Utilities
    cpdef object get_join_stats(self)
