# cython: language_level=3
"""
Graph Stream Operator Header

Continuous graph pattern matching operator that extends BaseOperator.
"""

from libc.stdint cimport int64_t, int32_t
from libcpp cimport bool as cbool
from libcpp.string cimport string
from libcpp.vector cimport vector

cimport pyarrow.lib as ca
from sabot._cython.operators.base_operator cimport BaseOperator
from sabot._cython.time.watermark_tracker cimport WatermarkTracker
from .match_tracker cimport MatchTracker


cdef class GraphStreamOperator(BaseOperator):
    """
    Stream operator for continuous graph pattern matching.

    Extends BaseOperator - integrates with Stream API and morsel parallelism.
    """

    # Graph state
    cdef object graph  # PyPropertyGraph instance
    cdef object query_engine  # GraphQueryEngine instance

    # Query configuration
    cdef str query_str
    cdef str query_language
    cdef str mode  # 'incremental' or 'continuous'
    cdef object compiled_plan  # Compiled physical plan

    # Deduplication (incremental mode only)
    cdef MatchTracker match_tracker
    cdef cbool track_matches

    # Event-time tracking
    cdef WatermarkTracker watermark_tracker
    cdef str timestamp_column
    cdef cbool has_watermarks

    # Statistics
    cdef int64_t total_updates_processed
    cdef int64_t total_matches_emitted
    cdef int64_t total_matches_deduplicated
    cdef double total_pattern_matching_time_ms

    # Methods
    cpdef object process_batch(self, object batch)
    cpdef list get_partition_keys(self)

    # Private methods
    cdef object _process_updates(self, object batch) except *
    cdef object _apply_updates_to_graph(self, object batch) except *
    cdef object _run_pattern_matching(self) except *
    cdef object _filter_new_matches(self, object matches) except *
    cdef void _update_watermarks(self, object batch) except *
    cdef dict _get_operator_stats(self)
