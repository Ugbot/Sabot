# cython: language_level=3

"""
Feature Extractors - Header file for Cython declarations.
"""

from sabot.cyarrow cimport CRecordBatch
cimport numpy as np


cdef class BaseFeatureExtractor:
    cdef str feature_name
    cdef list source_columns
    cdef int window_size_seconds

    cdef CRecordBatch extract(self, CRecordBatch batch)


cdef class RollingMeanExtractor(BaseFeatureExtractor):
    cdef double[:] _window_buffer
    cdef int _buffer_pos
    cdef int _buffer_size

    cdef CRecordBatch extract(self, CRecordBatch batch)


cdef class RollingStdExtractor(BaseFeatureExtractor):
    cdef double[:] _window_buffer
    cdef int _buffer_pos
    cdef int _buffer_size

    cdef CRecordBatch extract(self, CRecordBatch batch)


cdef class PercentileExtractor(BaseFeatureExtractor):
    cdef double percentile
    cdef double[:] _window_buffer
    cdef int _buffer_size

    cdef CRecordBatch extract(self, CRecordBatch batch)


cdef class TimeBasedExtractor(BaseFeatureExtractor):
    cdef str time_unit  # 'hour', 'day', 'day_of_week'

    cdef CRecordBatch extract(self, CRecordBatch batch)
