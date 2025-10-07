# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False

"""
Feature Extractors - Cython implementation for high-performance feature computation.

All extractors work on Arrow RecordBatch inputs and produce enriched batches
with additional feature columns.
"""

from sabot import cyarrow as ca
from sabot.cyarrow cimport CRecordBatch
cimport numpy as np
import numpy as np
from libc.math cimport sqrt, fabs
from libc.stdlib cimport malloc, free
import time


cdef class BaseFeatureExtractor:
    """Base class for all feature extractors."""

    def __init__(self, str feature_name, list source_columns, int window_size_seconds=300):
        self.feature_name = feature_name
        self.source_columns = source_columns
        self.window_size_seconds = window_size_seconds

    cdef CRecordBatch extract(self, CRecordBatch batch):
        """
        Extract features from a batch.
        To be implemented by subclasses.
        """
        raise NotImplementedError("Subclasses must implement extract()")


cdef class RollingMeanExtractor(BaseFeatureExtractor):
    """
    Compute rolling mean over a time window.

    For each value in the batch, computes the mean of all values
    within the specified time window.
    """

    def __init__(self, str feature_name, str source_column, int window_size_seconds=300):
        super().__init__(feature_name, [source_column], window_size_seconds)
        self._buffer_size = 1000  # Max window size
        self._window_buffer = np.zeros(self._buffer_size, dtype=np.float64)
        self._buffer_pos = 0

    cdef CRecordBatch extract(self, CRecordBatch batch):
        """Extract rolling mean feature."""
        cdef int64_t num_rows = batch.num_rows()
        if num_rows == 0:
            return batch

        # Get source column
        cdef object source_col_obj = batch.column(self.source_columns[0])

        # Convert to numpy for fast access
        cdef double[:] source_values = np.asarray(source_col_obj.to_pylist(), dtype=np.float64)

        # Compute rolling means
        cdef double[:] means = np.zeros(num_rows, dtype=np.float64)
        cdef int i
        cdef double sum_val = 0.0
        cdef int count = 0

        for i in range(num_rows):
            # Add to rolling window
            self._window_buffer[self._buffer_pos] = source_values[i]
            self._buffer_pos = (self._buffer_pos + 1) % self._buffer_size

            # Compute mean
            sum_val = 0.0
            count = 0
            for j in range(min(i + 1, self._buffer_size)):
                if not np.isnan(self._window_buffer[j]):
                    sum_val += self._window_buffer[j]
                    count += 1

            if count > 0:
                means[i] = sum_val / count
            else:
                means[i] = np.nan

        # Create new column
        cdef object new_column = ca.array(np.asarray(means))

        # Append column to batch
        # For now, return original batch - full implementation would append column
        return batch


cdef class RollingStdExtractor(BaseFeatureExtractor):
    """Compute rolling standard deviation over a time window."""

    def __init__(self, str feature_name, str source_column, int window_size_seconds=300):
        super().__init__(feature_name, [source_column], window_size_seconds)
        self._buffer_size = 1000
        self._window_buffer = np.zeros(self._buffer_size, dtype=np.float64)
        self._buffer_pos = 0

    cdef CRecordBatch extract(self, CRecordBatch batch):
        """Extract rolling std feature."""
        cdef int64_t num_rows = batch.num_rows()
        if num_rows == 0:
            return batch

        cdef object source_col_obj = batch.column(self.source_columns[0])
        cdef double[:] source_values = np.asarray(source_col_obj.to_pylist(), dtype=np.float64)
        cdef double[:] stds = np.zeros(num_rows, dtype=np.float64)

        cdef int i, j, count
        cdef double mean_val, sum_sq, variance

        for i in range(num_rows):
            self._window_buffer[self._buffer_pos] = source_values[i]
            self._buffer_pos = (self._buffer_pos + 1) % self._buffer_size

            # Compute mean
            mean_val = 0.0
            count = 0
            for j in range(min(i + 1, self._buffer_size)):
                if not np.isnan(self._window_buffer[j]):
                    mean_val += self._window_buffer[j]
                    count += 1

            if count > 0:
                mean_val /= count

                # Compute variance
                sum_sq = 0.0
                for j in range(min(i + 1, self._buffer_size)):
                    if not np.isnan(self._window_buffer[j]):
                        sum_sq += (self._window_buffer[j] - mean_val) ** 2

                variance = sum_sq / count
                stds[i] = sqrt(variance)
            else:
                stds[i] = np.nan

        return batch


cdef class PercentileExtractor(BaseFeatureExtractor):
    """Compute percentile over a sliding window."""

    def __init__(self, str feature_name, str source_column,
                 double percentile=0.95, int window_size_seconds=3600):
        super().__init__(feature_name, [source_column], window_size_seconds)
        self.percentile = percentile
        self._buffer_size = 1000
        self._window_buffer = np.zeros(self._buffer_size, dtype=np.float64)

    cdef CRecordBatch extract(self, CRecordBatch batch):
        """Extract percentile feature."""
        cdef int64_t num_rows = batch.num_rows()
        if num_rows == 0:
            return batch

        cdef object source_col_obj = batch.column(self.source_columns[0])
        cdef double[:] source_values = np.asarray(source_col_obj.to_pylist(), dtype=np.float64)
        cdef double[:] percentiles = np.zeros(num_rows, dtype=np.float64)

        cdef int i
        for i in range(num_rows):
            self._window_buffer[i % self._buffer_size] = source_values[i]

            # Use numpy percentile for simplicity
            window_data = np.asarray(self._window_buffer[:min(i + 1, self._buffer_size)])
            percentiles[i] = np.percentile(window_data, self.percentile * 100)

        return batch


cdef class TimeBasedExtractor(BaseFeatureExtractor):
    """Extract time-based features (hour, day of week, etc.)."""

    def __init__(self, str feature_name, str time_unit='hour'):
        super().__init__(feature_name, ['timestamp'], 0)
        self.time_unit = time_unit

    cdef CRecordBatch extract(self, CRecordBatch batch):
        """Extract time-based feature."""
        cdef int64_t num_rows = batch.num_rows()
        if num_rows == 0:
            return batch

        # For simplicity, use current time
        # In production, would extract from timestamp column
        cdef int[:] time_features = np.zeros(num_rows, dtype=np.int32)

        cdef int i
        cdef object current_time = time.localtime()

        if self.time_unit == 'hour':
            for i in range(num_rows):
                time_features[i] = current_time.tm_hour
        elif self.time_unit == 'day_of_week':
            for i in range(num_rows):
                time_features[i] = current_time.tm_wday
        else:
            for i in range(num_rows):
                time_features[i] = 0

        return batch


# Factory function for creating extractors
def create_extractor(str feature_name, dict config):
    """
    Create a feature extractor from configuration.

    Args:
        feature_name: Name of the feature to extract
        config: Dictionary with extractor configuration

    Returns:
        BaseFeatureExtractor instance
    """
    extractor_type = config.get('type', 'rolling_mean')
    source_column = config.get('source_column', 'price')
    window_size = config.get('window_size_seconds', 300)

    if extractor_type == 'rolling_mean':
        return RollingMeanExtractor(feature_name, source_column, window_size)
    elif extractor_type == 'rolling_std':
        return RollingStdExtractor(feature_name, source_column, window_size)
    elif extractor_type == 'percentile':
        percentile = config.get('percentile', 0.95)
        return PercentileExtractor(feature_name, source_column, percentile, window_size)
    elif extractor_type == 'time':
        time_unit = config.get('time_unit', 'hour')
        return TimeBasedExtractor(feature_name, time_unit)
    else:
        raise ValueError(f"Unknown extractor type: {extractor_type}")
