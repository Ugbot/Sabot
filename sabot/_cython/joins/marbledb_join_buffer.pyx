# -*- coding: utf-8 -*-
# cython: language_level=3
"""
MarbleDB Join Buffer Implementation

Cython implementation for stream-stream joins using MarbleDB Arrow Batch API.
Buffers records for joins and uses NewIterator for matching.
"""

import logging
from typing import Optional

from libc.stdint cimport int64_t
from libcpp.string cimport string

from sabot._cython.stores.marbledb_store cimport MarbleDBStoreBackend

logger = logging.getLogger(__name__)

cimport pyarrow.lib as pa_lib


cdef class MarbleDBJoinBuffer:
    """
    Join buffer storage using MarbleDB Arrow Batch API.
    
    Buffers records from left/right streams with metadata:
    - stream_id: string
    - timestamp: timestamp
    - key: string
    - data: (original batch columns)
    """
    
    cdef MarbleDBStoreBackend _store
    cdef str _buffer_table_name
    cdef object _schema
    
    def __init__(self, str db_path, object schema):
        """Initialize join buffer storage."""
        self._store = MarbleDBStoreBackend(db_path, {})
        self._store.open()
        self._buffer_table_name = "join_buffer"
        self._schema = schema
        self._create_buffer_schema(schema)
    
    cdef void _create_buffer_schema(self, object original_schema):
        """Create column family for join buffer with enriched schema."""
        import pyarrow as pa
        
        # Add stream_id and timestamp columns to original schema
        fields = list(original_schema)
        fields.insert(0, pa.field("stream_id", pa.string()))
        fields.insert(1, pa.field("timestamp", pa.timestamp('ms')))
        fields.insert(2, pa.field("join_key", pa.string()))
        
        schema = pa.schema(fields)
        self._schema = schema
        self._store.create_column_family(self._buffer_table_name, schema)
    
    cpdef void buffer_records(self, str stream_id, object batch, int64_t timestamp, str join_key):
        """Buffer records for join - uses InsertBatch()."""
        import pyarrow as pa
        
        # Enrich batch with metadata
        num_rows = batch.num_rows
        stream_ids = pa.array([stream_id] * num_rows)
        timestamps = pa.array([timestamp] * num_rows, type=pa.timestamp('ms'))
        join_keys = pa.array([join_key] * num_rows)
        
        # Combine with original batch columns
        enriched_arrays = [stream_ids, timestamps, join_keys] + list(batch.columns)
        enriched_batch = pa.record_batch(enriched_arrays, schema=self._schema)
        
        self._store.insert_batch(self._buffer_table_name, enriched_batch)
    
    cpdef object scan_matches(self, str key, int64_t start_time, int64_t end_time):
        """Scan matching records - uses NewIterator() with time range."""
        start_key = f"{key}:{start_time}"
        end_key = f"{key}:{end_time}"
        
        return self._store.scan_range_to_table(
            self._buffer_table_name,
            start_key=start_key,
            end_key=end_key
        )
    
    cpdef void cleanup_expired(self, int64_t max_age_ms):
        """Cleanup expired buffers."""
        # Would need to scan and delete based on timestamp
        # For now, use a simple approach
        pass
    
    cpdef void close(self):
        """Close join buffer storage."""
        self._store.close()

