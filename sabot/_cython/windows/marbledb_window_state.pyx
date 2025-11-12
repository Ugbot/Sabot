# -*- coding: utf-8 -*-
# cython: language_level=3
"""
MarbleDB Window State Implementation

Cython implementation for window aggregates using MarbleDB Arrow Batch API.
Stores window aggregates as Arrow batches and uses NewIterator for scanning closed windows.
"""

import logging
from typing import List, Optional

from libc.stdint cimport int64_t, uint64_t
from libcpp.string cimport string
from libcpp.memory cimport shared_ptr

from sabot._cython.stores.marbledb_store cimport MarbleDBStoreBackend

logger = logging.getLogger(__name__)

cimport pyarrow.lib as pa_lib


cdef class MarbleDBWindowState:
    """
    Window state storage using MarbleDB Arrow Batch API.
    
    Stores window aggregates as Arrow RecordBatches with schema:
    - window_key: string (e.g., "symbol:window_start")
    - window_start: timestamp
    - count: int64
    - sum: float64
    - min: float64
    - max: float64
    """
    
    cdef MarbleDBStoreBackend _store
    cdef str _window_table_name
    cdef object _schema
    
    def __init__(self, str db_path):
        """Initialize window state storage."""
        self._store = MarbleDBStoreBackend(db_path, {})
        self._store.open()
        self._window_table_name = "window_aggregates"
        self._create_window_schema()
    
    cdef void _create_window_schema(self):
        """Create column family for window aggregates."""
        import pyarrow as pa
        
        schema = pa.schema([
            pa.field("window_key", pa.string()),      # "symbol:window_start"
            pa.field("window_start", pa.timestamp('ms')),
            pa.field("count", pa.int64()),
            pa.field("sum", pa.float64()),
            pa.field("min", pa.float64()),
            pa.field("max", pa.float64())
        ])
        
        self._schema = schema
        self._store.create_column_family(self._window_table_name, schema)
    
    cpdef void update_window(self, str symbol, int64_t window_start, 
                            int64_t count, double sum_val, double min_val, double max_val):
        """Update window aggregate - uses InsertBatch()."""
        import pyarrow as pa
        
        window_key = f"{symbol}:{window_start}"
        
        # Build Arrow RecordBatch
        batch = pa.record_batch([
            pa.array([window_key]),
            pa.array([window_start], type=pa.timestamp('ms')),
            pa.array([count]),
            pa.array([sum_val]),
            pa.array([min_val]),
            pa.array([max_val])
        ], schema=self._schema)
        
        # Insert batch (upsert semantics handled by MarbleDB)
        self._store.insert_batch(self._window_table_name, batch)
    
    cpdef list scan_closed_windows(self, int64_t watermark):
        """Scan closed windows - uses NewIterator() with range."""
        # Use scan_range_to_table to get windows before watermark
        table = self._store.scan_range_to_table(
            self._window_table_name,
            start_key=None,
            end_key=f"watermark_{watermark}"
        )
        
        # Convert to list of windows
        return table.to_pylist()
    
    cpdef void cleanup_closed_windows(self, int64_t watermark):
        """Delete closed windows - uses DeleteRange()."""
        self._store.delete_range(
            self._window_table_name,
            "",
            f"watermark_{watermark}"
        )
    
    cpdef void close(self):
        """Close window state storage."""
        self._store.close()

