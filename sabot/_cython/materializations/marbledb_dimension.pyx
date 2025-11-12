# -*- coding: utf-8 -*-
# cython: language_level=3
"""
MarbleDB Dimension Table Implementation

Cython implementation for dimension tables using MarbleDB Arrow Batch API.
Uses InsertBatch for bulk load and MultiGet for fast lookups.
"""

import logging
from typing import List, Dict, Optional

from libcpp.string cimport string
from libcpp.vector cimport vector

from sabot._cython.stores.marbledb_store cimport MarbleDBStoreBackend
from sabot._cython.state.marbledb_backend cimport MarbleDBStateBackend

logger = logging.getLogger(__name__)

cimport pyarrow.lib as pa_lib


cdef class MarbleDBDimensionTable:
    """
    Dimension table storage using MarbleDB Arrow Batch API.
    
    Provides:
    - Bulk load via InsertBatch
    - Fast point lookups via MultiGet
    - Column family with primary key index
    """
    
    cdef MarbleDBStoreBackend _store
    cdef MarbleDBStateBackend _point_store  # For point lookups
    cdef str _table_name
    cdef object _schema
    
    def __init__(self, str db_path, str table_name, object schema):
        """Initialize dimension table storage."""
        self._store = MarbleDBStoreBackend(db_path, {})
        self._store.open()
        self._point_store = MarbleDBStateBackend(db_path)
        self._point_store.open()
        self._table_name = table_name
        self._schema = schema
        self._create_dimension_schema(schema)
    
    cdef void _create_dimension_schema(self, object schema):
        """Create column family for dimension table."""
        self._store.create_column_family(self._table_name, schema)
    
    cpdef void load_dimension_table(self, object pyarrow_table):
        """Load dimension table - uses InsertBatch() for bulk load."""
        # Bulk insert batches
        for batch in pyarrow_table.to_batches():
            self._store.insert_batch(self._table_name, batch)
    
    cpdef dict batch_lookup(self, list keys):
        """Batch lookup - uses MultiGet() for efficiency."""
        # Use point API for fast lookups
        return self._point_store.multi_get_raw(keys)
    
    cpdef object scan_dimension_table(self):
        """Scan entire dimension table."""
        return self._store.scan_table(self._table_name)
    
    cpdef void close(self):
        """Close dimension table storage."""
        self._store.close()
        self._point_store.close()

