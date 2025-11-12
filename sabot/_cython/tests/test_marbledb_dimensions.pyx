# -*- coding: utf-8 -*-
# cython: language_level=3
"""
TDD Tests for MarbleDB Dimension Table

Tests for dimension tables: InsertBatch for bulk load, MultiGet for lookups
"""

import tempfile
import shutil
import os

from sabot._cython.materializations.marbledb_dimension cimport MarbleDBDimensionTable


def test_load_dimension_table():
    """Test bulk loading dimension table."""
    cdef MarbleDBDimensionTable dim_table
    cdef str db_path = tempfile.mkdtemp()
    cdef object schema
    cdef object table
    
    try:
        import pyarrow as pa
        
        # Create schema
        schema = pa.schema([
            pa.field("id", pa.int64()),
            pa.field("name", pa.string()),
            pa.field("value", pa.float64())
        ])
        
        dim_table = MarbleDBDimensionTable(db_path, "dim_table", schema)
        
        # Create table
        table = pa.table({
            "id": [1, 2, 3],
            "name": ["a", "b", "c"],
            "value": [1.1, 2.2, 3.3]
        })
        
        # Load dimension table
        dim_table.load_dimension_table(table)
        
        dim_table.close()
        print("✅ test_load_dimension_table passed")
        
    finally:
        shutil.rmtree(db_path, ignore_errors=True)


def test_batch_lookup():
    """Test batch lookup operations."""
    cdef MarbleDBDimensionTable dim_table
    cdef str db_path = tempfile.mkdtemp()
    cdef object schema
    cdef list keys
    cdef dict results
    
    try:
        import pyarrow as pa
        
        # Create schema
        schema = pa.schema([
            pa.field("id", pa.int64()),
            pa.field("name", pa.string()),
            pa.field("value", pa.float64())
        ])
        
        dim_table = MarbleDBDimensionTable(db_path, "dim_table", schema)
        
        # Create and load table
        table = pa.table({
            "id": [1, 2, 3],
            "name": ["a", "b", "c"],
            "value": [1.1, 2.2, 3.3]
        })
        dim_table.load_dimension_table(table)
        
        # Batch lookup
        keys = ["key1", "key2", "key3"]
        results = dim_table.batch_lookup(keys)
        
        assert len(results) == 3, f"Expected 3 results, got {len(results)}"
        
        dim_table.close()
        print("✅ test_batch_lookup passed")
        
    finally:
        shutil.rmtree(db_path, ignore_errors=True)


if __name__ == "__main__":
    test_load_dimension_table()
    test_batch_lookup()
    print("\n✅ All MarbleDB dimension tests passed!")

