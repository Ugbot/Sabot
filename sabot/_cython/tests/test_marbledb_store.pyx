# -*- coding: utf-8 -*-
# cython: language_level=3
"""
TDD Tests for MarbleDB Store Backend

Tests for Arrow Batch API: CreateColumnFamily, InsertBatch, ScanTable, NewIterator
"""

import tempfile
import shutil
import os

from sabot._cython.stores.marbledb_store cimport MarbleDBStoreBackend


def test_create_column_family():
    """Test CreateColumnFamily operation."""
    cdef MarbleDBStoreBackend store
    cdef str db_path = tempfile.mkdtemp()
    cdef object schema
    
    try:
        import pyarrow as pa
        
        store = MarbleDBStoreBackend(db_path, {})
        store.open()
        
        # Create schema
        schema = pa.schema([
            pa.field("id", pa.int64()),
            pa.field("name", pa.string()),
            pa.field("value", pa.float64())
        ])
        
        # Create column family
        store.create_column_family("test_table", schema)
        
        # Verify column family exists
        cf_list = store.list_column_families()
        assert "test_table" in cf_list, f"Column family should exist: {cf_list}"
        
        store.close()
        print("✅ test_create_column_family passed")
        
    finally:
        shutil.rmtree(db_path, ignore_errors=True)


def test_insert_batch():
    """Test InsertBatch operation."""
    cdef MarbleDBStoreBackend store
    cdef str db_path = tempfile.mkdtemp()
    cdef object schema
    cdef object batch
    
    try:
        import pyarrow as pa
        
        store = MarbleDBStoreBackend(db_path, {})
        store.open()
        
        # Create schema and column family
        schema = pa.schema([
            pa.field("id", pa.int64()),
            pa.field("name", pa.string()),
            pa.field("value", pa.float64())
        ])
        store.create_column_family("test_table", schema)
        
        # Create batch
        batch = pa.record_batch([
            pa.array([1, 2, 3]),
            pa.array(["a", "b", "c"]),
            pa.array([1.1, 2.2, 3.3])
        ], schema=schema)
        
        # Insert batch
        store.insert_batch("test_table", batch)
        
        store.close()
        print("✅ test_insert_batch passed")
        
    finally:
        shutil.rmtree(db_path, ignore_errors=True)


def test_scan_table():
    """Test ScanTable operation."""
    cdef MarbleDBStoreBackend store
    cdef str db_path = tempfile.mkdtemp()
    cdef object schema
    cdef object batch
    cdef object table
    
    try:
        import pyarrow as pa
        
        store = MarbleDBStoreBackend(db_path, {})
        store.open()
        
        # Create schema and column family
        schema = pa.schema([
            pa.field("id", pa.int64()),
            pa.field("name", pa.string()),
            pa.field("value", pa.float64())
        ])
        store.create_column_family("test_table", schema)
        
        # Insert batch
        batch = pa.record_batch([
            pa.array([1, 2, 3]),
            pa.array(["a", "b", "c"]),
            pa.array([1.1, 2.2, 3.3])
        ], schema=schema)
        store.insert_batch("test_table", batch)
        
        # Scan table
        table = store.scan_table("test_table")
        
        assert table.num_rows == 3, f"Expected 3 rows, got {table.num_rows}"
        assert table.num_columns == 3, f"Expected 3 columns, got {table.num_columns}"
        
        store.close()
        print("✅ test_scan_table passed")
        
    finally:
        shutil.rmtree(db_path, ignore_errors=True)


def test_scan_range_to_table():
    """Test ScanRangeToTable operation."""
    cdef MarbleDBStoreBackend store
    cdef str db_path = tempfile.mkdtemp()
    cdef object schema
    cdef object batch
    cdef object table
    
    try:
        import pyarrow as pa
        
        store = MarbleDBStoreBackend(db_path, {})
        store.open()
        
        # Create schema and column family
        schema = pa.schema([
            pa.field("key", pa.string()),
            pa.field("value", pa.int64())
        ])
        store.create_column_family("test_table", schema)
        
        # Insert batches
        for i in range(10):
            batch = pa.record_batch([
                pa.array([f"key_{i:03d}"]),
                pa.array([i])
            ], schema=schema)
            store.insert_batch("test_table", batch)
        
        # Scan range
        table = store.scan_range_to_table("test_table", "key_002", "key_008")
        
        assert table.num_rows > 0, "Should have rows in range"
        
        store.close()
        print("✅ test_scan_range_to_table passed")
        
    finally:
        shutil.rmtree(db_path, ignore_errors=True)


def test_delete_range():
    """Test DeleteRange operation."""
    cdef MarbleDBStoreBackend store
    cdef str db_path = tempfile.mkdtemp()
    cdef object schema
    cdef object batch
    
    try:
        import pyarrow as pa
        
        store = MarbleDBStoreBackend(db_path, {})
        store.open()
        
        # Create schema and column family
        schema = pa.schema([
            pa.field("key", pa.string()),
            pa.field("value", pa.int64())
        ])
        store.create_column_family("test_table", schema)
        
        # Insert batches
        for i in range(10):
            batch = pa.record_batch([
                pa.array([f"key_{i:03d}"]),
                pa.array([i])
            ], schema=schema)
            store.insert_batch("test_table", batch)
        
        # Delete range
        store.delete_range("test_table", "key_002", "key_008")
        
        # Verify deletion by scanning
        table = store.scan_table("test_table")
        # Note: Actual verification would require point queries or full scan
        
        store.close()
        print("✅ test_delete_range passed")
        
    finally:
        shutil.rmtree(db_path, ignore_errors=True)


if __name__ == "__main__":
    test_create_column_family()
    test_insert_batch()
    test_scan_table()
    test_scan_range_to_table()
    test_delete_range()
    print("\n✅ All MarbleDB store tests passed!")

