# -*- coding: utf-8 -*-
# cython: language_level=3
"""
TDD Tests for MarbleDB Join Buffer

Tests for join buffering: InsertBatch for buffering, NewIterator for matching
"""

import tempfile
import shutil
import os

from sabot._cython.joins.marbledb_join_buffer cimport MarbleDBJoinBuffer


def test_buffer_records():
    """Test buffering records for joins."""
    cdef MarbleDBJoinBuffer buffer
    cdef str db_path = tempfile.mkdtemp()
    cdef object schema
    cdef object batch
    
    try:
        import pyarrow as pa
        
        # Create schema
        schema = pa.schema([
            pa.field("id", pa.int64()),
            pa.field("value", pa.float64())
        ])
        
        buffer = MarbleDBJoinBuffer(db_path, schema)
        
        # Create batch
        batch = pa.record_batch([
            pa.array([1, 2, 3]),
            pa.array([1.1, 2.2, 3.3])
        ], schema=schema)
        
        # Buffer records
        buffer.buffer_records("left_stream", batch, 1000, "key1")
        
        buffer.close()
        print("✅ test_buffer_records passed")
        
    finally:
        shutil.rmtree(db_path, ignore_errors=True)


def test_scan_matches():
    """Test scanning matching records."""
    cdef MarbleDBJoinBuffer buffer
    cdef str db_path = tempfile.mkdtemp()
    cdef object schema
    cdef object batch
    cdef object matches
    
    try:
        import pyarrow as pa
        
        # Create schema
        schema = pa.schema([
            pa.field("id", pa.int64()),
            pa.field("value", pa.float64())
        ])
        
        buffer = MarbleDBJoinBuffer(db_path, schema)
        
        # Buffer records
        batch = pa.record_batch([
            pa.array([1, 2, 3]),
            pa.array([1.1, 2.2, 3.3])
        ], schema=schema)
        buffer.buffer_records("left_stream", batch, 1000, "key1")
        buffer.buffer_records("right_stream", batch, 1000, "key1")
        
        # Scan matches
        matches = buffer.scan_matches("key1", 500, 1500)
        
        assert matches.num_rows > 0, "Should have matching records"
        
        buffer.close()
        print("✅ test_scan_matches passed")
        
    finally:
        shutil.rmtree(db_path, ignore_errors=True)


if __name__ == "__main__":
    test_buffer_records()
    test_scan_matches()
    print("\n✅ All MarbleDB join tests passed!")

