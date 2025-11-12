# -*- coding: utf-8 -*-
# cython: language_level=3
"""
TDD Tests for MarbleDB State Backend

Tests for point API operations: Get, Put, MultiGet, DeleteRange, NewIterator, Checkpoint/Restore
"""

import tempfile
import shutil
import os

from sabot._cython.state.marbledb_backend cimport MarbleDBStateBackend


def test_get_put_delete():
    """Test basic Get, Put, Delete operations."""
    cdef MarbleDBStateBackend backend
    cdef str db_path = tempfile.mkdtemp()
    
    try:
        backend = MarbleDBStateBackend(db_path)
        backend.open()
        
        # Test Put
        backend.put_raw("key1", b"value1")
        
        # Test Get
        result = backend.get_raw("key1")
        assert result == b"value1", f"Expected b'value1', got {result}"
        
        # Test Delete
        backend.delete_raw("key1")
        
        # Test Get after delete
        result = backend.get_raw("key1")
        assert result is None, f"Expected None after delete, got {result}"
        
        backend.close()
        print("✅ test_get_put_delete passed")
        
    finally:
        shutil.rmtree(db_path, ignore_errors=True)


def test_multi_get():
    """Test batch Get operations."""
    cdef MarbleDBStateBackend backend
    cdef str db_path = tempfile.mkdtemp()
    cdef list keys
    cdef dict results
    cdef str key
    
    try:
        backend = MarbleDBStateBackend(db_path)
        backend.open()
        
        # Insert multiple keys
        keys = [f"key_{i}" for i in range(100)]
        for key in keys:
            backend.put_raw(key, f"value_{key}".encode())
        
        # Test MultiGet
        results = backend.multi_get_raw(keys)
        
        assert len(results) == 100, f"Expected 100 results, got {len(results)}"
        for key in keys:
            assert results[key] is not None, f"Key {key} not found"
            assert results[key] == f"value_{key}".encode(), f"Wrong value for {key}"
        
        backend.close()
        print("✅ test_multi_get passed")
        
    finally:
        shutil.rmtree(db_path, ignore_errors=True)


def test_delete_range():
    """Test DeleteRange operation."""
    cdef MarbleDBStateBackend backend
    cdef str db_path = tempfile.mkdtemp()
    cdef str key
    
    try:
        backend = MarbleDBStateBackend(db_path)
        backend.open()
        
        # Insert keys in range
        for i in range(50):
            backend.put_raw(f"key_{i:03d}", f"value_{i}".encode())
        
        # Delete range
        backend.delete_range_raw("key_010", "key_030")
        
        # Verify deletion
        assert backend.get_raw("key_009") is not None, "key_009 should exist"
        assert backend.get_raw("key_010") is None, "key_010 should be deleted"
        assert backend.get_raw("key_020") is None, "key_020 should be deleted"
        assert backend.get_raw("key_030") is None, "key_030 should be deleted"
        assert backend.get_raw("key_031") is not None, "key_031 should exist"
        
        backend.close()
        print("✅ test_delete_range passed")
        
    finally:
        shutil.rmtree(db_path, ignore_errors=True)


def test_scan_range():
    """Test ScanRange operation."""
    cdef MarbleDBStateBackend backend
    cdef str db_path = tempfile.mkdtemp()
    cdef str key
    cdef object iterator
    cdef list results
    
    try:
        backend = MarbleDBStateBackend(db_path)
        backend.open()
        
        # Insert keys
        for i in range(100):
            backend.put_raw(f"key_{i:03d}", f"value_{i}".encode())
        
        # Scan range
        iterator = backend.scan_range("key_010", "key_030")
        results = list(iterator)
        
        assert len(results) > 0, "Should have results in range"
        for key, value in results:
            assert key >= "key_010", f"Key {key} should be >= key_010"
            assert key < "key_030", f"Key {key} should be < key_030"
        
        backend.close()
        print("✅ test_scan_range passed")
        
    finally:
        shutil.rmtree(db_path, ignore_errors=True)


def test_checkpoint_restore():
    """Test Checkpoint and Restore operations."""
    cdef MarbleDBStateBackend backend
    cdef str db_path = tempfile.mkdtemp()
    cdef str checkpoint_path
    
    try:
        backend = MarbleDBStateBackend(db_path)
        backend.open()
        
        # Insert data
        backend.put_raw("key1", b"value1")
        backend.put_raw("key2", b"value2")
        
        # Create checkpoint
        checkpoint_path = backend.checkpoint()
        assert checkpoint_path is not None, "Checkpoint path should not be None"
        assert os.path.exists(checkpoint_path), f"Checkpoint path should exist: {checkpoint_path}"
        
        # Modify data
        backend.put_raw("key1", b"modified")
        backend.put_raw("key3", b"value3")
        
        # Restore from checkpoint
        backend.restore(checkpoint_path)
        
        # Verify restored data
        assert backend.get_raw("key1") == b"value1", "key1 should be restored"
        assert backend.get_raw("key2") == b"value2", "key2 should be restored"
        assert backend.get_raw("key3") is None, "key3 should not exist after restore"
        
        backend.close()
        print("✅ test_checkpoint_restore passed")
        
    finally:
        shutil.rmtree(db_path, ignore_errors=True)


def test_exists():
    """Test Exists operation."""
    cdef MarbleDBStateBackend backend
    cdef str db_path = tempfile.mkdtemp()
    
    try:
        backend = MarbleDBStateBackend(db_path)
        backend.open()
        
        # Test non-existent key
        assert not backend.exists_raw("nonexistent"), "Key should not exist"
        
        # Insert key
        backend.put_raw("key1", b"value1")
        
        # Test existing key
        assert backend.exists_raw("key1"), "Key should exist"
        
        backend.close()
        print("✅ test_exists passed")
        
    finally:
        shutil.rmtree(db_path, ignore_errors=True)


if __name__ == "__main__":
    test_get_put_delete()
    test_multi_get()
    test_delete_range()
    test_scan_range()
    test_checkpoint_restore()
    test_exists()
    print("\n✅ All MarbleDB backend tests passed!")

