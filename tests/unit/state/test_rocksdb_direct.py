#!/usr/bin/env python3
"""
Direct Test of RocksDB State Backend

Tests the RocksDB state backend without full package imports.
"""

import asyncio
import tempfile
import os
import sys

# Add the rocksdb submodule to path first
rocksdb_path = os.path.join(os.path.dirname(__file__), 'third-party', 'python-rocksdb')
sys.path.insert(0, rocksdb_path)

# Test if rocksdb is available
try:
    import rocksdb
    ROCKSDB_AVAILABLE = True
    print("✅ RocksDB library available")
except ImportError:
    ROCKSDB_AVAILABLE = False
    print("⚠️  RocksDB library not available - using SQLite fallback")

# Now add sabot to path
sys.path.insert(0, os.path.dirname(__file__))

# Import the backend directly
from sabot._cython.state.rocksdb_state import RocksDBStateBackend


async def test_rocksdb_basic():
    """Test basic RocksDB operations."""
    print("🧪 Testing basic RocksDB operations...")

    with tempfile.TemporaryDirectory() as temp_dir:
        db_path = os.path.join(temp_dir, "test_basic")

        # Test raw rocksdb operations
        opts = rocksdb.Options()
        opts.create_if_missing = True

        db = rocksdb.DB(db_path, opts)

        # Basic put/get
        db.put(b'test_key', b'test_value')
        value = db.get(b'test_key')
        assert value == b'test_value', f"Expected b'test_value', got {value}"

        db.close()
        print("✅ Basic RocksDB operations work")


async def test_state_backend():
    """Test our state backend implementation."""
    print("🧪 Testing RocksDB State Backend...")

    with tempfile.TemporaryDirectory() as temp_dir:
        db_path = os.path.join(temp_dir, "test_backend")

        backend = RocksDBStateBackend(db_path)

        try:
            # Test opening
            backend.open()
            print("✅ Backend opened successfully")

            # Test setting key context
            backend.set_current_key("test_ns", "test_key")

            # Test ValueState operations
            backend.put_value("counter", 42)
            value = backend.get_value("counter")
            assert value == 42, f"Expected 42, got {value}"
            print("✅ ValueState operations work")

            # Test ListState operations
            backend.add_to_list("items", "item1")
            backend.add_to_list("items", "item2")
            items = backend.get_list("items")
            assert len(items) == 2, f"Expected 2 items, got {len(items)}"
            print("✅ ListState operations work")

            # Test MapState operations
            backend.put_to_map("config", "timeout", 30)
            timeout = backend.get_from_map("config", "timeout")
            assert timeout == 30, f"Expected 30, got {timeout}"
            print("✅ MapState operations work")

            # Test clearing
            backend.clear_value("counter")
            cleared = backend.get_value("counter")
            assert cleared is None, f"Expected None, got {cleared}"
            print("✅ Clear operations work")

        finally:
            backend.close()
            print("✅ Backend closed successfully")


async def main():
    """Run all tests."""
    print("🚀 Direct RocksDB State Backend Tests")
    print("=" * 40)

    try:
        await test_rocksdb_basic()
        await test_state_backend()

        print("\n🎉 All tests passed!")
        print("✅ RocksDB state backend implementation is working")

    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
