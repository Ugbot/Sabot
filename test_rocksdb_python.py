#!/usr/bin/env python3
"""
Test RocksDB Python Implementation

Tests the Python RocksDB backend implementation with SQLite fallback.
"""

import asyncio
import tempfile
import os
import sys

# Add sabot to path
sys.path.insert(0, os.path.dirname(__file__))

from sabot.stores.rocksdb_fallback import RocksDBBackend
from sabot.stores.base import StoreBackendConfig


async def test_rocksdb_backend():
    """Test the RocksDB backend implementation."""
    print("🧪 Testing RocksDB Backend...")

    with tempfile.TemporaryDirectory() as temp_dir:
        db_path = os.path.join(temp_dir, "test_backend")

        # Create backend
        config = StoreBackendConfig(path=temp_dir)
        backend = RocksDBBackend(config)

        try:
            # Test opening
            print("  Opening database...")
            await backend.start()

            # Test set/get
            print("  Testing set/get operations...")
            await backend.set("test_key", {"name": "test", "value": 42})
            result = await backend.get("test_key")
            assert result == {"name": "test", "value": 42}, f"Expected dict, got {result}"
            print("    ✅ Set/get works")

            # Test exists
            assert await backend.exists("test_key"), "Key should exist"
            assert not await backend.exists("nonexistent"), "Nonexistent key should not exist"
            print("    ✅ Exists check works")

            # Test delete
            deleted = await backend.delete("test_key")
            assert deleted, "Delete should return True for existing key"
            assert not await backend.exists("test_key"), "Key should not exist after delete"
            print("    ✅ Delete works")

            # Test batch operations
            await backend.batch_set({"batch_key1": "value1", "batch_key2": "value2"})
            result1 = await backend.get("batch_key1")
            result2 = await backend.get("batch_key2")
            assert result1 == "value1" and result2 == "value2", "Batch set failed"
            print("    ✅ Batch operations work")

            # Test stats
            stats = await backend.get_stats()
            print(f"    📊 Stats: {stats}")

            print("✅ All RocksDB backend tests passed!")

        finally:
            await backend.stop()


async def main():
    """Run all tests."""
    print("🚀 RocksDB Python Backend Tests")
    print("=" * 40)

    try:
        await test_rocksdb_backend()

        print("\n🎉 All tests completed successfully!")
        print("✅ RocksDB Python backend is working correctly")

    except Exception as e:
        print(f"\n❌ Tests failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
