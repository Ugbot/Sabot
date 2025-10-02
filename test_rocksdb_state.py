#!/usr/bin/env python3
"""
Test RocksDB State Backend Implementation

Tests the Cython RocksDB state backend integration with python-rocksdb.
"""

import asyncio
import tempfile
import os
import sys

# Add sabot to path
sys.path.insert(0, os.path.dirname(__file__))

from sabot._cython.state.rocksdb_state import RocksDBStateBackend


async def test_rocksdb_state_backend():
    """Test basic RocksDB state backend functionality."""
    print("🧪 Testing RocksDB State Backend...")

    # Create temporary directory for test
    with tempfile.TemporaryDirectory() as temp_dir:
        db_path = os.path.join(temp_dir, "test_rocksdb")

        # Create backend
        backend = RocksDBStateBackend(db_path)

        try:
            # Test opening
            print("  Opening database...")
            backend.open()

            # Test setting current key context
            backend.set_current_key("test_namespace", "test_key")

            # Test ValueState operations
            print("  Testing ValueState operations...")
            backend.put_value("counter", 42)
            value = backend.get_value("counter")
            assert value == 42, f"Expected 42, got {value}"
            print("    ✅ ValueState get/set works")

            # Test ListState operations
            print("  Testing ListState operations...")
            backend.add_to_list("items", "item1")
            backend.add_to_list("items", "item2")
            items = backend.get_list("items")
            assert len(items) == 2, f"Expected 2 items, got {len(items)}"
            assert "item1" in items and "item2" in items
            print("    ✅ ListState add/get works")

            # Test MapState operations
            print("  Testing MapState operations...")
            backend.put_to_map("config", "timeout", 30)
            backend.put_to_map("config", "retries", 3)
            timeout = backend.get_from_map("config", "timeout")
            assert timeout == 30, f"Expected 30, got {timeout}"
            print("    ✅ MapState put/get works")

            # Test clearing
            backend.clear_value("counter")
            cleared_value = backend.get_value("counter")
            assert cleared_value is None, f"Expected None, got {cleared_value}"
            print("    ✅ Clear operations work")

            # Test statistics
            stats = backend.get_stats()
            print(f"    📊 Stats: {stats}")

            print("✅ All RocksDB state backend tests passed!")

        finally:
            backend.close()


async def test_value_state_integration():
    """Test ValueState class integration."""
    print("🧪 Testing ValueState Integration...")

    # Import ValueState
    from sabot._cython.state.value_state import ValueState
    from sabot._cython.state.rocksdb_state import RocksDBStateBackend

    with tempfile.TemporaryDirectory() as temp_dir:
        db_path = os.path.join(temp_dir, "test_value_state")

        # Create backend and value state
        backend = RocksDBStateBackend(db_path)
        backend.open()

        try:
            value_state = ValueState(backend, "test_value", default_value=0)

            # Set current key
            backend.set_current_key("ns", "key1")

            # Test value operations
            value_state.update(100)
            retrieved = value_state.value()
            assert retrieved == 100, f"Expected 100, got {retrieved}"

            # Test with different key
            backend.set_current_key("ns", "key2")
            value_state.update(200)
            retrieved2 = value_state.value()
            assert retrieved2 == 200, f"Expected 200, got {retrieved2}"

            # Test default value
            value_state.clear()
            default_val = value_state.value()
            assert default_val == 0, f"Expected default 0, got {default_val}"

            print("✅ ValueState integration tests passed!")

        finally:
            backend.close()


async def main():
    """Run all tests."""
    print("🚀 RocksDB State Backend Tests")
    print("=" * 40)

    try:
        await test_rocksdb_state_backend()
        await test_value_state_integration()

        print("\n🎉 All tests completed successfully!")
        print("✅ RocksDB state backend is working correctly")

    except Exception as e:
        print(f"\n❌ Tests failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
