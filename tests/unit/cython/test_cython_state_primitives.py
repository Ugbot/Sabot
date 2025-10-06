#!/usr/bin/env python3
"""
Test Cython State Primitives

Tests the Flink-compatible state primitives implemented in Cython.
"""

import asyncio
import tempfile
import os
import sys

# Add sabot to path
sys.path.insert(0, os.path.dirname(__file__))

from sabot.stores.rocksdb_fallback import RocksDBBackend
from sabot.stores.base import StoreBackendConfig


async def test_value_state():
    """Test ValueState primitive."""
    print("🧪 Testing ValueState...")

    with tempfile.TemporaryDirectory() as temp_dir:
        config = StoreBackendConfig(path=temp_dir)
        backend = RocksDBBackend(config)

        try:
            await backend.start()

            # Create ValueState
            from sabot._cython.state.value_state import ValueState
            value_state = ValueState(backend, "test_value")

            # Set current key
            backend.set_current_key("ns", "key1")

            # Test basic operations
            value_state.update(42)
            result = value_state.value()
            assert result == 42, f"Expected 42, got {result}"

            # Test default value
            value_state.clear()
            result = value_state.value()
            assert result is None, f"Expected None, got {result}"

            # Test TTL
            value_state_ttl = ValueState(backend, "test_value_ttl", ttl_enabled=True, ttl_ms=100)
            value_state_ttl.update(100)
            result = value_state_ttl.value()
            assert result == 100, f"Expected 100, got {result}"

            print("✅ ValueState tests passed!")

        finally:
            await backend.stop()


async def test_list_state():
    """Test ListState primitive."""
    print("🧪 Testing ListState...")

    with tempfile.TemporaryDirectory() as temp_dir:
        config = StoreBackendConfig(path=temp_dir)
        backend = RocksDBBackend(config)

        try:
            await backend.start()

            # Create ListState
            from sabot._cython.state.list_state import ListState
            list_state = ListState(backend, "test_list")

            # Set current key
            backend.set_current_key("ns", "key1")

            # Test basic operations
            list_state.add("item1")
            list_state.add("item2")
            result = list_state.get()
            assert len(result) == 2, f"Expected 2 items, got {len(result)}"
            assert "item1" in result and "item2" in result

            # Test contains
            assert list_state.contains("item1")
            assert not list_state.contains("item3")

            # Test remove
            list_state.remove("item1")
            result = list_state.get()
            assert len(result) == 1, f"Expected 1 item after remove, got {len(result)}"
            assert "item2" in result

            # Test clear
            list_state.clear()
            assert list_state.is_empty()

            print("✅ ListState tests passed!")

        finally:
            await backend.stop()


async def test_map_state():
    """Test MapState primitive."""
    print("🧪 Testing MapState...")

    with tempfile.TemporaryDirectory() as temp_dir:
        config = StoreBackendConfig(path=temp_dir)
        backend = RocksDBBackend(config)

        try:
            await backend.start()

            # Create MapState
            from sabot._cython.state.map_state import MapState
            map_state = MapState(backend, "test_map")

            # Set current key
            backend.set_current_key("ns", "key1")

            # Test basic operations
            map_state.put("key1", "value1")
            map_state.put("key2", "value2")
            result = map_state.get("key1")
            assert result == "value1", f"Expected 'value1', got {result}"

            # Test contains
            assert map_state.contains("key1")
            assert not map_state.contains("key3")

            # Test get_all
            all_items = map_state.get_all()
            assert len(all_items) == 2, f"Expected 2 items, got {len(all_items)}"
            assert all_items["key1"] == "value1"
            assert all_items["key2"] == "value2"

            # Test keys/values
            keys = map_state.keys()
            assert len(keys) == 2 and "key1" in keys and "key2" in keys

            values = map_state.values()
            assert len(values) == 2 and "value1" in values and "value2" in values

            # Test remove
            map_state.remove("key1")
            assert not map_state.contains("key1")
            assert map_state.size() == 1

            # Test clear
            map_state.clear()
            assert map_state.is_empty()

            print("✅ MapState tests passed!")

        finally:
            await backend.stop()


async def test_reducing_state():
    """Test ReducingState primitive."""
    print("🧪 Testing ReducingState...")

    with tempfile.TemporaryDirectory() as temp_dir:
        config = StoreBackendConfig(path=temp_dir)
        backend = RocksDBBackend(config)

        try:
            await backend.start()

            # Create ReducingState with sum reducer
            def sum_reducer(a, b):
                return a + b

            from sabot._cython.state.reducing_state import ReducingState
            reducing_state = ReducingState(backend, "test_reducing", sum_reducer)

            # Set current key
            backend.set_current_key("ns", "key1")

            # Test basic operations
            reducing_state.add(10)
            reducing_state.add(20)
            result = reducing_state.get()
            assert result == 30, f"Expected 30, got {result}"

            reducing_state.add(5)
            result = reducing_state.get()
            assert result == 35, f"Expected 35, got {result}"

            # Test clear
            reducing_state.clear()
            assert reducing_state.is_empty()

            print("✅ ReducingState tests passed!")

        finally:
            await backend.stop()


async def test_aggregating_state():
    """Test AggregatingState primitive."""
    print("🧪 Testing AggregatingState...")

    with tempfile.TemporaryDirectory() as temp_dir:
        config = StoreBackendConfig(path=temp_dir)
        backend = RocksDBBackend(config)

        try:
            await backend.start()

            # Create AggregatingState (count + sum)
            def add_count(value, accumulator=None):
                if accumulator is None:
                    return {"count": 1, "sum": value}
                else:
                    return {"count": accumulator["count"] + 1, "sum": accumulator["sum"] + value}

            def aggregate_count(accumulator):
                return f"Count: {accumulator['count']}, Sum: {accumulator['sum']}"

            from sabot._cython.state.aggregating_state import AggregatingState
            agg_state = AggregatingState(backend, "test_agg", add_count, aggregate_count)

            # Set current key
            backend.set_current_key("ns", "key1")

            # Test basic operations
            agg_state.add(10)
            agg_state.add(20)
            result = agg_state.get()
            assert "Count: 2" in result and "Sum: 30" in result, f"Unexpected result: {result}"

            agg_state.add(5)
            result = agg_state.get()
            assert "Count: 3" in result and "Sum: 35" in result, f"Unexpected result: {result}"

            # Test get_accumulator
            accumulator = agg_state.get_accumulator()
            assert accumulator["count"] == 3 and accumulator["sum"] == 35

            # Test clear
            agg_state.clear()
            assert agg_state.is_empty()

            print("✅ AggregatingState tests passed!")

        finally:
            await backend.stop()


async def test_state_key_scoping():
    """Test state key scoping across different keys."""
    print("🧪 Testing state key scoping...")

    with tempfile.TemporaryDirectory() as temp_dir:
        config = StoreBackendConfig(path=temp_dir)
        backend = RocksDBBackend(config)

        try:
            await backend.start()

            # Create states
            from sabot._cython.state.value_state import ValueState
            from sabot._cython.state.list_state import ListState

            value_state = ValueState(backend, "shared_value")
            list_state = ListState(backend, "shared_list")

            # Test key1
            backend.set_current_key("ns", "key1")
            value_state.update(100)
            list_state.add("item1")

            result = value_state.value()
            assert result == 100, f"Key1 value should be 100, got {result}"

            lst = list_state.get()
            assert len(lst) == 1 and "item1" in lst

            # Test key2 (should be isolated)
            backend.set_current_key("ns", "key2")
            result = value_state.value()
            assert result is None, f"Key2 should have no value, got {result}"

            assert list_state.is_empty(), "Key2 should have empty list"

            # Set different values for key2
            value_state.update(200)
            list_state.add("item2")

            result = value_state.value()
            assert result == 200, f"Key2 value should be 200, got {result}"

            lst = list_state.get()
            assert len(lst) == 1 and "item2" in lst

            # Switch back to key1 - should have original values
            backend.set_current_key("ns", "key1")
            result = value_state.value()
            assert result == 100, f"Key1 should still have 100, got {result}"

            lst = list_state.get()
            assert len(lst) == 1 and "item1" in lst

            print("✅ State key scoping tests passed!")

        finally:
            await backend.stop()


async def main():
    """Run all tests."""
    print("🚀 Cython State Primitives Tests")
    print("=" * 50)

    try:
        await test_value_state()
        await test_list_state()
        await test_map_state()
        await test_reducing_state()
        await test_aggregating_state()
        await test_state_key_scoping()

        print("\n🎉 All Cython state primitive tests passed!")
        print("✅ Flink-compatible state management is working")

    except Exception as e:
        print(f"\n❌ Tests failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
