#!/usr/bin/env python3
"""
Test Tonbo State Backend

Comprehensive test suite for the Cython Tonbo state backend implementation.
Verifies Flink-compatible state primitives with columnar storage capabilities.
"""

import asyncio
import tempfile
import shutil
from pathlib import Path
import sys

# Add sabot to path
sys.path.insert(0, str(Path(__file__).parent))

import pytest


class TestTonboStateBackend:
    """Test suite for Tonbo state backend."""

    def setup_method(self):
        """Set up test environment."""
        self.temp_dir = tempfile.mkdtemp(prefix="sabot_tonbo_test_")
        self.db_path = Path(self.temp_dir) / "test_db"

    def teardown_method(self):
        """Clean up test environment."""
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    @pytest.mark.asyncio
    async def test_tonbo_backend_initialization(self):
        """Test Tonbo backend initialization."""
        try:
            from sabot._cython.state.tonbo_state import TonboStateBackend
            from sabot.stores.tonbo import TonboBackend
            from sabot.stores.base import StoreBackendConfig

            # Create Tonbo store backend
            config = StoreBackendConfig(path=str(self.db_path))
            tonbo_store = TonboBackend(config)

            # Initialize store
            await tonbo_store.start()

            # Create state backend
            state_backend = TonboStateBackend()
            state_backend.initialize(tonbo_store)

            assert state_backend._initialized == True
            assert state_backend.get_current_namespace() == "default"

            # Clean up
            await tonbo_store.stop()

        except ImportError:
            pytest.skip("Tonbo state backend not available")

    @pytest.mark.asyncio
    async def test_value_state_operations(self):
        """Test ValueState operations."""
        try:
            from sabot._cython.state.tonbo_state import TonboStateBackend
            from sabot.stores.tonbo import TonboBackend
            from sabot.stores.base import StoreBackendConfig

            # Set up backends
            config = StoreBackendConfig(path=str(self.db_path))
            tonbo_store = TonboBackend(config)
            await tonbo_store.start()

            state_backend = TonboStateBackend()
            state_backend.initialize(tonbo_store)
            state_backend.set_current_namespace("test_namespace")

            # Get ValueState
            value_state = state_backend.get_value_state("test_value")

            # Test operations
            value_state.set_current_key("test_namespace", "key1")

            # Initially None
            assert value_state.get() is None

            # Update and get
            value_state.update("test_value")
            assert value_state.get() == "test_value"

            # Update again
            value_state.update(42)
            assert value_state.get() == 42

            # Clear
            value_state.clear()
            assert value_state.get() is None

            await tonbo_store.stop()

        except ImportError:
            pytest.skip("Tonbo state backend not available")

    @pytest.mark.asyncio
    async def test_list_state_operations(self):
        """Test ListState operations."""
        try:
            from sabot._cython.state.tonbo_state import TonboStateBackend
            from sabot.stores.tonbo import TonboBackend
            from sabot.stores.base import StoreBackendConfig

            # Set up backends
            config = StoreBackendConfig(path=str(self.db_path))
            tonbo_store = TonboBackend(config)
            await tonbo_store.start()

            state_backend = TonboStateBackend()
            state_backend.initialize(tonbo_store)
            state_backend.set_current_namespace("test_namespace")

            # Get ListState
            list_state = state_backend.get_list_state("test_list")

            # Test operations
            list_state.set_current_key("test_namespace", "key1")

            # Initially empty
            assert list_state.get() == []

            # Add items
            list_state.add("item1")
            list_state.add("item2")
            list_state.add(42)

            current_list = list_state.get()
            assert len(current_list) == 3
            assert "item1" in current_list
            assert "item2" in current_list
            assert 42 in current_list

            # Update entire list
            list_state.update(["new_item1", "new_item2"])
            assert list_state.get() == ["new_item1", "new_item2"]

            # Clear
            list_state.clear()
            assert list_state.get() == []

            await tonbo_store.stop()

        except ImportError:
            pytest.skip("Tonbo state backend not available")

    @pytest.mark.asyncio
    async def test_map_state_operations(self):
        """Test MapState operations."""
        try:
            from sabot._cython.state.tonbo_state import TonboStateBackend
            from sabot.stores.tonbo import TonboBackend
            from sabot.stores.base import StoreBackendConfig

            # Set up backends
            config = StoreBackendConfig(path=str(self.db_path))
            tonbo_store = TonboBackend(config)
            await tonbo_store.start()

            state_backend = TonboStateBackend()
            state_backend.initialize(tonbo_store)
            state_backend.set_current_namespace("test_namespace")

            # Get MapState
            map_state = state_backend.get_map_state("test_map")

            # Test operations
            map_state.set_current_key("test_namespace", "key1")

            # Initially empty
            assert map_state.is_empty() == True
            assert map_state.keys() == []
            assert map_state.values() == []
            assert map_state.entries() == []

            # Put items
            map_state.put("k1", "v1")
            map_state.put("k2", 42)
            map_state.put("k3", [1, 2, 3])

            # Test individual access
            assert map_state.get("k1") == "v1"
            assert map_state.get("k2") == 42
            assert map_state.get("k3") == [1, 2, 3]
            assert map_state.contains("k1") == True
            assert map_state.contains("nonexistent") == False

            # Test bulk access
            keys = map_state.keys()
            values = map_state.values()
            entries = map_state.entries()

            assert len(keys) == 3
            assert len(values) == 3
            assert len(entries) == 3

            assert "k1" in keys
            assert "k2" in keys
            assert "k3" in keys

            assert "v1" in values
            assert 42 in values
            assert [1, 2, 3] in values

            # Remove item
            assert map_state.remove("k2") == True
            assert map_state.contains("k2") == False
            assert len(map_state.keys()) == 2

            # Clear all
            map_state.clear()
            assert map_state.is_empty() == True

            await tonbo_store.stop()

        except ImportError:
            pytest.skip("Tonbo state backend not available")

    @pytest.mark.asyncio
    async def test_reducing_state_operations(self):
        """Test ReducingState operations."""
        try:
            from sabot._cython.state.tonbo_state import TonboStateBackend
            from sabot.stores.tonbo import TonboBackend
            from sabot.stores.base import StoreBackendConfig

            # Set up backends
            config = StoreBackendConfig(path=str(self.db_path))
            tonbo_store = TonboBackend(config)
            await tonbo_store.start()

            state_backend = TonboStateBackend()
            state_backend.initialize(tonbo_store)
            state_backend.set_current_namespace("test_namespace")

            # Sum reducer function
            def sum_reducer(a, b):
                return a + b

            # Get ReducingState
            reducing_state = state_backend.get_reducing_state("test_reducing", sum_reducer)

            # Test operations
            reducing_state.set_current_key("test_namespace", "key1")

            # Initially None
            assert reducing_state.get() is None

            # Add values (they should be summed)
            reducing_state.add(10)
            assert reducing_state.get() == 10

            reducing_state.add(20)
            assert reducing_state.get() == 30

            reducing_state.add(5)
            assert reducing_state.get() == 35

            # Clear
            reducing_state.clear()
            assert reducing_state.get() is None

            await tonbo_store.stop()

        except ImportError:
            pytest.skip("Tonbo state backend not available")

    @pytest.mark.asyncio
    async def test_aggregating_state_operations(self):
        """Test AggregatingState operations."""
        try:
            from sabot._cython.state.tonbo_state import TonboStateBackend
            from sabot.stores.tonbo import TonboBackend
            from sabot.stores.base import StoreBackendConfig

            # Set up backends
            config = StoreBackendConfig(path=str(self.db_path))
            tonbo_store = TonboBackend(config)
            await tonbo_store.start()

            state_backend = TonboStateBackend()
            state_backend.initialize(tonbo_store)
            state_backend.set_current_namespace("test_namespace")

            # Running average: (sum, count) -> average
            def add_to_avg(accumulator, value):
                if accumulator is None:
                    return (value, 1)
                sum_val, count = accumulator
                return (sum_val + value, count + 1)

            def get_avg_result(accumulator):
                if accumulator is None:
                    return None
                sum_val, count = accumulator
                return sum_val / count

            # Get AggregatingState
            agg_state = state_backend.get_aggregating_state(
                "test_agg", add_to_avg, get_avg_result
            )

            # Test operations
            agg_state.set_current_key("test_namespace", "key1")

            # Initially None
            assert agg_state.get() is None

            # Add values (running average)
            agg_state.add(10)
            assert agg_state.get() == 10.0

            agg_state.add(20)
            assert agg_state.get() == 15.0

            agg_state.add(30)
            assert agg_state.get() == 20.0

            # Clear
            agg_state.clear()
            assert agg_state.get() is None

            await tonbo_store.stop()

        except ImportError:
            pytest.skip("Tonbo state backend not available")

    @pytest.mark.asyncio
    async def test_arrow_operations(self):
        """Test Arrow columnar operations on state data."""
        try:
            from sabot._cython.state.tonbo_state import TonboStateBackend
            from sabot.stores.tonbo import TonboBackend
            from sabot.stores.base import StoreBackendConfig

            # Set up backends
            config = StoreBackendConfig(path=str(self.db_path))
            tonbo_store = TonboBackend(config)
            await tonbo_store.start()

            state_backend = TonboStateBackend()
            state_backend.initialize(tonbo_store)
            state_backend.set_current_namespace("test_namespace")

            # Check if Arrow operations are available
            if not state_backend._arrow_store:
                pytest.skip("Arrow integration not available")

            # Create some test state data
            value_state = state_backend.get_value_state("test_values")
            for i in range(10):
                value_state.set_current_key("test_namespace", f"key_{i}")
                value_state.update({"id": i, "value": i * 10, "category": f"cat_{i % 3}"})

            # Test Arrow scan
            table = state_backend.arrow_scan_state("test_values", limit=5)
            if table is not None:
                assert table.num_rows <= 5
                # Table should have columns for the stored data

            # Test Arrow aggregation
            agg_result = state_backend.arrow_aggregate_state(
                "test_values",
                ["category"],
                {"value": "sum"}
            )
            if agg_result is not None:
                # Should have aggregated results
                pass

            await tonbo_store.stop()

        except ImportError:
            pytest.skip("Arrow operations not available")

    @pytest.mark.asyncio
    async def test_backend_stats(self):
        """Test backend statistics reporting."""
        try:
            from sabot._cython.state.tonbo_state import TonboStateBackend
            from sabot.stores.tonbo import TonboBackend
            from sabot.stores.base import StoreBackendConfig

            # Set up backends
            config = StoreBackendConfig(path=str(self.db_path))
            tonbo_store = TonboBackend(config)
            await tonbo_store.start()

            state_backend = TonboStateBackend()
            state_backend.initialize(tonbo_store)
            state_backend.set_current_namespace("stats_test")

            # Get stats
            stats = state_backend.get_backend_stats()

            assert stats['backend_type'] == 'tonbo_arrow'
            assert stats['initialized'] == True
            assert stats['current_namespace'] == 'stats_test'
            assert 'arrow_integration' in stats

            await tonbo_store.stop()

        except ImportError:
            pytest.skip("Tonbo state backend not available")

    @pytest.mark.asyncio
    async def test_namespace_isolation(self):
        """Test namespace isolation between different contexts."""
        try:
            from sabot._cython.state.tonbo_state import TonboStateBackend
            from sabot.stores.tonbo import TonboBackend
            from sabot.stores.base import StoreBackendConfig

            # Set up backends
            config = StoreBackendConfig(path=str(self.db_path))
            tonbo_store = TonboBackend(config)
            await tonbo_store.start()

            state_backend = TonboStateBackend()
            state_backend.initialize(tonbo_store)

            # Test namespace switching
            state_backend.set_current_namespace("ns1")
            assert state_backend.get_current_namespace() == "ns1"

            value_state = state_backend.get_value_state("test_value")

            # Set value in ns1
            value_state.set_current_key("ns1", "shared_key")
            value_state.update("value_in_ns1")
            assert value_state.get() == "value_in_ns1"

            # Switch namespace
            state_backend.set_current_namespace("ns2")
            assert state_backend.get_current_namespace() == "ns2"

            # Same key in different namespace should be isolated
            value_state.set_current_key("ns2", "shared_key")
            assert value_state.get() is None  # No value in ns2 yet

            value_state.update("value_in_ns2")
            assert value_state.get() == "value_in_ns2"

            # Switch back to ns1 - should see original value
            state_backend.set_current_namespace("ns1")
            value_state.set_current_key("ns1", "shared_key")
            assert value_state.get() == "value_in_ns1"

            await tonbo_store.stop()

        except ImportError:
            pytest.skip("Tonbo state backend not available")


if __name__ == "__main__":
    # Run tests directly
    import asyncio

    async def run_tests():
        test_instance = TestTonboStateBackend()

        # Run individual tests
        try:
            print("Setting up test environment...")
            test_instance.setup_method()

            print("Testing backend initialization...")
            await test_instance.test_tonbo_backend_initialization()

            print("Testing ValueState operations...")
            await test_instance.test_value_state_operations()

            print("Testing ListState operations...")
            await test_instance.test_list_state_operations()

            print("Testing MapState operations...")
            await test_instance.test_map_state_operations()

            print("Testing ReducingState operations...")
            await test_instance.test_reducing_state_operations()

            print("Testing AggregatingState operations...")
            await test_instance.test_aggregating_state_operations()

            print("Testing namespace isolation...")
            await test_instance.test_namespace_isolation()

            print("Testing backend stats...")
            await test_instance.test_backend_stats()

            print("✅ All Tonbo state backend tests passed!")

        except Exception as e:
            print(f"❌ Test failed: {e}")
            import traceback
            traceback.print_exc()
        finally:
            test_instance.teardown_method()

    asyncio.run(run_tests())
