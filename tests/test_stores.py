# -*- coding: utf-8 -*-
"""Tests for pluggable store backends."""

import pytest
import asyncio
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

from sabot.stores.base import StoreBackend, StoreBackendConfig, create_backend_from_string
from sabot.stores.memory import MemoryBackend
from sabot.stores.arrow_files import ArrowFileBackend


class TestStoreBackendBase:
    """Test the base store backend interface."""

    def test_backend_config(self):
        """Test StoreBackendConfig creation."""
        config = StoreBackendConfig(
            backend_type="memory",
            max_size=1000,
            ttl_seconds=300,
            compression="gzip",
            options={"custom": "value"}
        )

        assert config.backend_type == "memory"
        assert config.max_size == 1000
        assert config.ttl_seconds == 300.0
        assert config.compression == "gzip"
        assert config.options == {"custom": "value"}

    def test_create_backend_from_string(self):
        """Test backend creation from string specifications."""
        # Memory backend
        backend = create_backend_from_string("memory://")
        assert isinstance(backend, MemoryBackend)

        # Arrow files backend with path
        data_dir = Path("./test_data")
        backend = create_backend_from_string(f"arrow_files://{data_dir}")
        assert isinstance(backend, ArrowFileBackend)
        assert backend.config.path == data_dir

        # Backend with just type
        backend = create_backend_from_string("memory")
        assert isinstance(backend, MemoryBackend)


class TestMemoryBackend:
    """Test the memory backend implementation."""

    @pytest.fixture
    async def backend(self):
        """Create a memory backend for testing."""
        config = StoreBackendConfig(backend_type="memory")
        backend = MemoryBackend(config)
        await backend.start()
        yield backend
        await backend.stop()

    @pytest.mark.asyncio
    async def test_memory_operations(self, backend):
        """Test basic memory backend operations."""
        # Test set/get
        await backend.set("key1", "value1")
        value = await backend.get("key1")
        assert value == "value1"

        # Test exists
        assert await backend.exists("key1")
        assert not await backend.exists("nonexistent")

        # Test delete
        assert await backend.delete("key1")
        assert not await backend.exists("key1")
        assert not await backend.delete("nonexistent")

    @pytest.mark.asyncio
    async def test_memory_batch_operations(self, backend):
        """Test batch operations."""
        # Batch set
        items = {"a": 1, "b": 2, "c": 3}
        await backend.batch_set(items)

        # Verify all items
        for key, expected_value in items.items():
            value = await backend.get(key)
            assert value == expected_value

        # Batch delete
        deleted = await backend.batch_delete(["a", "b"])
        assert deleted == 2
        assert not await backend.exists("a")
        assert not await backend.exists("b")
        assert await backend.exists("c")

    @pytest.mark.asyncio
    async def test_memory_keys_values_items(self, backend):
        """Test keys/values/items operations."""
        # Set up test data
        test_data = {"x": 10, "y": 20, "z": 30}
        await backend.batch_set(test_data)

        # Test keys
        keys = await backend.keys()
        assert set(keys) == {"x", "y", "z"}

        # Test keys with prefix
        await backend.set("xyz", 40)
        xyz_keys = await backend.keys(prefix="xy")
        assert xyz_keys == ["xyz"]

        # Test values
        values = await backend.values()
        assert set(values) == {10, 20, 30, 40}

        # Test items
        items = await backend.items()
        items_dict = dict(items)
        assert items_dict == {"x": 10, "y": 20, "z": 30, "xyz": 40}

        # Test items with prefix
        prefixed_items = await backend.items(prefix="x")
        assert dict(prefixed_items) == {"x": 10, "xyz": 40}

    @pytest.mark.asyncio
    async def test_memory_size_and_clear(self, backend):
        """Test size and clear operations."""
        # Initially empty
        assert await backend.size() == 0

        # Add items
        await backend.batch_set({"a": 1, "b": 2, "c": 3})
        assert await backend.size() == 3

        # Clear
        await backend.clear()
        assert await backend.size() == 0
        assert not await backend.exists("a")

    @pytest.mark.asyncio
    async def test_memory_scan(self, backend):
        """Test scan/range operations."""
        # Set up sorted test data
        test_data = {f"key_{i:02d}": i for i in range(10)}
        await backend.batch_set(test_data)

        # Scan all
        all_items = list(await backend.scan())
        assert len(all_items) == 10

        # Scan with range
        range_items = list(await backend.scan(start_key="key_03", end_key="key_07"))
        keys = [k for k, v in range_items]
        assert keys == ["key_03", "key_04", "key_05", "key_06"]

        # Scan with limit
        limited_items = list(await backend.scan(limit=3))
        assert len(limited_items) == 3

        # Scan with prefix
        await backend.set("prefix_a", 100)
        await backend.set("prefix_b", 200)
        await backend.set("other", 300)

        prefix_items = list(await backend.scan(prefix="prefix"))
        assert len(prefix_items) == 2
        keys = [k for k, v in prefix_items]
        assert set(keys) == {"prefix_a", "prefix_b"}

    @pytest.mark.asyncio
    async def test_memory_stats(self, backend):
        """Test backend statistics."""
        # Add some data
        await backend.batch_set({"a": 1, "b": "hello", "c": [1, 2, 3]})

        stats = await backend.get_stats()
        assert stats["backend_type"] == "memory"
        assert stats["size"] == 3
        assert "memory_usage_bytes" in stats
        assert "max_size" in stats
        assert "ttl_seconds" in stats


class TestArrowFileBackend:
    """Test the Arrow files backend implementation."""

    @pytest.fixture
    async def backend(self, tmp_path):
        """Create an Arrow files backend for testing."""
        config = StoreBackendConfig(
            backend_type="arrow_files",
            path=tmp_path / "test_data"
        )
        backend = ArrowFileBackend(config)
        await backend.start()
        yield backend
        await backend.stop()

    @pytest.mark.asyncio
    async def test_arrow_file_operations(self, backend):
        """Test basic Arrow files backend operations."""
        # Test set/get
        test_data = {"name": "Alice", "age": 30, "city": "NYC"}
        await backend.set("user1", test_data)

        value = await backend.get("user1")
        assert value == test_data

        # Test persistence across "restarts"
        await backend._save_data()  # Force save

        # Simulate restart by clearing in-memory cache and reloading
        backend._data.clear()
        backend._loaded = False
        await backend._load_data()

        # Data should still be there
        value = await backend.get("user1")
        assert value == test_data

    @pytest.mark.asyncio
    async def test_arrow_file_stats(self, backend):
        """Test Arrow files backend statistics."""
        await backend.set("test", {"data": "value"})

        stats = await backend.get_stats()
        assert stats["backend_type"] == "arrow_files"
        assert stats["size"] == 1
        assert "data_path" in stats
        assert "file_size_bytes" in stats
        assert "loaded" in stats


class TestCythonBackends:
    """Test Cython-optimized backends when available."""

    @pytest.mark.asyncio
    async def test_cython_memory_fallback(self):
        """Test that Cython backends fall back gracefully."""
        # This test ensures the import system works even if Cython isn't compiled
        from sabot.stores import create_backend_auto, CYTHON_AVAILABLE

        # Try to create a backend with Cython preference
        backend = create_backend_auto("memory://", use_cython=True)

        # Should work regardless of Cython availability
        assert backend is not None

        # Should be able to start/stop
        await backend.start()
        await backend.stop()

        # Basic operations should work
        await backend.set("test", "value")
        value = await backend.get("test")
        assert value == "value"


if __name__ == "__main__":
    pytest.main([__file__])
