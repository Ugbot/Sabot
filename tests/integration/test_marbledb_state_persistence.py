"""
Test MarbleDB state persistence across backend restarts.

These tests verify that:
1. Raw key-value data persists across close/reopen
2. ListState data persists across close/reopen
3. MapState data persists across close/reopen

This validates the Phase 1.2 roadmap item: "Persist ListState/MapState to LSMTree"
"""
import os
import shutil
import tempfile
import sys
import pytest

sys.path.insert(0, '/Users/bengamble/Sabot')


@pytest.fixture
def test_dir():
    """Create a temporary directory for each test."""
    test_dir = tempfile.mkdtemp(prefix="marble_test_")
    yield test_dir
    shutil.rmtree(test_dir, ignore_errors=True)


def get_backend(test_dir):
    """Import and create MarbleDBStateBackend."""
    from sabot._cython.state.marbledb_backend import MarbleDBStateBackend
    return MarbleDBStateBackend(test_dir)


class TestRawKVPersistence:
    """Test raw key-value persistence."""

    def test_single_key_survives_restart(self, test_dir):
        """Single key-value pair survives restart."""
        backend = get_backend(test_dir)
        backend.open()
        backend.put_raw("test_key", b"test_value")
        assert backend.get_raw("test_key") == b"test_value"
        backend.close()

        backend2 = get_backend(test_dir)
        backend2.open()
        assert backend2.get_raw("test_key") == b"test_value"
        backend2.close()

    def test_multiple_keys_survive_restart(self, test_dir):
        """Multiple key-value pairs survive restart."""
        backend = get_backend(test_dir)
        backend.open()

        backend.put_raw("key1", b"value1")
        backend.put_raw("key2", b"value2")
        backend.put_raw("key3", b"value3")

        backend.close()

        backend2 = get_backend(test_dir)
        backend2.open()

        assert backend2.get_raw("key1") == b"value1"
        assert backend2.get_raw("key2") == b"value2"
        assert backend2.get_raw("key3") == b"value3"

        backend2.close()

    def test_nonexistent_key_returns_none(self, test_dir):
        """Nonexistent key returns None after restart."""
        backend = get_backend(test_dir)
        backend.open()
        backend.put_raw("existing", b"value")
        backend.close()

        backend2 = get_backend(test_dir)
        backend2.open()
        assert backend2.get_raw("existing") == b"value"
        assert backend2.get_raw("nonexistent") is None
        backend2.close()


class TestListStatePersistence:
    """Test ListState persistence."""

    def test_list_survives_restart(self, test_dir):
        """ListState data survives restart."""
        backend = get_backend(test_dir)
        backend.open()

        backend.add_to_list("my_list", "item1")
        backend.add_to_list("my_list", "item2")
        backend.add_to_list("my_list", 42)

        assert backend.get_list("my_list") == ["item1", "item2", 42]
        backend.close()

        backend2 = get_backend(test_dir)
        backend2.open()
        assert backend2.get_list("my_list") == ["item1", "item2", 42]
        backend2.close()

    def test_empty_list_after_clear(self, test_dir):
        """Cleared list is empty after restart."""
        backend = get_backend(test_dir)
        backend.open()

        backend.add_to_list("my_list", "item1")
        backend.add_to_list("my_list", "item2")
        backend.clear_list("my_list")

        assert backend.get_list("my_list") == []
        backend.close()

        backend2 = get_backend(test_dir)
        backend2.open()
        assert backend2.get_list("my_list") == []
        backend2.close()

    def test_multiple_lists_survive_restart(self, test_dir):
        """Multiple ListStates survive restart."""
        backend = get_backend(test_dir)
        backend.open()

        backend.add_to_list("list_a", "a1")
        backend.add_to_list("list_a", "a2")
        backend.add_to_list("list_b", "b1")

        # Verify before close
        assert backend.get_list("list_a") == ["a1", "a2"], "list_a wrong before close"
        assert backend.get_list("list_b") == ["b1"], "list_b wrong before close"

        backend.close()

        backend2 = get_backend(test_dir)
        backend2.open()
        assert backend2.get_list("list_a") == ["a1", "a2"], "list_a wrong after restart"
        assert backend2.get_list("list_b") == ["b1"], "list_b wrong after restart"
        backend2.close()


class TestRawKVMultipleKeys:
    """Test raw KV persistence with multiple distinct keys."""

    def test_two_raw_keys_survive_restart(self, test_dir):
        """Two distinct raw keys survive restart."""
        backend = get_backend(test_dir)
        backend.open()

        backend.put_raw("key_a", b"value_a")
        backend.put_raw("key_b", b"value_b")

        # Verify before close
        assert backend.get_raw("key_a") == b"value_a"
        assert backend.get_raw("key_b") == b"value_b"

        backend.close()

        backend2 = get_backend(test_dir)
        backend2.open()
        result_a = backend2.get_raw("key_a")
        result_b = backend2.get_raw("key_b")
        assert result_a == b"value_a", f"key_a: expected b'value_a' got {result_a}"
        assert result_b == b"value_b", f"key_b: expected b'value_b' got {result_b}"
        backend2.close()

    def test_three_raw_keys_survive_restart(self, test_dir):
        """Three distinct raw keys survive restart."""
        backend = get_backend(test_dir)
        backend.open()

        backend.put_raw("x", b"1")
        backend.put_raw("y", b"2")
        backend.put_raw("z", b"3")

        backend.close()

        backend2 = get_backend(test_dir)
        backend2.open()
        assert backend2.get_raw("x") == b"1", "key x failed"
        assert backend2.get_raw("y") == b"2", "key y failed"
        assert backend2.get_raw("z") == b"3", "key z failed"
        backend2.close()


class TestMapStatePersistence:
    """Test MapState persistence."""

    def test_map_survives_restart(self, test_dir):
        """MapState data survives restart."""
        backend = get_backend(test_dir)
        backend.open()

        backend.put_to_map("my_map", "key1", "value1")
        backend.put_to_map("my_map", "key2", 42)

        assert backend.get_map("my_map") == {"key1": "value1", "key2": 42}
        backend.close()

        backend2 = get_backend(test_dir)
        backend2.open()
        assert backend2.get_map("my_map") == {"key1": "value1", "key2": 42}
        backend2.close()

    def test_get_from_map_after_restart(self, test_dir):
        """get_from_map works after restart."""
        backend = get_backend(test_dir)
        backend.open()

        backend.put_to_map("my_map", "key1", "value1")
        backend.put_to_map("my_map", "key2", 42)

        backend.close()

        backend2 = get_backend(test_dir)
        backend2.open()
        assert backend2.get_from_map("my_map", "key1") == "value1"
        assert backend2.get_from_map("my_map", "key2") == 42
        assert backend2.get_from_map("my_map", "nonexistent") is None
        assert backend2.get_from_map("my_map", "nonexistent", "default") == "default"
        backend2.close()

    def test_remove_from_map_persists(self, test_dir):
        """Removed keys stay removed after restart."""
        backend = get_backend(test_dir)
        backend.open()

        backend.put_to_map("my_map", "a", 1)
        backend.put_to_map("my_map", "b", 2)
        backend.put_to_map("my_map", "c", 3)
        backend.remove_from_map("my_map", "b")

        assert backend.get_map("my_map") == {"a": 1, "c": 3}
        backend.close()

        backend2 = get_backend(test_dir)
        backend2.open()
        assert backend2.get_map("my_map") == {"a": 1, "c": 3}
        backend2.close()

    def test_clear_map_persists(self, test_dir):
        """Cleared map is empty after restart."""
        backend = get_backend(test_dir)
        backend.open()

        backend.put_to_map("my_map", "key1", "value1")
        backend.clear_map("my_map")

        assert backend.get_map("my_map") == {}
        backend.close()

        backend2 = get_backend(test_dir)
        backend2.open()
        assert backend2.get_map("my_map") == {}
        backend2.close()


class TestMixedStatePersistence:
    """Test mixed state types persisting together."""

    def test_all_state_types_together(self, test_dir):
        """Raw KV, ListState, and MapState all persist together."""
        backend = get_backend(test_dir)
        backend.open()

        # Raw KV
        backend.put_raw("raw_key", b"raw_value")

        # ListState
        backend.add_to_list("my_list", "item1")
        backend.add_to_list("my_list", "item2")

        # MapState
        backend.put_to_map("my_map", "k1", "v1")
        backend.put_to_map("my_map", "k2", 42)

        backend.close()

        backend2 = get_backend(test_dir)
        backend2.open()

        assert backend2.get_raw("raw_key") == b"raw_value"
        assert backend2.get_list("my_list") == ["item1", "item2"]
        assert backend2.get_map("my_map") == {"k1": "v1", "k2": 42}

        backend2.close()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
