#!/usr/bin/env python3
"""Test Scan and DeleteRange functionality for MarbleDB."""
import tempfile
import shutil
import sys
import pytest

sys.path.insert(0, '/Users/bengamble/Sabot')


class TestMarbleDBScan:
    """Test suite for MarbleDB Scan API."""

    def setup_method(self):
        """Set up test fixtures."""
        from sabot._cython.state.marbledb_backend import MarbleDBStateBackend
        self.test_dir = tempfile.mkdtemp(prefix='marble_scan_')
        self.backend = MarbleDBStateBackend(self.test_dir)
        self.backend.open()

    def teardown_method(self):
        """Tear down test fixtures."""
        self.backend.close()
        shutil.rmtree(self.test_dir)

    def test_scan_all_keys(self):
        """Test full scan returns all keys."""
        keys_to_insert = ['key_a', 'key_b', 'key_c']
        for key in keys_to_insert:
            self.backend.put_raw(key, f'value_{key}'.encode())
        self.backend.flush()

        # Full scan with None, None
        results = list(self.backend.scan_range(None, None))
        found_keys = [k for k, v in results]

        for key in keys_to_insert:
            assert key in found_keys, f'Key {key} not found in scan results'

    def test_scan_empty_store(self):
        """Test scan on empty store returns empty results."""
        results = list(self.backend.scan_range(None, None))
        assert len(results) == 0

    def test_scan_after_restart(self):
        """Test scan works after close and reopen."""
        keys_to_insert = ['key_a', 'key_b', 'key_c']
        for key in keys_to_insert:
            self.backend.put_raw(key, f'value_{key}'.encode())
        self.backend.flush()
        self.backend.close()

        from sabot._cython.state.marbledb_backend import MarbleDBStateBackend
        backend2 = MarbleDBStateBackend(self.test_dir)
        backend2.open()

        results = list(backend2.scan_range(None, None))
        found_keys = [k for k, v in results]
        backend2.close()

        for key in keys_to_insert:
            assert key in found_keys, f'Key {key} not found after restart'


class TestMarbleDBDeleteRange:
    """Test suite for MarbleDB DeleteRange API."""

    def setup_method(self):
        """Set up test fixtures."""
        from sabot._cython.state.marbledb_backend import MarbleDBStateBackend
        self.test_dir = tempfile.mkdtemp(prefix='marble_delrange_')
        self.backend = MarbleDBStateBackend(self.test_dir)
        self.backend.open()

    def teardown_method(self):
        """Tear down test fixtures."""
        self.backend.close()
        shutil.rmtree(self.test_dir)

    def test_delete_range_middle_keys(self):
        """Test deleting a range of keys in the middle."""
        # Insert 20 keys
        for i in range(20):
            self.backend.put_raw(f'key_{i:04d}', f'value_{i}'.encode())
        self.backend.flush()

        # Delete range key_0005 to key_0015 (exclusive end)
        self.backend.delete_range_raw('key_0005', 'key_0015')
        self.backend.flush()

        # Verify remaining keys
        expected_remaining = list(range(0, 5)) + list(range(15, 20))
        expected_deleted = list(range(5, 15))

        for i in expected_remaining:
            val = self.backend.get_raw(f'key_{i:04d}')
            assert val is not None, f'key_{i:04d} should exist but was deleted'

        for i in expected_deleted:
            val = self.backend.get_raw(f'key_{i:04d}')
            assert val is None, f'key_{i:04d} should be deleted but still exists'

    def test_delete_range_all_keys(self):
        """Test deleting all keys."""
        keys = ['key_a', 'key_b', 'key_c']
        for key in keys:
            self.backend.put_raw(key, b'value')
        self.backend.flush()

        # Delete from '' to 'z' should delete all
        self.backend.delete_range_raw('', 'z')
        self.backend.flush()

        for key in keys:
            val = self.backend.get_raw(key)
            assert val is None, f'{key} should be deleted'

    def test_delete_range_no_matching_keys(self):
        """Test delete range with no matching keys does nothing."""
        self.backend.put_raw('key_a', b'value_a')
        self.backend.put_raw('key_z', b'value_z')
        self.backend.flush()

        # Delete range that doesn't match any keys
        self.backend.delete_range_raw('key_m', 'key_n')
        self.backend.flush()

        assert self.backend.get_raw('key_a') == b'value_a'
        assert self.backend.get_raw('key_z') == b'value_z'

    def test_delete_range_persists_after_restart(self):
        """Test deleted keys stay deleted after restart."""
        self.backend.put_raw('key_a', b'value_a')
        self.backend.put_raw('key_b', b'value_b')
        self.backend.put_raw('key_c', b'value_c')
        self.backend.flush()

        self.backend.delete_range_raw('key_a', 'key_c')  # Delete a and b
        self.backend.flush()
        self.backend.close()

        from sabot._cython.state.marbledb_backend import MarbleDBStateBackend
        backend2 = MarbleDBStateBackend(self.test_dir)
        backend2.open()

        assert backend2.get_raw('key_a') is None, 'key_a should be deleted'
        assert backend2.get_raw('key_b') is None, 'key_b should be deleted'
        assert backend2.get_raw('key_c') == b'value_c', 'key_c should exist'

        backend2.close()


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
