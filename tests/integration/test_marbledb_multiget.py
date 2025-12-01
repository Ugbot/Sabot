#!/usr/bin/env python3
"""Test MultiGet functionality for MarbleDB."""
import tempfile
import shutil
import sys
import pytest

sys.path.insert(0, '/Users/bengamble/Sabot')


class TestMarbleDBMultiGet:
    """Test suite for MarbleDB MultiGet API."""

    def setup_method(self):
        """Set up test fixtures."""
        from sabot._cython.state.marbledb_backend import MarbleDBStateBackend
        self.test_dir = tempfile.mkdtemp(prefix='marble_multiget_')
        self.backend = MarbleDBStateBackend(self.test_dir)
        self.backend.open()

    def teardown_method(self):
        """Tear down test fixtures."""
        self.backend.close()
        shutil.rmtree(self.test_dir)

    def test_multiget_all_keys_exist(self):
        """Test MultiGet when all requested keys exist."""
        keys_to_insert = ['key_a', 'key_b', 'key_c', 'key_d', 'key_e']
        for key in keys_to_insert:
            self.backend.put_raw(key, f'value_{key}'.encode())
        self.backend.flush()

        result = self.backend.multi_get_raw(keys_to_insert)

        for key in keys_to_insert:
            expected = f'value_{key}'.encode()
            actual = result.get(key)
            assert actual == expected, f'{key}: expected={expected}, actual={actual}'

    def test_multiget_mixed_existing_nonexisting(self):
        """Test MultiGet with a mix of existing and non-existing keys."""
        keys_to_insert = ['key_a', 'key_c']
        for key in keys_to_insert:
            self.backend.put_raw(key, f'value_{key}'.encode())
        self.backend.flush()

        mixed_keys = ['key_a', 'nonexistent_1', 'key_c', 'nonexistent_2']
        result = self.backend.multi_get_raw(mixed_keys)

        assert result.get('key_a') == b'value_key_a'
        assert result.get('nonexistent_1') is None
        assert result.get('key_c') == b'value_key_c'
        assert result.get('nonexistent_2') is None

    def test_multiget_empty_list(self):
        """Test MultiGet with empty list returns empty dict."""
        result = self.backend.multi_get_raw([])
        assert result == {}

    def test_multiget_after_restart(self):
        """Test MultiGet after close and reopen."""
        keys_to_insert = ['key_a', 'key_b', 'key_c']
        for key in keys_to_insert:
            self.backend.put_raw(key, f'value_{key}'.encode())
        self.backend.flush()
        self.backend.close()

        # Reopen
        from sabot._cython.state.marbledb_backend import MarbleDBStateBackend
        backend2 = MarbleDBStateBackend(self.test_dir)
        backend2.open()

        result = backend2.multi_get_raw(keys_to_insert)
        all_found = all(result.get(key) == f'value_{key}'.encode() for key in keys_to_insert)
        backend2.close()

        assert all_found, 'All keys should be found after restart'

    def test_multiget_before_flush(self):
        """Test MultiGet works before flush (memtable only)."""
        self.backend.put_raw('key_a', b'value_a')
        self.backend.put_raw('key_b', b'value_b')
        # No flush

        result = self.backend.multi_get_raw(['key_a', 'key_b'])
        assert result.get('key_a') == b'value_a'
        assert result.get('key_b') == b'value_b'

    def test_multiget_after_flush(self):
        """Test MultiGet works after flush (data visible during async flush)."""
        self.backend.put_raw('key_a', b'value_a')
        self.backend.put_raw('key_b', b'value_b')
        self.backend.flush()

        result = self.backend.multi_get_raw(['key_a', 'key_b'])
        assert result.get('key_a') == b'value_a'
        assert result.get('key_b') == b'value_b'


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
