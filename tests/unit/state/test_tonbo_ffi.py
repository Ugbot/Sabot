#!/usr/bin/env python3
"""
Test Tonbo FFI integration

This tests the Rust FFI bindings with Cython wrapper.
"""

import tempfile
import os
import sys

def test_tonbo_ffi():
    """Test basic Tonbo FFI operations."""

    # Import the Cython wrapper
    from sabot._cython.tonbo_wrapper import FastTonboBackend, TonboCythonWrapper

    # Create a temporary directory for the database
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "test_tonbo_ffi")

        print(f"Testing Tonbo FFI at {db_path}")

        # Test 1: FastTonboBackend (low-level API)
        print("\n1. Testing FastTonboBackend...")
        backend = FastTonboBackend(db_path)
        backend.initialize()

        # Insert some test data
        print("   - Inserting test data...")
        backend.fast_insert("key1", b"value1")
        backend.fast_insert("key2", b"value2")
        backend.fast_insert("key3", b"value3")

        # Get values
        print("   - Getting values...")
        value1 = backend.fast_get("key1")
        value2 = backend.fast_get("key2")
        value3 = backend.fast_get("key3")

        assert value1 == b"value1", f"Expected b'value1', got {value1}"
        assert value2 == b"value2", f"Expected b'value2', got {value2}"
        assert value3 == b"value3", f"Expected b'value3', got {value3}"
        print("   ✅ Get operations successful")

        # Test existence check
        print("   - Testing existence check...")
        assert backend.fast_exists("key1") == True
        assert backend.fast_exists("nonexistent") == False
        print("   ✅ Existence check successful")

        # Delete a key
        print("   - Testing delete...")
        result = backend.fast_delete("key2")
        assert result == True
        value2_after = backend.fast_get("key2")
        assert value2_after is None
        print("   ✅ Delete successful")

        # Get stats
        stats = backend.get_stats()
        print(f"   - Backend stats: {stats}")
        assert stats['backend_type'] == 'tonbo_ffi'
        assert stats['initialized'] == True

        backend.close()
        print("   ✅ FastTonboBackend tests passed!")

        # Test 2: TonboCythonWrapper (high-level API with serialization)
        print("\n2. Testing TonboCythonWrapper...")
        db_path2 = os.path.join(tmpdir, "test_tonbo_wrapper")
        wrapper = TonboCythonWrapper(db_path2)
        wrapper.initialize()

        # Insert Python objects (will be pickled)
        print("   - Inserting Python objects...")
        wrapper.put("user:1", {"name": "Alice", "age": 30})
        wrapper.put("user:2", {"name": "Bob", "age": 25})
        wrapper.put("counter", 42)

        # Get objects back
        print("   - Getting Python objects...")
        user1 = wrapper.get("user:1")
        user2 = wrapper.get("user:2")
        counter = wrapper.get("counter")

        assert user1 == {"name": "Alice", "age": 30}
        assert user2 == {"name": "Bob", "age": 25}
        assert counter == 42
        print("   ✅ Serialization/deserialization successful")

        # Test delete
        result = wrapper.delete("user:2")
        assert result == True
        user2_after = wrapper.get("user:2")
        assert user2_after is None
        print("   ✅ Delete successful")

        wrapper.close()
        print("   ✅ TonboCythonWrapper tests passed!")

        print("\n✅ All Tonbo FFI tests passed successfully!")
        return True

if __name__ == "__main__":
    try:
        test_tonbo_ffi()
        sys.exit(0)
    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
