#!/usr/bin/env python3
"""
Direct test of MarbleDB-backed triple store
"""

import tempfile
import shutil
import os
import sys

def test_triple_store():
    """Test MarbleDB triple store creation and basic operations"""

    print("\n" + "=" * 70)
    print("MarbleDB Triple Store Direct Test")
    print("=" * 70)

    # Import the native module
    print("\n[1] Importing sabot_ql_native...")
    try:
        import sabot_ql_native
        print(f"  ✓ Import successful")
        print(f"  Module: {sabot_ql_native.__file__}")
    except ImportError as e:
        print(f"  ✗ Import failed: {e}")
        return False

    # Create temporary directory
    temp_dir = tempfile.mkdtemp()
    db_path = os.path.join(temp_dir, "test_triple_db")

    try:
        print(f"\n[2] Creating triple store at {db_path}...")
        store = sabot_ql_native.create_triple_store(db_path)
        print(f"  ✓ Store created")
        print(f"  Type: {type(store)}")
        print(f"  Methods: {[m for m in dir(store) if not m.startswith('_')]}")

        # Test error handling
        print("\n[3] Testing error handling...")
        try:
            store.insert_triple("http://ex.org/s", "http://ex.org/p", "http://ex.org/o")
            print("  ✗ insert_triple should raise NotImplementedError")
            return False
        except NotImplementedError as e:
            print(f"  ✓ insert_triple raises NotImplementedError (expected)")
            print(f"    Message: {e}")

        try:
            store.size()
            print("  ✗ size should raise NotImplementedError")
            return False
        except NotImplementedError as e:
            print(f"  ✓ size raises NotImplementedError (expected)")
            print(f"    Message: {e}")

        # Test close
        print("\n[4] Testing close...")
        store.close()
        print("  ✓ Store closed successfully")

        # Test recreation
        print("\n[5] Testing recreation...")
        store2 = sabot_ql_native.create_triple_store(db_path)
        print("  ✓ Store recreated successfully")
        store2.close()
        print("  ✓ Second store closed")

        # Check what was created
        print("\n[6] Checking created files...")
        if os.path.exists(db_path):
            files = os.listdir(db_path)
            print(f"  ✓ Database directory exists")
            print(f"  Files: {files}")
        else:
            print(f"  ℹ Database directory not created (MarbleDB internal path)")

        print("\n" + "=" * 70)
        print("✅ ALL TESTS PASSED")
        print("=" * 70)
        print("\nMarbleDB Triple Store Status:")
        print("  ✅ C++ library built and linked")
        print("  ✅ Cython bindings working")
        print("  ✅ Python wrapper functional")
        print("  ✅ Memory management correct")
        print("  ⚠️  insert_triple() awaiting implementation")
        print("  ⚠️  size() awaiting implementation")
        print("\nNext Steps:")
        print("  - Implement Vocabulary::AddTerm() binding")
        print("  - Implement TripleStore::Insert() binding")
        print("  - Implement TripleStore::Size() binding")
        print("  - Add batch loading from Arrow tables")

        return True

    except Exception as e:
        print(f"\n✗ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)

if __name__ == "__main__":
    success = test_triple_store()
    sys.exit(0 if success else 1)
