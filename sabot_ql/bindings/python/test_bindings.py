#!/usr/bin/env python3
"""
Comprehensive test suite for SabotQL Python bindings
"""

import sabot_ql_native
import tempfile
import os
import shutil
import sys

def test_create_triple_store():
    """Test 1: Create and destroy triple store"""
    print("\n[Test 1] Creating triple store...")

    temp_dir = tempfile.mkdtemp()
    db_path = os.path.join(temp_dir, "test_db")

    try:
        store = sabot_ql_native.create_triple_store(db_path)
        print(f"  ✓ Created triple store")
        print(f"    Type: {type(store)}")
        print(f"    Methods: {[m for m in dir(store) if not m.startswith('_')]}")

        # Close store
        store.close()
        print(f"  ✓ Closed triple store")

        return True
    except Exception as e:
        print(f"  ✗ Error: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)


def test_persistence():
    """Test 2: Verify persistence across sessions"""
    print("\n[Test 2] Testing persistence...")

    temp_dir = tempfile.mkdtemp()
    db_path = os.path.join(temp_dir, "persist_db")

    try:
        # Create store and close
        print("  Creating first store instance...")
        store1 = sabot_ql_native.create_triple_store(db_path)
        store1.close()
        print("  ✓ First instance closed")

        # Check that database files were created
        if os.path.exists(db_path):
            files = os.listdir(db_path)
            print(f"  ✓ Database directory exists with {len(files)} files")
        else:
            print(f"  ✗ Database directory not created")
            return False

        # Re-open the same database
        print("  Reopening database...")
        store2 = sabot_ql_native.create_triple_store(db_path)
        print("  ✓ Successfully reopened existing database")
        store2.close()

        return True
    except Exception as e:
        print(f"  ✗ Error: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)


def test_multiple_stores():
    """Test 3: Create multiple independent stores"""
    print("\n[Test 3] Testing multiple stores...")

    temp_dir = tempfile.mkdtemp()

    try:
        # Create multiple stores
        stores = []
        for i in range(3):
            db_path = os.path.join(temp_dir, f"store_{i}")
            store = sabot_ql_native.create_triple_store(db_path)
            stores.append((store, db_path))
            print(f"  ✓ Created store {i+1}/3 at {db_path}")

        # Close all stores
        for store, path in stores:
            store.close()
        print("  ✓ All stores closed successfully")

        return True
    except Exception as e:
        print(f"  ✗ Error: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)


def test_error_handling():
    """Test 4: Error handling for invalid operations"""
    print("\n[Test 4] Testing error handling...")

    temp_dir = tempfile.mkdtemp()
    db_path = os.path.join(temp_dir, "error_test_db")

    try:
        store = sabot_ql_native.create_triple_store(db_path)

        # Test calling unimplemented methods
        print("  Testing unimplemented methods...")
        try:
            store.insert_triple("http://example.org/s", "http://example.org/p", "http://example.org/o")
            print("  ✗ insert_triple should raise NotImplementedError")
            return False
        except NotImplementedError as e:
            print(f"  ✓ insert_triple correctly raises NotImplementedError")

        try:
            store.size()
            print("  ✗ size should raise NotImplementedError")
            return False
        except NotImplementedError:
            print(f"  ✓ size correctly raises NotImplementedError")

        store.close()
        return True
    except Exception as e:
        print(f"  ✗ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)


def test_wrapper_function():
    """Test 5: Test create_triple_store wrapper function"""
    print("\n[Test 5] Testing wrapper function...")

    temp_dir = tempfile.mkdtemp()
    db_path = os.path.join(temp_dir, "wrapper_test")

    try:
        # Test using wrapper function
        store = sabot_ql_native.create_triple_store(db_path)
        print(f"  ✓ Wrapper function works")
        print(f"    Returns: {type(store).__name__}")

        # Verify it's the right type
        assert isinstance(store, sabot_ql_native.TripleStoreWrapper), \
            "create_triple_store should return TripleStoreWrapper"
        print("  ✓ Returns correct type")

        store.close()
        return True
    except Exception as e:
        print(f"  ✗ Error: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)


def test_memory_safety():
    """Test 6: Test memory safety with rapid create/destroy"""
    print("\n[Test 6] Testing memory safety...")

    temp_dir = tempfile.mkdtemp()

    try:
        print("  Creating and destroying 10 stores rapidly...")
        for i in range(10):
            db_path = os.path.join(temp_dir, f"mem_test_{i}")
            store = sabot_ql_native.create_triple_store(db_path)
            store.close()

        print("  ✓ All 10 stores created and closed without crashes")
        return True
    except Exception as e:
        print(f"  ✗ Error: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)


def test_path_handling():
    """Test 7: Test various path formats"""
    print("\n[Test 7] Testing path handling...")

    temp_dir = tempfile.mkdtemp()

    tests = [
        ("simple", os.path.join(temp_dir, "simple")),
        ("with-dash", os.path.join(temp_dir, "with-dash")),
        ("with_underscore", os.path.join(temp_dir, "with_underscore")),
        ("nested/path", os.path.join(temp_dir, "nested", "path")),
    ]

    passed = 0
    for name, db_path in tests:
        try:
            # Create parent dirs if needed
            parent = os.path.dirname(db_path)
            if parent and not os.path.exists(parent):
                os.makedirs(parent)

            store = sabot_ql_native.create_triple_store(db_path)
            store.close()
            print(f"  ✓ {name}: {db_path}")
            passed += 1
        except Exception as e:
            print(f"  ✗ {name}: {e}")

    success = passed == len(tests)
    if success:
        print(f"  ✓ All {len(tests)} path formats handled correctly")
    else:
        print(f"  ✗ Only {passed}/{len(tests)} path formats worked")

    if os.path.exists(temp_dir):
        shutil.rmtree(temp_dir)

    return success


def run_all_tests():
    """Run all tests and report results"""
    print("=" * 70)
    print("SabotQL Python Bindings Test Suite")
    print("=" * 70)

    tests = [
        ("Create triple store", test_create_triple_store),
        ("Persistence", test_persistence),
        ("Multiple stores", test_multiple_stores),
        ("Error handling", test_error_handling),
        ("Wrapper function", test_wrapper_function),
        ("Memory safety", test_memory_safety),
        ("Path handling", test_path_handling),
    ]

    results = []
    for name, test_func in tests:
        try:
            result = test_func()
            results.append((name, result))
        except Exception as e:
            print(f"\n[FATAL] {name} crashed: {e}")
            import traceback
            traceback.print_exc()
            results.append((name, False))

    # Summary
    print("\n" + "=" * 70)
    print("Test Summary")
    print("=" * 70)

    passed = sum(1 for _, result in results if result)
    total = len(results)

    for name, result in results:
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"  {status}: {name}")

    print("-" * 70)
    print(f"  Total: {passed}/{total} tests passed")

    if passed == total:
        print("\n🎉 All tests passed!")
        return 0
    else:
        print(f"\n⚠️  {total - passed} test(s) failed")
        return 1


if __name__ == "__main__":
    sys.exit(run_all_tests())
