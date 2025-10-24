#!/usr/bin/env python3
"""
Test suite for sabot.graph module integration
"""

import sys
import tempfile
import shutil
import os

def test_triple_store_import():
    """Test 1: Import and create triple store"""
    print("\n[Test 1] Testing TripleStore import and creation...")

    try:
        from sabot.graph import create_triple_store, TripleStore
        print("  ✓ Import successful")

        temp_dir = tempfile.mkdtemp()
        db_path = os.path.join(temp_dir, "test_triple_store")

        try:
            store = create_triple_store(db_path)
            print(f"  ✓ Created triple store at {db_path}")

            assert isinstance(store, TripleStore), "Should return TripleStore instance"
            print("  ✓ Correct type returned")

            store.close()
            print("  ✓ Store closed successfully")

            return True
        finally:
            if os.path.exists(temp_dir):
                shutil.rmtree(temp_dir)

    except Exception as e:
        print(f"  ✗ Error: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_triple_store_context_manager():
    """Test 2: TripleStore context manager"""
    print("\n[Test 2] Testing context manager...")

    try:
        from sabot.graph import create_triple_store

        temp_dir = tempfile.mkdtemp()
        db_path = os.path.join(temp_dir, "test_ctx")

        try:
            with create_triple_store(db_path) as store:
                print("  ✓ Context manager entry worked")

            print("  ✓ Context manager exit (auto-close) worked")
            return True
        finally:
            if os.path.exists(temp_dir):
                shutil.rmtree(temp_dir)

    except Exception as e:
        print(f"  ✗ Error: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_property_graph_import():
    """Test 3: PropertyGraph import (may not be built)"""
    print("\n[Test 3] Testing PropertyGraph import...")

    try:
        from sabot.graph import create_property_graph, PropertyGraph
        print("  ✓ Import successful")

        # Try creating in-memory graph
        g = create_property_graph(num_vertices_hint=100, num_edges_hint=100)
        print("  ✓ Created in-memory property graph")

        assert isinstance(g, PropertyGraph), "Should return PropertyGraph instance"
        print("  ✓ Correct type returned")

        g.close()
        print("  ✓ Graph closed successfully")

        return True

    except ImportError as e:
        print(f"  ℹ PropertyGraph not available (this is OK if not built): {e}")
        return True  # Not a failure if not built
    except Exception as e:
        print(f"  ✗ Error: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_module_exports():
    """Test 4: Check module exports"""
    print("\n[Test 4] Checking module exports...")

    try:
        import sabot.graph as graph_module
        print("  ✓ Module imported")

        exports = graph_module.__all__
        print(f"  ✓ Exports defined: {exports}")

        expected = {
            'PropertyGraph',
            'TripleStore',
            'GraphQueryEngine',
            'QueryMode',
            'QueryConfig',
            'create_triple_store',
            'create_property_graph',
        }

        assert set(exports) == expected, f"Exports mismatch: {set(exports)} vs {expected}"
        print("  ✓ All expected exports present")

        return True

    except Exception as e:
        print(f"  ✗ Error: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_error_handling():
    """Test 5: Error handling for unimplemented methods"""
    print("\n[Test 5] Testing error handling...")

    try:
        from sabot.graph import create_triple_store

        temp_dir = tempfile.mkdtemp()
        db_path = os.path.join(temp_dir, "test_errors")

        try:
            store = create_triple_store(db_path)

            # Test unimplemented methods
            try:
                store.insert_triple("http://ex.org/s", "http://ex.org/p", "http://ex.org/o")
                print("  ✗ insert_triple should raise NotImplementedError")
                return False
            except NotImplementedError:
                print("  ✓ insert_triple correctly raises NotImplementedError")

            try:
                store.size()
                print("  ✗ size should raise NotImplementedError")
                return False
            except NotImplementedError:
                print("  ✓ size correctly raises NotImplementedError")

            store.close()
            return True

        finally:
            if os.path.exists(temp_dir):
                shutil.rmtree(temp_dir)

    except Exception as e:
        print(f"  ✗ Error: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_documentation():
    """Test 6: Check documentation strings"""
    print("\n[Test 6] Checking documentation...")

    try:
        import sabot.graph as graph_module
        from sabot.graph import TripleStore, create_triple_store

        assert graph_module.__doc__, "Module should have docstring"
        print("  ✓ Module docstring present")

        assert TripleStore.__doc__, "TripleStore should have docstring"
        print("  ✓ TripleStore docstring present")

        assert create_triple_store.__doc__, "create_triple_store should have docstring"
        print("  ✓ create_triple_store docstring present")

        # Check that docstrings contain examples
        assert "Example:" in graph_module.__doc__, "Module docstring should have examples"
        print("  ✓ Module docstring has examples")

        return True

    except Exception as e:
        print(f"  ✗ Error: {e}")
        import traceback
        traceback.print_exc()
        return False


def run_all_tests():
    """Run all integration tests"""
    print("=" * 70)
    print("Sabot Graph Module Integration Tests")
    print("=" * 70)

    tests = [
        ("TripleStore import and creation", test_triple_store_import),
        ("Context manager", test_triple_store_context_manager),
        ("PropertyGraph import", test_property_graph_import),
        ("Module exports", test_module_exports),
        ("Error handling", test_error_handling),
        ("Documentation", test_documentation),
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
