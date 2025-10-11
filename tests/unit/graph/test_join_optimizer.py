"""
Test suite for pattern DAG join optimizer.

Verifies that OptimizeJoinOrder produces efficient join plans.
"""
import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Set library path for Arrow
os.environ['DYLD_LIBRARY_PATH'] = '/Users/bengamble/Sabot/vendor/arrow/cpp/build/install/lib'

import pyarrow as pa
from sabot._cython.graph.query.pattern_match import OptimizeJoinOrder


def test_basic_join_order():
    """Test that optimizer starts with smallest table"""
    print("\n=== Test 1: Basic Join Order ===")

    # Create three tables of different sizes
    # Small: 10 edges
    small_edges = pa.table({
        'source': pa.array(list(range(10)), type=pa.int64()),
        'target': pa.array(list(range(1, 11)), type=pa.int64())
    })

    # Medium: 100 edges
    medium_edges = pa.table({
        'source': pa.array(list(range(100)), type=pa.int64()),
        'target': pa.array(list(range(1, 101)), type=pa.int64())
    })

    # Large: 1000 edges
    large_edges = pa.table({
        'source': pa.array(list(range(1000)), type=pa.int64()),
        'target': pa.array(list(range(1, 1001)), type=pa.int64())
    })

    # Test 1: Order should be [small, medium, large]
    edges = [large_edges, medium_edges, small_edges]
    order = OptimizeJoinOrder(edges)

    print(f"Input order: [Large(1000), Medium(100), Small(10)]")
    print(f"Optimized order: {order}")
    print(f"Expected: [2, 1, 0] (start with smallest)")

    assert order[0] == 2, f"First table should be smallest (index 2), got {order[0]}"
    assert len(order) == 3, f"Order should have 3 elements, got {len(order)}"

    print("✅ Test passed!")


def test_single_table():
    """Test optimizer with single table"""
    print("\n=== Test 2: Single Table ===")

    edges = pa.table({
        'source': pa.array([0, 1, 2], type=pa.int64()),
        'target': pa.array([1, 2, 3], type=pa.int64())
    })

    order = OptimizeJoinOrder([edges])

    print(f"Optimized order: {order}")
    assert order == [0], f"Single table should return [0], got {order}"

    print("✅ Test passed!")


def test_empty_tables():
    """Test optimizer with empty input"""
    print("\n=== Test 3: Empty Tables ===")

    order = OptimizeJoinOrder([])

    print(f"Optimized order: {order}")
    assert order == [], f"Empty input should return [], got {order}"

    print("✅ Test passed!")


def test_greedy_join_selection():
    """Test that optimizer greedily selects best next join"""
    print("\n=== Test 4: Greedy Join Selection ===")

    # Create graph with selective join opportunities
    # E1: 10 edges, 5 sources, 5 targets (avg out-degree = 2)
    e1 = pa.table({
        'source': pa.array([0, 0, 1, 1, 2, 2, 3, 3, 4, 4], type=pa.int64()),
        'target': pa.array([5, 6, 7, 8, 9, 10, 11, 12, 13, 14], type=pa.int64())
    })

    # E2: 100 edges, 10 sources, 100 targets (avg out-degree = 10)
    e2_sources = []
    e2_targets = []
    for i in range(10):
        for j in range(10):
            e2_sources.append(5 + i)  # Sources overlap with E1 targets
            e2_targets.append(100 + i * 10 + j)
    e2 = pa.table({
        'source': pa.array(e2_sources, type=pa.int64()),
        'target': pa.array(e2_targets, type=pa.int64())
    })

    # E3: 50 edges, 10 sources, 50 targets
    e3_sources = []
    e3_targets = []
    for i in range(10):
        for j in range(5):
            e3_sources.append(100 + i)  # Sources overlap with E2 targets
            e3_targets.append(200 + i * 5 + j)
    e3 = pa.table({
        'source': pa.array(e3_sources, type=pa.int64()),
        'target': pa.array(e3_targets, type=pa.int64())
    })

    # Optimizer should choose: E1 (10) -> E2 (100) -> E3 (50)
    # Because E1 is smallest, then E1⋈E2 has better selectivity than E1⋈E3
    edges = [e1, e2, e3]
    order = OptimizeJoinOrder(edges)

    print(f"Edge sizes: E1={e1.num_rows}, E2={e2.num_rows}, E3={e3.num_rows}")
    print(f"Optimized order: {order}")

    # E1 should be first (smallest)
    assert order[0] == 0, f"E1 (smallest) should be first, got index {order[0]}"

    print("✅ Test passed!")


def test_identical_tables():
    """Test optimizer with identical tables"""
    print("\n=== Test 5: Identical Tables ===")

    # Three identical tables
    edges = pa.table({
        'source': pa.array([0, 1, 2], type=pa.int64()),
        'target': pa.array([1, 2, 3], type=pa.int64())
    })

    order = OptimizeJoinOrder([edges, edges, edges])

    print(f"Optimized order: {order}")
    # When all tables are identical, any order is valid
    assert len(order) == 3, f"Order should have 3 elements, got {len(order)}"
    assert set(order) == {0, 1, 2}, f"Order should contain all indices, got {set(order)}"

    print("✅ Test passed!")


def test_skewed_cardinalities():
    """Test optimizer with highly skewed cardinalities"""
    print("\n=== Test 6: Skewed Cardinalities ===")

    # E1: Very few edges (2)
    e1 = pa.table({
        'source': pa.array([0, 1], type=pa.int64()),
        'target': pa.array([10, 11], type=pa.int64())
    })

    # E2: Many edges but highly selective (1000 edges, 1000 distinct sources)
    e2 = pa.table({
        'source': pa.array(list(range(1000)), type=pa.int64()),
        'target': pa.array(list(range(1000, 2000)), type=pa.int64())
    })

    # E3: Medium edges (100)
    e3 = pa.table({
        'source': pa.array(list(range(100)), type=pa.int64()),
        'target': pa.array(list(range(100, 200)), type=pa.int64())
    })

    edges = [e2, e3, e1]  # Input order: large, medium, tiny
    order = OptimizeJoinOrder(edges)

    print(f"Edge sizes: E2={e2.num_rows}, E3={e3.num_rows}, E1={e1.num_rows}")
    print(f"Optimized order: {order}")
    print(f"Expected: [2, ...] (start with E1, the smallest)")

    # E1 (index 2) should be first
    assert order[0] == 2, f"E1 (index 2) should be first, got {order[0]}"

    print("✅ Test passed!")


def run_all_tests():
    """Run all join optimizer tests"""
    tests = [
        test_basic_join_order,
        test_single_table,
        test_empty_tables,
        test_greedy_join_selection,
        test_identical_tables,
        test_skewed_cardinalities,
    ]

    print("=" * 60)
    print("Join Optimizer Test Suite")
    print("=" * 60)

    passed = 0
    failed = 0

    for test in tests:
        try:
            test()
            passed += 1
        except Exception as e:
            print(f"❌ Test failed: {e}")
            import traceback
            traceback.print_exc()
            failed += 1

    print("\n" + "=" * 60)
    print(f"Results: {passed} passed, {failed} failed")
    print("=" * 60)

    return failed == 0


if __name__ == "__main__":
    success = run_all_tests()
    sys.exit(0 if success else 1)
