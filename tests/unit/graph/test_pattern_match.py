"""
Test suite for pattern matching (2-hop and 3-hop queries).

Tests the graph query engine's ability to match patterns using hash joins.
"""
import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Set library path for Arrow
os.environ['DYLD_LIBRARY_PATH'] = '/Users/bengamble/Sabot/vendor/arrow/cpp/build/install/lib'

import pyarrow as pa
from sabot._cython.graph.query import match_2hop, match_3hop, PyPatternMatchResult


def test_basic_2hop_pattern():
    """Test basic 2-hop pattern: A->B->C"""
    print("\n=== Test 1: Basic 2-hop Pattern ===")

    # Create simple linear graph: 0->1->2->3
    edges = pa.table({
        'source': pa.array([0, 1, 2], type=pa.int64()),
        'target': pa.array([1, 2, 3], type=pa.int64())
    })

    result = match_2hop(edges, edges)

    print(f"Input edges: {edges.num_rows} edges")
    print(f"Found {result.num_matches()} 2-hop patterns")
    print(f"Bindings: {result.binding_names()}")
    print(f"Result table:\n{result.result_table()}")

    # Should find: 0->1->2 and 1->2->3
    assert result.num_matches() == 2
    assert result.binding_names() == ['a', 'b', 'c']

    # Verify specific paths
    table = result.result_table()
    a_ids = table.column('a_id').to_pylist()
    b_ids = table.column('b_id').to_pylist()
    c_ids = table.column('c_id').to_pylist()

    assert set(zip(a_ids, b_ids, c_ids)) == {(0, 1, 2), (1, 2, 3)}

    print("✅ Test passed!")


def test_2hop_custom_names():
    """Test 2-hop pattern with custom vertex binding names"""
    print("\n=== Test 2: Custom Binding Names ===")

    edges = pa.table({
        'source': pa.array([0, 1], type=pa.int64()),
        'target': pa.array([1, 2], type=pa.int64())
    })

    result = match_2hop(
        edges, edges,
        source_name="user",
        intermediate_name="page",
        target_name="product"
    )

    print(f"Bindings: {result.binding_names()}")
    print(f"Result columns: {result.result_table().column_names}")

    assert result.binding_names() == ['user', 'page', 'product']
    assert 'user_id' in result.result_table().column_names
    assert 'page_id' in result.result_table().column_names
    assert 'product_id' in result.result_table().column_names

    print("✅ Test passed!")


def test_2hop_branching_graph():
    """Test 2-hop on a branching graph"""
    print("\n=== Test 3: Branching Graph ===")

    # Graph: 0->1, 0->2, 1->3, 1->4, 2->4
    edges = pa.table({
        'source': pa.array([0, 0, 1, 1, 2], type=pa.int64()),
        'target': pa.array([1, 2, 3, 4, 4], type=pa.int64())
    })

    result = match_2hop(edges, edges)

    print(f"Input edges: {edges.num_rows} edges")
    print(f"Found {result.num_matches()} 2-hop patterns")

    table = result.result_table()
    a_ids = table.column('a_id').to_pylist()
    b_ids = table.column('b_id').to_pylist()
    c_ids = table.column('c_id').to_pylist()

    expected_paths = {
        (0, 1, 3),  # 0->1->3
        (0, 1, 4),  # 0->1->4
        (0, 2, 4),  # 0->2->4
    }

    actual_paths = set(zip(a_ids, b_ids, c_ids))
    print(f"Expected paths: {expected_paths}")
    print(f"Actual paths: {actual_paths}")

    assert actual_paths == expected_paths

    print("✅ Test passed!")


def test_2hop_cycle():
    """Test 2-hop on a graph with cycles"""
    print("\n=== Test 4: Graph with Cycles ===")

    # Cycle: 0->1->2->0
    edges = pa.table({
        'source': pa.array([0, 1, 2], type=pa.int64()),
        'target': pa.array([1, 2, 0], type=pa.int64())
    })

    result = match_2hop(edges, edges)

    print(f"Found {result.num_matches()} 2-hop patterns in cycle")

    table = result.result_table()
    a_ids = table.column('a_id').to_pylist()
    b_ids = table.column('b_id').to_pylist()
    c_ids = table.column('c_id').to_pylist()

    # Should find all 3 2-hop paths: 0->1->2, 1->2->0, 2->0->1
    expected_paths = {(0, 1, 2), (1, 2, 0), (2, 0, 1)}
    actual_paths = set(zip(a_ids, b_ids, c_ids))

    print(f"Paths: {actual_paths}")
    assert actual_paths == expected_paths

    print("✅ Test passed!")


def test_2hop_disconnected():
    """Test 2-hop with disconnected components"""
    print("\n=== Test 5: Disconnected Components ===")

    # Two separate chains: 0->1->2 and 10->11->12
    edges = pa.table({
        'source': pa.array([0, 1, 10, 11], type=pa.int64()),
        'target': pa.array([1, 2, 11, 12], type=pa.int64())
    })

    result = match_2hop(edges, edges)

    print(f"Found {result.num_matches()} 2-hop patterns")

    # Should find 2 paths (one in each component)
    assert result.num_matches() == 2

    table = result.result_table()
    paths = set(zip(
        table.column('a_id').to_pylist(),
        table.column('b_id').to_pylist(),
        table.column('c_id').to_pylist()
    ))

    expected = {(0, 1, 2), (10, 11, 12)}
    print(f"Paths: {paths}")
    assert paths == expected

    print("✅ Test passed!")


def test_2hop_empty_graph():
    """Test 2-hop on empty graph"""
    print("\n=== Test 6: Empty Graph ===")

    edges = pa.table({
        'source': pa.array([], type=pa.int64()),
        'target': pa.array([], type=pa.int64())
    })

    result = match_2hop(edges, edges)

    print(f"Found {result.num_matches()} matches (should be 0)")
    assert result.num_matches() == 0
    assert result.result_table().num_rows == 0

    print("✅ Test passed!")


def test_2hop_single_edge():
    """Test 2-hop with only one edge (no 2-hop paths possible)"""
    print("\n=== Test 7: Single Edge ===")

    edges = pa.table({
        'source': pa.array([0], type=pa.int64()),
        'target': pa.array([1], type=pa.int64())
    })

    result = match_2hop(edges, edges)

    print(f"Found {result.num_matches()} matches (should be 0)")
    assert result.num_matches() == 0

    print("✅ Test passed!")


def test_3hop_pattern():
    """Test basic 3-hop pattern: A->B->C->D"""
    print("\n=== Test 8: Basic 3-hop Pattern ===")

    # Linear graph: 0->1->2->3->4
    edges = pa.table({
        'source': pa.array([0, 1, 2, 3], type=pa.int64()),
        'target': pa.array([1, 2, 3, 4], type=pa.int64())
    })

    result = match_3hop(edges, edges, edges)

    print(f"Input edges: {edges.num_rows} edges")
    print(f"Found {result.num_matches()} 3-hop patterns")
    print(f"Bindings: {result.binding_names()}")
    print(f"Result table:\n{result.result_table()}")

    # Should find: 0->1->2->3 and 1->2->3->4
    assert result.num_matches() == 2
    assert result.binding_names() == ['a', 'b', 'c', 'd']

    # Verify paths
    table = result.result_table()
    a_ids = table.column('a_id').to_pylist()
    b_ids = table.column('b_id').to_pylist()
    c_ids = table.column('c_id').to_pylist()
    d_ids = table.column('d_id').to_pylist()

    paths = set(zip(a_ids, b_ids, c_ids, d_ids))
    expected = {(0, 1, 2, 3), (1, 2, 3, 4)}

    print(f"Expected paths: {expected}")
    print(f"Actual paths: {paths}")
    assert paths == expected

    print("✅ Test passed!")


def test_3hop_branching():
    """Test 3-hop on branching graph"""
    print("\n=== Test 9: 3-hop Branching Graph ===")

    # Graph: 0->1->2->3, 0->1->2->4
    edges = pa.table({
        'source': pa.array([0, 1, 2, 2], type=pa.int64()),
        'target': pa.array([1, 2, 3, 4], type=pa.int64())
    })

    result = match_3hop(edges, edges, edges)

    print(f"Found {result.num_matches()} 3-hop patterns")

    # Should find 2 paths
    assert result.num_matches() == 2

    table = result.result_table()
    paths = set(zip(
        table.column('a_id').to_pylist(),
        table.column('b_id').to_pylist(),
        table.column('c_id').to_pylist(),
        table.column('d_id').to_pylist()
    ))

    expected = {(0, 1, 2, 3), (0, 1, 2, 4)}
    print(f"Paths: {paths}")
    assert paths == expected

    print("✅ Test passed!")


def test_3hop_empty():
    """Test 3-hop on empty graph"""
    print("\n=== Test 10: 3-hop Empty Graph ===")

    edges = pa.table({
        'source': pa.array([], type=pa.int64()),
        'target': pa.array([], type=pa.int64())
    })

    result = match_3hop(edges, edges, edges)

    assert result.num_matches() == 0
    assert result.result_table().num_rows == 0

    print("✅ Test passed!")


def run_all_tests():
    """Run all pattern matching tests"""
    tests = [
        test_basic_2hop_pattern,
        test_2hop_custom_names,
        test_2hop_branching_graph,
        test_2hop_cycle,
        test_2hop_disconnected,
        test_2hop_empty_graph,
        test_2hop_single_edge,
        test_3hop_pattern,
        test_3hop_branching,
        test_3hop_empty,
    ]

    print("=" * 60)
    print("Pattern Matching Test Suite")
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
