"""
Test suite for variable-length path matching.

Tests the ability to find all paths within a hop range.
"""
import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Set library path for Arrow
os.environ['DYLD_LIBRARY_PATH'] = '/Users/bengamble/Sabot/vendor/arrow/cpp/build/install/lib'

import pyarrow as pa
from sabot._cython.graph.query import match_variable_length_path, PyPatternMatchResult


def test_basic_variable_length():
    """Test basic variable-length path matching"""
    print("\n=== Test 1: Basic Variable-Length Path ===")

    # Simple chain: 0->1->2->3->4
    edges = pa.table({
        'source': pa.array([0, 1, 2, 3], type=pa.int64()),
        'target': pa.array([1, 2, 3, 4], type=pa.int64())
    })

    # Find all paths of length 2-3 from vertex 0
    result = match_variable_length_path(
        edges,
        source_vertex=0,
        target_vertex=-1,  # Any target
        min_hops=2,
        max_hops=3
    )

    print(f"Found {result.num_matches()} paths of length 2-3 from vertex 0")
    print(f"Result table:\n{result.result_table()}")

    # Should find:
    # - Length 2: 0->1->2
    # - Length 3: 0->1->2->3
    assert result.num_matches() == 2

    table = result.result_table()
    hop_counts = table.column('hop_count').to_pylist()
    assert set(hop_counts) == {2, 3}

    print("✅ Test passed!")


def test_specific_target():
    """Test variable-length path to specific target"""
    print("\n=== Test 2: Specific Target Vertex ===")

    # Chain: 0->1->2->3->4->5
    edges = pa.table({
        'source': pa.array([0, 1, 2, 3, 4], type=pa.int64()),
        'target': pa.array([1, 2, 3, 4, 5], type=pa.int64())
    })

    # Find paths from 0 to 5 with length 3-5
    result = match_variable_length_path(
        edges,
        source_vertex=0,
        target_vertex=5,
        min_hops=3,
        max_hops=5
    )

    print(f"Found {result.num_matches()} paths from 0 to 5 (length 3-5)")

    # Only one path: 0->1->2->3->4->5 (length 5)
    assert result.num_matches() == 1

    table = result.result_table()
    hop_count = table.column('hop_count').to_pylist()[0]
    vertices_list = table.column('vertices').to_pylist()[0]

    assert hop_count == 5
    assert vertices_list == [0, 1, 2, 3, 4, 5]

    print(f"Path: {vertices_list}")
    print("✅ Test passed!")


def test_branching_graph():
    """Test variable-length paths in branching graph"""
    print("\n=== Test 3: Branching Graph ===")

    # Graph: 0->1, 0->2, 1->3, 2->3, 3->4
    edges = pa.table({
        'source': pa.array([0, 0, 1, 2, 3], type=pa.int64()),
        'target': pa.array([1, 2, 3, 3, 4], type=pa.int64())
    })

    # Find all 2-hop paths from 0
    result = match_variable_length_path(
        edges,
        source_vertex=0,
        target_vertex=-1,
        min_hops=2,
        max_hops=2
    )

    print(f"Found {result.num_matches()} 2-hop paths from vertex 0")

    # Should find: 0->1->3, 0->2->3
    assert result.num_matches() == 2

    table = result.result_table()
    vertices_lists = table.column('vertices').to_pylist()

    expected_paths = [
        [0, 1, 3],
        [0, 2, 3]
    ]

    print(f"Paths: {vertices_lists}")
    assert len(vertices_lists) == 2
    for path in vertices_lists:
        assert path in expected_paths

    print("✅ Test passed!")


def test_multiple_paths_to_target():
    """Test finding multiple paths to same target"""
    print("\n=== Test 4: Multiple Paths to Target ===")

    # Diamond graph: 0->1, 0->2, 1->3, 2->3
    edges = pa.table({
        'source': pa.array([0, 0, 1, 2], type=pa.int64()),
        'target': pa.array([1, 2, 3, 3], type=pa.int64())
    })

    # Find all paths from 0 to 3
    result = match_variable_length_path(
        edges,
        source_vertex=0,
        target_vertex=3,
        min_hops=1,
        max_hops=2
    )

    print(f"Found {result.num_matches()} paths from 0 to 3")

    # Should find 2 paths of length 2: 0->1->3, 0->2->3
    assert result.num_matches() == 2

    table = result.result_table()
    vertices_lists = table.column('vertices').to_pylist()

    print(f"Paths: {vertices_lists}")
    assert set(map(tuple, vertices_lists)) == {(0, 1, 3), (0, 2, 3)}

    print("✅ Test passed!")


def test_cycle_detection():
    """Test variable-length paths in graph with cycles"""
    print("\n=== Test 5: Graph with Cycles ===")

    # Cycle: 0->1->2->0
    edges = pa.table({
        'source': pa.array([0, 1, 2], type=pa.int64()),
        'target': pa.array([1, 2, 0], type=pa.int64())
    })

    # Find all paths of length 1-3 from 0
    # (Note: may include paths that revisit vertices)
    result = match_variable_length_path(
        edges,
        source_vertex=0,
        target_vertex=-1,
        min_hops=1,
        max_hops=3
    )

    print(f"Found {result.num_matches()} paths in cyclic graph")

    table = result.result_table()
    print(f"Result table:\n{table}")

    # With cycles, we can have:
    # Len 1: 0->1
    # Len 2: 0->1->2
    # Len 3: 0->1->2->0
    assert result.num_matches() >= 3

    print("✅ Test passed!")


def test_no_paths():
    """Test when no paths exist"""
    print("\n=== Test 6: No Paths ===")

    # Disconnected graph: 0->1, 3->4
    edges = pa.table({
        'source': pa.array([0, 3], type=pa.int64()),
        'target': pa.array([1, 4], type=pa.int64())
    })

    # Try to find path from 0 to 4 (doesn't exist)
    result = match_variable_length_path(
        edges,
        source_vertex=0,
        target_vertex=4,
        min_hops=1,
        max_hops=5
    )

    print(f"Found {result.num_matches()} paths (should be 0)")
    assert result.num_matches() == 0

    print("✅ Test passed!")


def test_exact_length():
    """Test finding paths of exact length"""
    print("\n=== Test 7: Exact Length Paths ===")

    # Chain: 0->1->2->3->4
    edges = pa.table({
        'source': pa.array([0, 1, 2, 3], type=pa.int64()),
        'target': pa.array([1, 2, 3, 4], type=pa.int64())
    })

    # Find paths of exactly length 3
    result = match_variable_length_path(
        edges,
        source_vertex=0,
        target_vertex=-1,
        min_hops=3,
        max_hops=3
    )

    print(f"Found {result.num_matches()} paths of exactly length 3")

    # Should find only: 0->1->2->3
    assert result.num_matches() == 1

    table = result.result_table()
    vertices_list = table.column('vertices').to_pylist()[0]
    hop_count = table.column('hop_count').to_pylist()[0]

    assert hop_count == 3
    assert vertices_list == [0, 1, 2, 3]

    print(f"Path: {vertices_list}")
    print("✅ Test passed!")


def test_edge_indices():
    """Test that edge indices are correctly tracked"""
    print("\n=== Test 8: Edge Index Tracking ===")

    # Simple path: 0->1->2
    edges = pa.table({
        'source': pa.array([0, 1], type=pa.int64()),
        'target': pa.array([1, 2], type=pa.int64())
    })

    result = match_variable_length_path(
        edges,
        source_vertex=0,
        target_vertex=2,
        min_hops=2,
        max_hops=2
    )

    print(f"Found {result.num_matches()} paths")

    table = result.result_table()
    edge_indices = table.column('edges').to_pylist()[0]

    # Should traverse edges 0 and 1
    assert edge_indices == [0, 1]

    print(f"Edge indices: {edge_indices}")
    print("✅ Test passed!")


def test_empty_graph():
    """Test on empty graph"""
    print("\n=== Test 9: Empty Graph ===")

    edges = pa.table({
        'source': pa.array([], type=pa.int64()),
        'target': pa.array([], type=pa.int64())
    })

    result = match_variable_length_path(
        edges,
        source_vertex=0,
        target_vertex=-1,
        min_hops=1,
        max_hops=3
    )

    assert result.num_matches() == 0
    print("✅ Test passed!")


def test_max_hop_limit():
    """Test respecting max hop limit"""
    print("\n=== Test 10: Max Hop Limit ===")

    # Long chain: 0->1->2->3->4->5->6->7->8->9->10
    edges = pa.table({
        'source': pa.array(list(range(10)), type=pa.int64()),
        'target': pa.array(list(range(1, 11)), type=pa.int64())
    })

    # Find paths up to length 5
    result = match_variable_length_path(
        edges,
        source_vertex=0,
        target_vertex=-1,
        min_hops=1,
        max_hops=5
    )

    print(f"Found {result.num_matches()} paths (max length 5)")

    table = result.result_table()
    hop_counts = table.column('hop_count').to_pylist()

    # Should have paths of length 1, 2, 3, 4, 5
    assert set(hop_counts) == {1, 2, 3, 4, 5}
    assert result.num_matches() == 5

    print("✅ Test passed!")


def run_all_tests():
    """Run all variable-length path tests"""
    tests = [
        test_basic_variable_length,
        test_specific_target,
        test_branching_graph,
        test_multiple_paths_to_target,
        test_cycle_detection,
        test_no_paths,
        test_exact_length,
        test_edge_indices,
        test_empty_graph,
        test_max_hop_limit,
    ]

    print("=" * 60)
    print("Variable-Length Path Matching Test Suite")
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
