#!/usr/bin/env python3
"""
Manual test for property path operators.

Verifies algorithm logic before building C++ version.
Implements the three operators in pure Python to match C++ implementation.
"""

import pyarrow as pa


def sequence_path_python(path_p, path_q):
    """
    Python implementation of sequence path (p/q).

    Hash join on intermediate node: path_p.end = path_q.start
    """
    # Build hash map: intermediate -> list of starts
    intermediate_to_starts = {}
    for i in range(path_p.num_rows):
        start = path_p['start'][i].as_py()
        intermediate = path_p['end'][i].as_py()
        if intermediate not in intermediate_to_starts:
            intermediate_to_starts[intermediate] = []
        intermediate_to_starts[intermediate].append(start)

    # Join on intermediate node
    start_results = []
    end_results = []

    for i in range(path_q.num_rows):
        intermediate = path_q['start'][i].as_py()
        end = path_q['end'][i].as_py()

        if intermediate in intermediate_to_starts:
            for start in intermediate_to_starts[intermediate]:
                start_results.append(start)
                end_results.append(end)

    return pa.RecordBatch.from_pydict({
        'start': pa.array(start_results, type=pa.int64()),
        'end': pa.array(end_results, type=pa.int64())
    })


def alternative_path_python(path_p, path_q):
    """
    Python implementation of alternative path (p|q).

    Union with deduplication using set.
    """
    unique_pairs = set()

    # Add pairs from path_p
    for i in range(path_p.num_rows):
        start = path_p['start'][i].as_py()
        end = path_p['end'][i].as_py()
        unique_pairs.add((start, end))

    # Add pairs from path_q
    for i in range(path_q.num_rows):
        start = path_q['start'][i].as_py()
        end = path_q['end'][i].as_py()
        unique_pairs.add((start, end))

    # Convert to arrays
    start_results = []
    end_results = []
    for start, end in unique_pairs:
        start_results.append(start)
        end_results.append(end)

    return pa.RecordBatch.from_pydict({
        'start': pa.array(start_results, type=pa.int64()),
        'end': pa.array(end_results, type=pa.int64())
    })


def filter_by_predicate_python(edges, excluded_predicates, predicate_col='predicate'):
    """
    Python implementation of negated property set (!p).

    Filter edges excluding specific predicates.
    """
    excluded_set = set(excluded_predicates)

    # Filter mask
    mask = []
    for i in range(edges.num_rows):
        predicate = edges[predicate_col][i].as_py()
        include = predicate not in excluded_set
        mask.append(include)

    # Apply filter
    mask_array = pa.array(mask, type=pa.bool_())
    import pyarrow.compute as pc
    result = pc.filter(edges, mask_array)

    return result


def test_sequence_simple():
    """Test: 0 --p--> 1 --q--> 2"""
    print("=" * 60)
    print("Test: Sequence Path (p/q) - Simple")
    print("=" * 60)

    path_p = pa.RecordBatch.from_pydict({
        'start': pa.array([0], type=pa.int64()),
        'end': pa.array([1], type=pa.int64())
    })

    path_q = pa.RecordBatch.from_pydict({
        'start': pa.array([1], type=pa.int64()),
        'end': pa.array([2], type=pa.int64())
    })

    result = sequence_path_python(path_p, path_q)

    print(f"\nPath p: 0 → 1")
    print(f"Path q: 1 → 2")
    print(f"\nResult ({result.num_rows} rows):")
    for i in range(result.num_rows):
        print(f"  {result['start'][i].as_py()} → {result['end'][i].as_py()}")

    # Verify
    assert result.num_rows == 1
    assert result['start'][0].as_py() == 0
    assert result['end'][0].as_py() == 2
    print("\n✅ Sequence simple test passed!")


def test_sequence_branching():
    """Test: 0 --p--> {1, 2}, {1, 2} --q--> {3, 4}"""
    print("\n" + "=" * 60)
    print("Test: Sequence Path (p/q) - Branching")
    print("=" * 60)

    path_p = pa.RecordBatch.from_pydict({
        'start': pa.array([0, 0], type=pa.int64()),
        'end': pa.array([1, 2], type=pa.int64())
    })

    path_q = pa.RecordBatch.from_pydict({
        'start': pa.array([1, 2], type=pa.int64()),
        'end': pa.array([3, 4], type=pa.int64())
    })

    result = sequence_path_python(path_p, path_q)

    print(f"\nPath p: 0 → {{1, 2}}")
    print(f"Path q: 1 → 3, 2 → 4")
    print(f"\nResult ({result.num_rows} rows):")
    for i in range(result.num_rows):
        print(f"  {result['start'][i].as_py()} → {result['end'][i].as_py()}")

    # Verify
    pairs = set(zip(result['start'].to_pylist(), result['end'].to_pylist()))
    expected = {(0, 3), (0, 4)}
    assert pairs == expected
    print("\n✅ Sequence branching test passed!")


def test_sequence_no_match():
    """Test: Disconnected paths"""
    print("\n" + "=" * 60)
    print("Test: Sequence Path (p/q) - No Match")
    print("=" * 60)

    path_p = pa.RecordBatch.from_pydict({
        'start': pa.array([0], type=pa.int64()),
        'end': pa.array([1], type=pa.int64())
    })

    path_q = pa.RecordBatch.from_pydict({
        'start': pa.array([2], type=pa.int64()),  # Disconnected!
        'end': pa.array([3], type=pa.int64())
    })

    result = sequence_path_python(path_p, path_q)

    print(f"\nPath p: 0 → 1")
    print(f"Path q: 2 → 3 (disconnected)")
    print(f"\nResult: {result.num_rows} rows (empty)")

    # Verify
    assert result.num_rows == 0
    print("\n✅ Sequence no-match test passed!")


def test_alternative_simple():
    """Test: 0 --p--> 1, 0 --q--> 2"""
    print("\n" + "=" * 60)
    print("Test: Alternative Path (p|q) - Simple")
    print("=" * 60)

    path_p = pa.RecordBatch.from_pydict({
        'start': pa.array([0], type=pa.int64()),
        'end': pa.array([1], type=pa.int64())
    })

    path_q = pa.RecordBatch.from_pydict({
        'start': pa.array([0], type=pa.int64()),
        'end': pa.array([2], type=pa.int64())
    })

    result = alternative_path_python(path_p, path_q)

    print(f"\nPath p: 0 → 1")
    print(f"Path q: 0 → 2")
    print(f"\nResult ({result.num_rows} rows):")
    for i in range(result.num_rows):
        print(f"  {result['start'][i].as_py()} → {result['end'][i].as_py()}")

    # Verify
    pairs = set(zip(result['start'].to_pylist(), result['end'].to_pylist()))
    expected = {(0, 1), (0, 2)}
    assert pairs == expected
    print("\n✅ Alternative simple test passed!")


def test_alternative_duplicates():
    """Test: Deduplication"""
    print("\n" + "=" * 60)
    print("Test: Alternative Path (p|q) - Duplicates")
    print("=" * 60)

    path_p = pa.RecordBatch.from_pydict({
        'start': pa.array([0], type=pa.int64()),
        'end': pa.array([1], type=pa.int64())
    })

    path_q = pa.RecordBatch.from_pydict({
        'start': pa.array([0], type=pa.int64()),
        'end': pa.array([1], type=pa.int64())  # Same as path_p!
    })

    result = alternative_path_python(path_p, path_q)

    print(f"\nPath p: 0 → 1")
    print(f"Path q: 0 → 1 (duplicate)")
    print(f"\nResult ({result.num_rows} rows):")
    for i in range(result.num_rows):
        print(f"  {result['start'][i].as_py()} → {result['end'][i].as_py()}")

    # Verify deduplication
    assert result.num_rows == 1
    assert result['start'][0].as_py() == 0
    assert result['end'][0].as_py() == 1
    print("\n✅ Alternative deduplication test passed!")


def test_filter_simple():
    """Test: !p2 excludes predicate 20"""
    print("\n" + "=" * 60)
    print("Test: Filter by Predicate (!p) - Simple")
    print("=" * 60)

    edges = pa.RecordBatch.from_pydict({
        'start': pa.array([0, 0, 0], type=pa.int64()),
        'predicate': pa.array([10, 20, 30], type=pa.int64()),
        'end': pa.array([1, 2, 3], type=pa.int64())
    })

    result = filter_by_predicate_python(edges, [20])

    print(f"\nEdges: (0,p10,1), (0,p20,2), (0,p30,3)")
    print(f"Exclude: p20")
    print(f"\nResult ({result.num_rows} rows):")
    for i in range(result.num_rows):
        print(f"  ({result['start'][i].as_py()}, p{result['predicate'][i].as_py()}, {result['end'][i].as_py()})")

    # Verify
    assert result.num_rows == 2
    pairs = set(zip(result['start'].to_pylist(), result['end'].to_pylist()))
    expected = {(0, 1), (0, 3)}
    assert pairs == expected
    print("\n✅ Filter simple test passed!")


def test_filter_multiple():
    """Test: Exclude multiple predicates"""
    print("\n" + "=" * 60)
    print("Test: Filter by Predicate (!p) - Multiple")
    print("=" * 60)

    edges = pa.RecordBatch.from_pydict({
        'start': pa.array([0, 0, 0, 0], type=pa.int64()),
        'predicate': pa.array([10, 20, 30, 40], type=pa.int64()),
        'end': pa.array([1, 2, 3, 4], type=pa.int64())
    })

    result = filter_by_predicate_python(edges, [20, 30])

    print(f"\nEdges: (0,p10,1), (0,p20,2), (0,p30,3), (0,p40,4)")
    print(f"Exclude: p20, p30")
    print(f"\nResult ({result.num_rows} rows):")
    for i in range(result.num_rows):
        print(f"  ({result['start'][i].as_py()}, p{result['predicate'][i].as_py()}, {result['end'][i].as_py()})")

    # Verify
    assert result.num_rows == 2
    remaining_preds = set(result['predicate'].to_pylist())
    expected = {10, 40}
    assert remaining_preds == expected
    print("\n✅ Filter multiple test passed!")


if __name__ == "__main__":
    # Sequence path tests
    test_sequence_simple()
    test_sequence_branching()
    test_sequence_no_match()

    # Alternative path tests
    test_alternative_simple()
    test_alternative_duplicates()

    # Filter tests
    test_filter_simple()
    test_filter_multiple()

    print("\n" + "=" * 60)
    print("All property path operator tests passed!")
    print("=" * 60)
    print("\nAlgorithms verified! Ready for C++ implementation.")
