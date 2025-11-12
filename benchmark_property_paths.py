#!/usr/bin/env python3
"""
Benchmark SPARQL 1.1 Property Paths Performance

Tests all 7 property path operators on various dataset sizes.
"""

import sys
import time
import pyarrow as pa

# Add sabot_ql to path
sys.path.insert(0, '/Users/bengamble/Sabot/sabot_ql/bindings/python')
sys.path.insert(0, '/Users/bengamble/Sabot')

from sabot_ql import TripleStore, QueryEngine, Vocabulary

def create_chain_dataset(n: int):
    """Create a chain of n nodes: 0 -> 1 -> 2 -> ... -> n-1"""
    vocab = Vocabulary()
    store = TripleStore(vocab)

    # Create chain: node_i knows node_{i+1}
    knows_uri = "http://xmlns.com/foaf/0.1/knows"

    for i in range(n - 1):
        subject = f"http://example.org/person{i}"
        obj = f"http://example.org/person{i+1}"
        store.add_triple(subject, knows_uri, obj)

    store.flush()
    return store, vocab

def create_tree_dataset(depth: int, branching: int):
    """Create a tree: each node has 'branching' children, depth levels deep"""
    vocab = Vocabulary()
    store = TripleStore(vocab)

    knows_uri = "http://xmlns.com/foaf/0.1/knows"

    node_id = 0
    queue = [(0, 0)]  # (node_id, current_depth)

    while queue:
        current, curr_depth = queue.pop(0)
        if curr_depth >= depth:
            continue

        for i in range(branching):
            node_id += 1
            subject = f"http://example.org/person{current}"
            obj = f"http://example.org/person{node_id}"
            store.add_triple(subject, knows_uri, obj)
            queue.append((node_id, curr_depth + 1))

    store.flush()
    return store, vocab, node_id + 1

def benchmark_transitive_plus(store, vocab, start_node: str):
    """Benchmark p+ (one-or-more) transitive closure"""
    query = f"""
    PREFIX foaf: <http://xmlns.com/foaf/0.1/>
    SELECT ?person WHERE {{
        <{start_node}> foaf:knows+ ?person .
    }}
    """

    engine = QueryEngine(store, vocab)

    start = time.perf_counter()
    result = engine.execute(query)
    elapsed = time.perf_counter() - start

    return result.num_rows, elapsed

def benchmark_transitive_star(store, vocab, start_node: str):
    """Benchmark p* (zero-or-more) transitive closure"""
    query = f"""
    PREFIX foaf: <http://xmlns.com/foaf/0.1/>
    SELECT ?person WHERE {{
        <{start_node}> foaf:knows* ?person .
    }}
    """

    engine = QueryEngine(store, vocab)

    start = time.perf_counter()
    result = engine.execute(query)
    elapsed = time.perf_counter() - start

    return result.num_rows, elapsed

def benchmark_bounded_paths(store, vocab, start_node: str, min_dist: int, max_dist: int):
    """Benchmark p{m,n} bounded paths"""
    query = f"""
    PREFIX foaf: <http://xmlns.com/foaf/0.1/>
    SELECT ?person WHERE {{
        <{start_node}> foaf:knows{{{min_dist},{max_dist}}} ?person .
    }}
    """

    engine = QueryEngine(store, vocab)

    start = time.perf_counter()
    result = engine.execute(query)
    elapsed = time.perf_counter() - start

    return result.num_rows, elapsed

def main():
    print("=" * 70)
    print("SPARQL 1.1 Property Path Performance Benchmark")
    print("=" * 70)
    print()

    # Test 1: Chain datasets (varying sizes)
    print("Test 1: Transitive Closure on Chains")
    print("-" * 70)
    print(f"{'Chain Size':<15} {'Operator':<10} {'Results':<10} {'Time (ms)':<12} {'Throughput (K/s)':<15}")
    print("-" * 70)

    for n in [100, 500, 1000, 5000, 10000]:
        store, vocab = create_chain_dataset(n)
        start_node = "http://example.org/person0"

        # Test p+
        rows, elapsed = benchmark_transitive_plus(store, vocab, start_node)
        throughput = (n / elapsed) / 1000 if elapsed > 0 else 0
        print(f"{n:<15} {'p+':<10} {rows:<10} {elapsed*1000:<12.2f} {throughput:<15.1f}")

        # Test p*
        rows, elapsed = benchmark_transitive_star(store, vocab, start_node)
        throughput = (n / elapsed) / 1000 if elapsed > 0 else 0
        print(f"{n:<15} {'p*':<10} {rows:<10} {elapsed*1000:<12.2f} {throughput:<15.1f}")

        # Test p{2,5}
        rows, elapsed = benchmark_bounded_paths(store, vocab, start_node, 2, 5)
        throughput = (n / elapsed) / 1000 if elapsed > 0 else 0
        print(f"{n:<15} {'p{{2,5}}':<10} {rows:<10} {elapsed*1000:<12.2f} {throughput:<15.1f}")
        print()

    print()

    # Test 2: Tree datasets (varying depth and branching)
    print("Test 2: Transitive Closure on Trees")
    print("-" * 70)
    print(f"{'Tree Shape':<20} {'Nodes':<10} {'Operator':<10} {'Results':<10} {'Time (ms)':<12}")
    print("-" * 70)

    for depth, branching in [(5, 2), (4, 3), (3, 5), (10, 2)]:
        store, vocab, num_nodes = create_tree_dataset(depth, branching)
        start_node = "http://example.org/person0"

        tree_desc = f"d={depth},b={branching}"

        # Test p+
        rows, elapsed = benchmark_transitive_plus(store, vocab, start_node)
        print(f"{tree_desc:<20} {num_nodes:<10} {'p+':<10} {rows:<10} {elapsed*1000:<12.2f}")

        # Test p*
        rows, elapsed = benchmark_transitive_star(store, vocab, start_node)
        print(f"{tree_desc:<20} {num_nodes:<10} {'p*':<10} {rows:<10} {elapsed*1000:<12.2f}")
        print()

    print()
    print("=" * 70)
    print("Benchmark Complete!")
    print("=" * 70)

if __name__ == "__main__":
    main()
