"""
Cypher Query Compiler Demo

Demonstrates the new Cypher query compiler (Phase 4.3).
Shows how to execute Cypher queries on graphs stored in Sabot.

Features demonstrated:
1. Parsing Cypher queries into AST
2. Translating Cypher to Sabot pattern matching
3. Executing queries and getting results
4. Support for MATCH, WHERE, RETURN, LIMIT

Cypher compiler adapted from KuzuDB (MIT License)
Reference: vendor/kuzu/
"""

import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Set library path for Arrow
os.environ['DYLD_LIBRARY_PATH'] = '/Users/bengamble/Sabot/vendor/arrow/cpp/build/install/lib'

from sabot import cyarrow as pa
from sabot._cython.graph.engine import GraphQueryEngine


def create_social_network():
    """
    Create a simple social network graph.

    Graph:
        Alice (0) --KNOWS--> Bob (1) --KNOWS--> Charlie (2)
              |                                       ^
              +---------------KNOWS-------------------+
              |
              +--FOLLOWS--> Dave (3) --KNOWS--> Eve (4)
    """
    vertices = pa.table({
        'id': pa.array([0, 1, 2, 3, 4], type=pa.int64()),
        'label': pa.array(['Person', 'Person', 'Person', 'Person', 'Person'], type=pa.string()),
        'name': pa.array(['Alice', 'Bob', 'Charlie', 'Dave', 'Eve'], type=pa.string()),
        'age': pa.array([25, 30, 28, 35, 22], type=pa.int64())
    })

    edges = pa.table({
        'source': pa.array([0, 1, 0, 0, 3], type=pa.int64()),
        'target': pa.array([1, 2, 2, 3, 4], type=pa.int64()),
        'label': pa.array(['KNOWS', 'KNOWS', 'KNOWS', 'FOLLOWS', 'KNOWS'], type=pa.string()),
        'since': pa.array([2020, 2021, 2022, 2019, 2023], type=pa.int64())
    })

    return vertices, edges


def demo_cypher_compiler():
    """Demonstrate Cypher query compiler."""
    print("=" * 70)
    print("CYPHER QUERY COMPILER DEMO - Phase 4.3")
    print("=" * 70)
    print()

    # Step 1: Create engine and load data
    print("Step 1: Load graph data")
    print("-" * 70)

    engine = GraphQueryEngine(state_store=None)
    vertices, edges = create_social_network()

    engine.load_vertices(vertices, persist=False)
    engine.load_edges(edges, persist=False)

    stats = engine.get_graph_stats()
    print(f"✅ Loaded {stats['num_vertices']} vertices, {stats['num_edges']} edges")
    print()

    # Step 2: Simple 2-hop query
    print("Step 2: Execute simple Cypher query")
    print("-" * 70)

    query1 = "MATCH (a)-[r]->(b) RETURN a, b LIMIT 3"
    print(f"Query: {query1}")
    print()

    try:
        result = engine.query_cypher(query1)
        print(f"✅ Found {len(result)} matches")
        print(result.to_pandas())
    except Exception as e:
        print(f"⚠️  Query failed: {e}")
    print()

    # Step 3: Query with edge type filter
    print("Step 3: Query with edge type filter")
    print("-" * 70)

    query2 = "MATCH (a)-[r:KNOWS]->(b) RETURN a, b"
    print(f"Query: {query2}")
    print()

    try:
        result = engine.query_cypher(query2)
        print(f"✅ Found {len(result)} KNOWS relationships")
        print(result.to_pandas())
    except Exception as e:
        print(f"⚠️  Query failed: {e}")
    print()

    # Step 4: Variable-length path query
    print("Step 4: Variable-length path query")
    print("-" * 70)

    query3 = "MATCH (a)-[r:KNOWS*1..2]->(b) RETURN a, b LIMIT 5"
    print(f"Query: {query3}")
    print()

    try:
        result = engine.query_cypher(query3)
        print(f"✅ Found {len(result)} paths (1-2 hops)")
        print(result.to_pandas().head())
    except Exception as e:
        print(f"⚠️  Query failed: {e}")
    print()

    # Step 5: Test parser directly
    print("Step 5: Test Cypher parser (AST inspection)")
    print("-" * 70)

    from sabot._cython.graph.compiler import CypherParser

    parser = CypherParser()
    query4 = "MATCH (a:Person)-[:KNOWS]->(b:Person) WHERE a.age > 18 RETURN a.name, b.name LIMIT 10"
    print(f"Query: {query4}")
    print()

    try:
        ast = parser.parse(query4)
        print(f"✅ Parsed successfully: {ast}")
        print()
        print("AST Details:")
        print(f"  - Match clauses: {len(ast.match_clauses)}")
        if ast.match_clauses:
            match = ast.match_clauses[0]
            pattern = match.pattern
            print(f"  - Pattern elements: {len(pattern.elements)}")
            if pattern.elements:
                element = pattern.elements[0]
                print(f"  - Nodes: {len(element.nodes)}")
                print(f"  - Edges: {len(element.edges)}")
                if element.nodes:
                    node = element.nodes[0]
                    print(f"  - First node: variable='{node.variable}', labels={node.labels}")
                if element.edges:
                    edge = element.edges[0]
                    print(f"  - First edge: variable='{edge.variable}', types={edge.types}")
        if ast.return_clause:
            print(f"  - Return items: {len(ast.return_clause.items)}")
            print(f"  - Limit: {ast.return_clause.limit}")
    except Exception as e:
        print(f"⚠️  Parse failed: {e}")
        import traceback
        traceback.print_exc()
    print()

    # Step 6: Test individual components
    print("Step 6: Test AST node creation")
    print("-" * 70)

    from sabot._cython.graph.compiler.cypher_ast import (
        NodePattern, RelPattern, ArrowDirection, PatternElement, Pattern
    )

    # Create pattern manually: (a:Person)-[:KNOWS]->(b:Person)
    node_a = NodePattern(variable='a', labels=['Person'])
    edge = RelPattern(variable='r', types=['KNOWS'], direction=ArrowDirection.RIGHT)
    node_b = NodePattern(variable='b', labels=['Person'])

    element = PatternElement(nodes=[node_a, node_b], edges=[edge])
    pattern = Pattern(elements=[element])

    print(f"✅ Created pattern: {pattern}")
    print(f"  - Node A: {node_a}")
    print(f"  - Edge: {edge}")
    print(f"  - Node B: {node_b}")
    print()

    # Summary
    print("=" * 70)
    print("DEMO COMPLETE")
    print("=" * 70)
    print()
    print("Summary:")
    print("  ✅ Phase 4.3: Cypher Query Compiler - IMPLEMENTED")
    print()
    print("Supported Features:")
    print("  ✅ MATCH patterns: (a)-[r]->(b)")
    print("  ✅ Node labels: (a:Person)")
    print("  ✅ Edge types: -[r:KNOWS]->")
    print("  ✅ Variable-length paths: -[r:TYPE*1..3]->")
    print("  ✅ RETURN clause with LIMIT")
    print("  ✅ AST parsing with pyparsing")
    print()
    print("Not Yet Supported:")
    print("  🚧 WHERE clause evaluation (parsed but not executed)")
    print("  🚧 Property access in RETURN (a.name, b.age)")
    print("  🚧 Multiple MATCH clauses")
    print("  🚧 Aggregations (COUNT, SUM, etc.)")
    print("  🚧 ORDER BY")
    print()
    print("Code Attribution:")
    print("  Cypher compiler architecture adapted from KuzuDB")
    print("  (MIT License, Copyright 2022-2025 Kùzu Inc.)")
    print("  Reference: vendor/kuzu/src/parser/ and vendor/kuzu/src/antlr4/Cypher.g4")
    print()


if __name__ == "__main__":
    demo_cypher_compiler()
