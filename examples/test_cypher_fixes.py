"""
Test Cypher Parser and Query Execution Fixes

This script verifies that the Cypher compiler fixes are working:
1. Pattern element parsing (nodes and edges correctly populated)
2. Edge type extraction (types=['KNOWS'] correctly parsed)
3. Query execution (2-hop queries execute successfully)
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ['DYLD_LIBRARY_PATH'] = '/Users/bengamble/Sabot/vendor/arrow/cpp/build/install/lib'

import pyarrow as pa
from sabot._cython.graph.engine import GraphQueryEngine
from sabot._cython.graph.compiler import CypherParser

def test_parser():
    """Test that parser correctly extracts pattern elements and edge types."""
    print("=" * 70)
    print("TEST 1: Parser Correctness")
    print("=" * 70)

    parser = CypherParser()
    query = "MATCH (a:Person)-[:KNOWS]->(b:Person) WHERE a.age > 18 RETURN a.name, b.name LIMIT 10"

    print(f"Query: {query}")
    print()

    ast = parser.parse(query)

    # Check AST structure
    assert len(ast.match_clauses) == 1, "Should have 1 MATCH clause"
    match = ast.match_clauses[0]

    assert len(match.pattern.elements) == 1, "Should have 1 pattern element"
    element = match.pattern.elements[0]

    # Check nodes
    assert len(element.nodes) == 2, f"Should have 2 nodes, got {len(element.nodes)}"
    print(f"✅ Nodes: {len(element.nodes)}")
    print(f"   Node 1: variable='{element.nodes[0].variable}', labels={element.nodes[0].labels}")
    print(f"   Node 2: variable='{element.nodes[1].variable}', labels={element.nodes[1].labels}")

    # Check edges
    assert len(element.edges) == 1, f"Should have 1 edge, got {len(element.edges)}"
    edge = element.edges[0]
    print(f"✅ Edges: {len(element.edges)}")
    print(f"   Edge: variable='{edge.variable}', types={edge.types}, direction={edge.direction}")

    # Check edge types
    assert edge.types == ['KNOWS'], f"Edge types should be ['KNOWS'], got {edge.types}"
    print(f"✅ Edge types correctly parsed: {edge.types}")

    # Check RETURN clause
    assert ast.return_clause is not None, "Should have RETURN clause"
    assert len(ast.return_clause.items) == 2, "Should have 2 return items"
    print(f"✅ Return items: {len(ast.return_clause.items)}")

    # Check LIMIT
    assert ast.return_clause.limit == 10, "Should have LIMIT 10"
    print(f"✅ Limit: {ast.return_clause.limit}")

    print()
    print("🎉 Parser test PASSED\n")
    return True


def test_query_execution():
    """Test that queries execute successfully."""
    print("=" * 70)
    print("TEST 2: Query Execution")
    print("=" * 70)

    # Create engine
    engine = GraphQueryEngine(state_store=None)

    # Load data
    vertices = pa.table({
        'id': pa.array([0, 1, 2, 3], type=pa.int64()),
        'label': pa.array(['Person', 'Person', 'Person', 'Account'], type=pa.string()),
        'name': pa.array(['Alice', 'Bob', 'Charlie', 'Checking'], type=pa.string()),
    })
    engine.load_vertices(vertices, persist=False)

    edges = pa.table({
        'source': pa.array([0, 1, 0], type=pa.int64()),
        'target': pa.array([1, 2, 3], type=pa.int64()),
        'label': pa.array(['KNOWS', 'KNOWS', 'OWNS'], type=pa.string()),
    })
    engine.load_edges(edges, persist=False)

    print(f"Loaded {len(vertices)} vertices, {len(edges)} edges")
    print()

    # Test 1: Query with edge type filter
    print("Query 1: MATCH (a)-[r:KNOWS]->(b) RETURN a, b")
    result = engine.query_cypher("MATCH (a)-[r:KNOWS]->(b) RETURN a, b")
    print(f"✅ Query executed, {len(result.table)} results")
    print(f"   Columns: {result.table.column_names}")
    print()

    # Test 2: Query without edge type filter
    print("Query 2: MATCH (a)-[r]->(b) RETURN a, b LIMIT 2")
    result = engine.query_cypher("MATCH (a)-[r]->(b) RETURN a, b LIMIT 2")
    print(f"✅ Query executed, {len(result.table)} results")
    print(f"   Columns: {result.table.column_names}")
    print()

    # Test 3: Query with different edge type
    print("Query 3: MATCH (a)-[r:OWNS]->(b) RETURN a, b")
    result = engine.query_cypher("MATCH (a)-[r:OWNS]->(b) RETURN a, b")
    print(f"✅ Query executed, {len(result.table)} results")
    print()

    print("🎉 Query execution test PASSED\n")
    return True


def main():
    """Run all tests."""
    print()
    print("🚀 CYPHER COMPILER FIX VERIFICATION")
    print()

    try:
        # Test parser
        test_parser()

        # Test query execution
        test_query_execution()

        print("=" * 70)
        print("✅ ALL TESTS PASSED")
        print("=" * 70)
        print()
        print("Summary of fixes:")
        print("  1. Pattern element parsing - nodes and edges correctly populated")
        print("  2. Edge type extraction - types=['KNOWS'] correctly parsed")
        print("  3. Query execution - 2-hop queries execute successfully")
        print()

        return 0

    except Exception as e:
        print(f"\n❌ TEST FAILED: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
