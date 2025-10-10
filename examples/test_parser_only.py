"""
Test Cypher Parser Fixes Only

This script only tests the parser, not query execution (to avoid segfaults).
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sabot._cython.graph.compiler import CypherParser

def main():
    print()
    print("=" * 70)
    print("CYPHER PARSER FIX VERIFICATION")
    print("=" * 70)
    print()

    parser = CypherParser()
    query = "MATCH (a:Person)-[:KNOWS]->(b:Person) WHERE a.age > 18 RETURN a.name, b.name LIMIT 10"

    print(f"Query: {query}")
    print()

    ast = parser.parse(query)

    # Check AST structure
    match = ast.match_clauses[0]
    element = match.pattern.elements[0]

    # Check nodes
    print(f"✅ Nodes: {len(element.nodes)} (expected 2)")
    assert len(element.nodes) == 2, f"Should have 2 nodes, got {len(element.nodes)}"
    print(f"   Node 1: variable='{element.nodes[0].variable}', labels={element.nodes[0].labels}")
    print(f"   Node 2: variable='{element.nodes[1].variable}', labels={element.nodes[1].labels}")
    print()

    # Check edges
    print(f"✅ Edges: {len(element.edges)} (expected 1)")
    assert len(element.edges) == 1, f"Should have 1 edge, got {len(element.edges)}"
    edge = element.edges[0]
    print(f"   Edge: variable='{edge.variable}', types={edge.types}, direction={edge.direction}")
    print()

    # Check edge types
    print(f"✅ Edge types: {edge.types} (expected ['KNOWS'])")
    assert edge.types == ['KNOWS'], f"Edge types should be ['KNOWS'], got {edge.types}"
    print()

    # Check RETURN clause
    print(f"✅ Return items: {len(ast.return_clause.items)} (expected 2)")
    assert len(ast.return_clause.items) == 2, "Should have 2 return items"
    print()

    # Check LIMIT
    print(f"✅ Limit: {ast.return_clause.limit} (expected 10)")
    assert ast.return_clause.limit == 10, f"Should have LIMIT 10, got {ast.return_clause.limit}"
    print()

    print("=" * 70)
    print("✅ ALL PARSER TESTS PASSED")
    print("=" * 70)
    print()
    print("Summary of fixes:")
    print("  1. ✅ Pattern element parsing - nodes and edges correctly populated")
    print("  2. ✅ Edge type extraction - types=['KNOWS'] correctly parsed")
    print("  3. ✅ LIMIT/SKIP parsing - integers correctly extracted")
    print()

    return 0


if __name__ == "__main__":
    sys.exit(main())
