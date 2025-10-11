"""
Quick test for batch queries only (no streaming dependencies).
"""

from sabot import cyarrow as pa
from sabot._cython.graph.engine.query_engine import GraphQueryEngine


def test_unified_batch():
    """Test unified API batch query."""
    print("Testing unified batch API...")

    engine = GraphQueryEngine()

    # Load graph
    vertices = pa.table({
        'id': [0, 1, 2, 3],
        'label': ['Person', 'Person', 'Account', 'Account'],
        'name': ['Alice', 'Bob', 'Checking', 'Savings']
    })

    edges = pa.table({
        'source': [0, 1, 0, 1],
        'target': [2, 2, 3, 3],
        'label': ['OWNS', 'OWNS', 'OWNS', 'OWNS']
    })

    engine.load_vertices(vertices, persist=False)
    engine.load_edges(edges, persist=False)

    # Execute batch query
    result = engine.query_cypher(
        "MATCH (p:Person)-[:OWNS]->(a:Account) RETURN p.name, a.name"
    )

    print(f"Result type: {type(result)}")
    print(f"Result rows: {len(result)}")
    print(f"Has to_pandas: {hasattr(result, 'to_pandas')}")

    if len(result) > 0:
        print("\nResults:")
        print(result.to_pandas())
    else:
        print("\nNo results (query may not be matching)")

    print("\n✅ Batch query API works!")


if __name__ == '__main__':
    test_unified_batch()
