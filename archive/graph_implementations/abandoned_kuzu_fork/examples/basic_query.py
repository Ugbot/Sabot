#!/usr/bin/env python3
"""
Basic SabotCypher Query Example

Demonstrates simple graph queries using SabotCypher.
"""

import sys
sys.path.insert(0, '/Users/bengamble/Sabot')

import pyarrow as pa

# Import sabot_cypher (will use placeholder until built)
try:
    import sabot_cypher
    print(f"SabotCypher version: {sabot_cypher.__version__}")
    print(f"Native available: {sabot_cypher.is_native_available()}")
    print()
except ImportError as e:
    print(f"Error importing sabot_cypher: {e}")
    sys.exit(1)

def main():
    """Run basic query example."""
    
    # Create sample graph data
    print("Creating sample graph...")
    
    vertices = pa.table({
        'id': pa.array([1, 2, 3, 4, 5], type=pa.int64()),
        'label': pa.array(['Person', 'Person', 'Person', 'City', 'Interest']),
        'name': pa.array(['Alice', 'Bob', 'Charlie', 'NYC', 'Tennis']),
        'age': pa.array([25, 30, 35, None, None], type=pa.int32()),
    })
    
    edges = pa.table({
        'source': pa.array([1, 1, 2, 3, 1], type=pa.int64()),
        'target': pa.array([2, 3, 3, 4, 5], type=pa.int64()),
        'type': pa.array(['KNOWS', 'KNOWS', 'KNOWS', 'LIVES_IN', 'INTERESTED_IN']),
    })
    
    print(f"  Vertices: {vertices.num_rows}")
    print(f"  Edges: {edges.num_rows}")
    print()
    
    # Create bridge
    print("Creating SabotCypher bridge...")
    try:
        bridge = sabot_cypher.SabotCypherBridge.create()
        print("  ✅ Bridge created")
    except NotImplementedError as e:
        print(f"  ⚠️ {e}")
        print()
        print("This is a placeholder example.")
        print("Once SabotCypher is built, this will execute real queries!")
        return
    
    # Register graph
    print("Registering graph...")
    bridge.register_graph(vertices, edges)
    print("  ✅ Graph registered")
    print()
    
    # Execute queries
    queries = [
        ("Simple match", "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name, b.name"),
        ("Count query", "MATCH (p:Person) RETURN count(*) AS person_count"),
        ("Aggregation", "MATCH (p:Person)-[:KNOWS]->(f) RETURN p.name, count(f) AS friends"),
        ("Filtering", "MATCH (p:Person) WHERE p.age > 25 RETURN p.name, p.age ORDER BY p.age"),
    ]
    
    for description, query in queries:
        print(f"Query: {description}")
        print(f"  {query}")
        
        try:
            result = bridge.execute(query)
            print(f"  ✅ {result.num_rows} rows in {result.execution_time_ms:.2f}ms")
            print(f"  {result.table}")
        except Exception as e:
            print(f"  ❌ Error: {e}")
        
        print()

if __name__ == "__main__":
    main()

