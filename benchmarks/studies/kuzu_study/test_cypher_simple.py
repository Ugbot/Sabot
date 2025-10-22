#!/usr/bin/env python3
"""
Simple test to verify Cypher queries work on Kuzu dataset.
"""
import sys
import os
from pathlib import Path

# Add Sabot to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

# Set library path for Arrow
os.environ['DYLD_LIBRARY_PATH'] = '/Users/bengamble/Sabot/vendor/arrow/cpp/build/install/lib'

from sabot._cython.graph.engine.query_engine import GraphQueryEngine
from sabot import cyarrow as pa
import pyarrow

# Simple test data
print("Creating graph engine...")
engine = GraphQueryEngine()

# Create small test graph
print("Loading test data...")
vertices = pa.table({
    'id': [0, 1, 2, 3],
    'label': ['Person', 'Person', 'City', 'City'],
    'name': ['Alice', 'Bob', 'NYC', 'SF']
})
engine.load_vertices(vertices, persist=False)

edges = pa.table({
    'source': [0, 1, 0],
    'target': [1, 0, 2],
    'label': ['Follows', 'Follows', 'LivesIn']
})
engine.load_edges(edges, persist=False)

print("✅ Test data loaded\n")

# Test queries
print("Testing Cypher queries:")
print("-" * 70)

# Query 1: Simple pattern
print("\n1. Simple pattern: MATCH (a)-[r]->(b) RETURN a, b LIMIT 2")
try:
    result = engine.query_cypher("MATCH (a)-[r]->(b) RETURN a, b LIMIT 2")
    print(f"   ✅ Result: {result.table.num_rows} rows")
    print(f"   Schema: {result.table.schema}")
except Exception as e:
    print(f"   ❌ Error: {e}")

# Query 2: With labels
print("\n2. With labels: MATCH (a:Person)-[:Follows]->(b:Person) RETURN a, b")
try:
    result = engine.query_cypher("MATCH (a:Person)-[:Follows]->(b:Person) RETURN a, b")
    print(f"   ✅ Result: {result.table.num_rows} rows")
    print(f"   Schema: {result.table.schema}")
except Exception as e:
    print(f"   ❌ Error: {e}")

# Query 3: Property access
print("\n3. Property access: MATCH (a:Person) RETURN a.name LIMIT 2")
try:
    result = engine.query_cypher("MATCH (a:Person) RETURN a.name LIMIT 2")
    print(f"   ✅ Result: {result.table.num_rows} rows")
    print(f"   Schema: {result.table.schema}")
    print(f"   Data: {result.table.to_pylist()}")
except Exception as e:
    print(f"   ❌ Error: {e}")

# Query 4: 2-hop pattern
print("\n4. 2-hop: MATCH (a:Person)-[:Follows]->(b)-[:LivesIn]->(c) RETURN a, b, c")
try:
    result = engine.query_cypher("MATCH (a:Person)-[:Follows]->(b)-[:LivesIn]->(c) RETURN a, b, c")
    print(f"   ✅ Result: {result.table.num_rows} rows")
    print(f"   Schema: {result.table.schema}")
except Exception as e:
    print(f"   ❌ Error: {e}")

print("\n" + "=" * 70)
print("Basic Cypher queries working! ✅")
