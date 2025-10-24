#!/usr/bin/env python3
"""
Test SPARQL Query Execution
"""

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
os.environ['DYLD_LIBRARY_PATH'] = '/Users/bengamble/Sabot/vendor/arrow/cpp/build/install/lib'

from sabot import cyarrow as pa

print("=" * 70)
print("SPARQL Query Execution Test")
print("=" * 70)

# Test 1: Check what's available
print("\n[Test 1] Checking available components...")

try:
    from sabot._cython.graph.compiler import SPARQLParser
    print("  ✅ SPARQLParser available")
except ImportError as e:
    print(f"  ❌ SPARQLParser not available: {e}")
    sys.exit(1)

try:
    from sabot._cython.graph.engine import GraphQueryEngine
    print("  ✅ GraphQueryEngine available")
except ImportError as e:
    print(f"  ❌ GraphQueryEngine not available: {e}")
    sys.exit(1)

# Test 2: Parse a simple SPARQL query
print("\n[Test 2] Testing SPARQL parser...")

parser = SPARQLParser()

# Try a very simple query
simple_query = """
SELECT ?s ?p ?o
WHERE {
    ?s ?p ?o .
}
"""

print(f"Query: {simple_query.strip()}")

try:
    ast = parser.parse(simple_query)
    print(f"  ✅ Parsed successfully")
    print(f"  AST type: {type(ast)}")
    print(f"  Variables: {ast.select_clause.variables if hasattr(ast, 'select_clause') else 'N/A'}")
except Exception as e:
    print(f"  ❌ Parse failed: {e}")
    import traceback
    traceback.print_exc()

# Test 3: Create graph engine and load data
print("\n[Test 3] Creating graph and loading data...")

try:
    engine = GraphQueryEngine(state_store=None)
    print("  ✅ Engine created")

    # Create simple graph
    vertices = pa.table({
        'id': pa.array([0, 1, 2], type=pa.int64()),
        'label': pa.array(['Person', 'Person', 'Person'], type=pa.string()),
        'name': pa.array(['Alice', 'Bob', 'Charlie'], type=pa.string()),
    })

    edges = pa.table({
        'source': pa.array([0, 1], type=pa.int64()),
        'target': pa.array([1, 2], type=pa.int64()),
        'label': pa.array(['knows', 'knows'], type=pa.string()),
    })

    engine.load_vertices(vertices, persist=False)
    engine.load_edges(edges, persist=False)

    stats = engine.get_graph_stats()
    print(f"  ✅ Loaded {stats['num_vertices']} vertices, {stats['num_edges']} edges")

except Exception as e:
    print(f"  ❌ Failed to create engine: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

# Test 4: Try to execute SPARQL query
print("\n[Test 4] Attempting to execute SPARQL query...")

test_queries = [
    # Very simple pattern
    ("Simple pattern", """
        SELECT *
        WHERE {
            ?s ?p ?o .
        }
    """),

    # With variable names
    ("Named variables", """
        SELECT ?person ?friend
        WHERE {
            ?person knows ?friend .
        }
    """),
]

for name, query in test_queries:
    print(f"\n  Query: {name}")
    print(f"  {query.strip()}")

    try:
        result = engine.query_sparql(query)
        print(f"  ✅ Query executed!")
        print(f"     Result type: {type(result)}")

        if hasattr(result, 'to_pandas'):
            df = result.to_pandas()
            print(f"     Rows: {len(df)}")
            print(f"     Columns: {list(df.columns)}")
            print("\n     Data:")
            print(df)
        else:
            print(f"     Result: {result}")

    except NotImplementedError as e:
        print(f"  ⚠️  Not implemented: {e}")
    except Exception as e:
        print(f"  ❌ Query failed: {e}")
        import traceback
        traceback.print_exc()

print("\n" + "=" * 70)
print("Test Complete")
print("=" * 70)
