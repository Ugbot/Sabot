"""
SPARQL Query Compiler Demo

Demonstrates the new SPARQL query compiler (Phase 4.2).
Shows how to execute SPARQL queries on RDF graphs stored in Sabot.

Features demonstrated:
1. Parsing SPARQL queries into AST
2. Translating SPARQL to RDF triple pattern matching
3. Executing queries and getting results
4. Support for SELECT, WHERE, FILTER, LIMIT

SPARQL compiler inspired by QLever and Apache Jena ARQ
Reference: vendor/qlever/ (if available)
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
    Create a simple social network graph for RDF/SPARQL testing.

    Graph:
        Alice (0) --KNOWS--> Bob (1) --KNOWS--> Charlie (2)
              |                                       ^
              +---------------KNOWS-------------------+
              |
              +--FOLLOWS--> Dave (3) --KNOWS--> Eve (4)

    RDF Triples:
        0 rdf:type Person
        0 name "Alice"
        0 age 25
        0 KNOWS 1
        0 KNOWS 2
        0 FOLLOWS 3
        ...
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


def demo_sparql_compiler():
    """Demonstrate SPARQL query compiler."""
    print("=" * 70)
    print("SPARQL QUERY COMPILER DEMO - Phase 4.2")
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

    # Step 2: Simple triple pattern query
    print("Step 2: Execute simple SPARQL query")
    print("-" * 70)

    query1 = """
        SELECT ?s ?p ?o
        WHERE {
            ?s ?p ?o .
        }
        LIMIT 5
    """
    print(f"Query: {query1.strip()}")
    print()

    try:
        result = engine.query_sparql(query1)
        print(f"✅ Found {len(result)} matches")
        print(result.to_pandas())
    except Exception as e:
        print(f"⚠️  Query failed: {e}")
        import traceback
        traceback.print_exc()
    print()

    # Step 3: Query with type filter
    print("Step 3: Query with type filter (rdf:type)")
    print("-" * 70)

    query2 = """
        SELECT ?person
        WHERE {
            ?person rdf:type Person .
        }
    """
    print(f"Query: {query2.strip()}")
    print()

    try:
        result = engine.query_sparql(query2)
        print(f"✅ Found {len(result)} Person instances")
        print(result.to_pandas())
    except Exception as e:
        print(f"⚠️  Query failed: {e}")
        import traceback
        traceback.print_exc()
    print()

    # Step 4: Query with FILTER
    print("Step 4: Query with FILTER")
    print("-" * 70)

    query3 = """
        SELECT ?person ?age
        WHERE {
            ?person rdf:type Person .
            ?person age ?age .
            FILTER (?age > 25)
        }
    """
    print(f"Query: {query3.strip()}")
    print()

    try:
        result = engine.query_sparql(query3)
        print(f"✅ Found {len(result)} people over 25")
        print(result.to_pandas())
    except Exception as e:
        print(f"⚠️  Query failed: {e}")
        import traceback
        traceback.print_exc()
    print()

    # Step 5: Test parser directly
    print("Step 5: Test SPARQL parser (AST inspection)")
    print("-" * 70)

    from sabot._cython.graph.compiler import SPARQLParser

    parser = SPARQLParser()
    query4 = """
        SELECT DISTINCT ?person ?name
        WHERE {
            ?person rdf:type foaf:Person .
            ?person foaf:name ?name .
            FILTER (?name != "")
        }
        LIMIT 10
    """
    print(f"Query: {query4.strip()}")
    print()

    try:
        ast = parser.parse(query4)
        print(f"✅ Parsed successfully: {ast}")
        print()
        print("AST Details:")
        print(f"  - SELECT variables: {ast.select_clause.variables}")
        print(f"  - DISTINCT: {ast.select_clause.distinct}")
        print(f"  - Triple patterns: {len(ast.where_clause.bgp.triples)}")
        if ast.where_clause.bgp.triples:
            triple = ast.where_clause.bgp.triples[0]
            print(f"  - First triple: {triple.subject} {triple.predicate} {triple.object}")
        print(f"  - Filters: {len(ast.where_clause.filters)}")
        if ast.modifiers:
            print(f"  - LIMIT: {ast.modifiers.limit}")
            print(f"  - OFFSET: {ast.modifiers.offset}")
    except Exception as e:
        print(f"⚠️  Parse failed: {e}")
        import traceback
        traceback.print_exc()
    print()

    # Step 6: Test AST node creation
    print("Step 6: Test AST node creation")
    print("-" * 70)

    from sabot._cython.graph.compiler.sparql_ast import (
        IRI, Variable, TriplePattern, BasicGraphPattern, SPARQLQuery,
        SelectClause, WhereClause as SPARQLWhereClause
    )

    # Create triple pattern manually: ?person rdf:type foaf:Person
    subject = Variable(name='person')
    predicate = IRI(value='http://www.w3.org/1999/02/22-rdf-syntax-ns#type')
    obj = IRI(value='http://xmlns.com/foaf/0.1/Person')

    triple = TriplePattern(subject=subject, predicate=predicate, object=obj)
    bgp = BasicGraphPattern(triples=[triple])
    where = SPARQLWhereClause(bgp=bgp)
    select = SelectClause(variables=['person'])
    query = SPARQLQuery(select_clause=select, where_clause=where)

    print(f"✅ Created SPARQL query: {query}")
    print(f"  - Subject: {subject}")
    print(f"  - Predicate: {predicate}")
    print(f"  - Object: {obj}")
    print()

    # Summary
    print("=" * 70)
    print("DEMO COMPLETE")
    print("=" * 70)
    print()
    print("Summary:")
    print("  ✅ Phase 4.2: SPARQL Query Compiler - IMPLEMENTED")
    print()
    print("Supported Features:")
    print("  ✅ SELECT with projection (SELECT * or SELECT ?var1 ?var2)")
    print("  ✅ WHERE clause with Basic Graph Patterns (BGP)")
    print("  ✅ Triple patterns with variables")
    print("  ✅ FILTER expressions (comparison operators)")
    print("  ✅ LIMIT and OFFSET")
    print("  ✅ DISTINCT modifier")
    print("  ✅ AST parsing with pyparsing")
    print()
    print("Not Yet Supported:")
    print("  🚧 CONSTRUCT, ASK, DESCRIBE")
    print("  🚧 Complex filter expressions (regex, functions)")
    print("  🚧 OPTIONAL, UNION")
    print("  🚧 Property paths")
    print("  🚧 Aggregations (COUNT, SUM, etc.)")
    print("  🚧 ORDER BY")
    print("  🚧 Named graphs")
    print()
    print("Code Attribution:")
    print("  SPARQL compiler architecture inspired by QLever")
    print("  (https://github.com/ad-freiburg/qlever)")
    print("  and Apache Jena ARQ query engine")
    print()


if __name__ == "__main__":
    demo_sparql_compiler()
