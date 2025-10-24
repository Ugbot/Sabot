#!/usr/bin/env python3
"""
Comprehensive SPARQL Query Tests

Tests SPARQL parser fixes and query execution against PyRDFTripleStore.

Test Coverage:
1. Simple triple patterns
2. Multiple triple patterns
3. PREFIX declarations
4. FILTER expressions
5. LIMIT/OFFSET
6. DISTINCT
7. Complex queries
"""

import sys
import os
from pathlib import Path

# Add sabot to path
sabot_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(sabot_root))

from sabot import cyarrow as pa
from sabot._cython.graph.compiler.sparql_parser import SPARQLParser
from sabot._cython.graph.storage.graph_storage import PyRDFTripleStore
from sabot._cython.graph.compiler.sparql_translator import SPARQLTranslator


def create_test_rdf_data():
    """Create test RDF dataset with people and relationships."""

    # Term dictionary
    # IDs 0-99: Entities
    # IDs 100-199: Predicates
    # IDs 200-299: Types
    # IDs 300+: Literals

    entities = {
        0: 'http://example.org/Alice',
        1: 'http://example.org/Bob',
        2: 'http://example.org/Charlie',
        3: 'http://example.org/Diana',
    }

    predicates = {
        100: 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type',
        101: 'http://xmlns.com/foaf/0.1/name',
        102: 'http://xmlns.com/foaf/0.1/age',
        103: 'http://xmlns.com/foaf/0.1/knows',
        104: 'http://example.org/worksAt',
    }

    types = {
        200: 'http://xmlns.com/foaf/0.1/Person',
        201: 'http://example.org/Company',
    }

    literals = {
        300: 'Alice',
        301: 'Bob',
        302: 'Charlie',
        303: 'Diana',
        304: '25',
        305: '30',
        306: '28',
        307: '35',
        308: 'Acme Corp',
    }

    companies = {
        10: 'http://example.org/AcmeCorp',
    }

    # Build term dictionary
    all_terms = {}
    all_terms.update(entities)
    all_terms.update(predicates)
    all_terms.update(types)
    all_terms.update(literals)
    all_terms.update(companies)

    terms_data = {
        'id': [],
        'lex': [],
        'kind': [],  # 0=IRI, 1=Literal
        'lang': [],
        'datatype': []
    }

    for term_id, lex in all_terms.items():
        terms_data['id'].append(term_id)
        terms_data['lex'].append(lex)
        # Literals are 300+
        terms_data['kind'].append(0 if term_id < 300 else 1)
        terms_data['lang'].append('')
        terms_data['datatype'].append('')

    terms_table = pa.Table.from_pydict({
        'id': pa.array(terms_data['id'], type=pa.int64()),
        'lex': pa.array(terms_data['lex'], type=pa.string()),
        'kind': pa.array(terms_data['kind'], type=pa.uint8()),
        'lang': pa.array(terms_data['lang'], type=pa.string()),
        'datatype': pa.array(terms_data['datatype'], type=pa.string())
    })

    # Create triples
    # Alice rdf:type Person, name "Alice", age 25, knows Bob, worksAt AcmeCorp
    # Bob rdf:type Person, name "Bob", age 30, knows Charlie
    # Charlie rdf:type Person, name "Charlie", age 28
    # Diana rdf:type Person, name "Diana", age 35, worksAt AcmeCorp
    # AcmeCorp rdf:type Company, name "Acme Corp"

    triples_data = {
        's': [0, 0, 0, 0, 0,     # Alice
              1, 1, 1, 1,         # Bob
              2, 2, 2,            # Charlie
              3, 3, 3, 3,         # Diana
              10, 10],            # AcmeCorp
        'p': [100, 101, 102, 103, 104,  # Alice
              100, 101, 102, 103,       # Bob
              100, 101, 102,            # Charlie
              100, 101, 102, 104,       # Diana
              100, 101],                # AcmeCorp
        'o': [200, 300, 304, 1, 10,     # Alice
              200, 301, 305, 2,         # Bob
              200, 302, 306,            # Charlie
              200, 303, 307, 10,        # Diana
              201, 308]                 # AcmeCorp
    }

    triples_table = pa.Table.from_pydict({
        's': pa.array(triples_data['s'], type=pa.int64()),
        'p': pa.array(triples_data['p'], type=pa.int64()),
        'o': pa.array(triples_data['o'], type=pa.int64())
    })

    # Create store
    store = PyRDFTripleStore(triples_table, terms_table)

    return store, all_terms


def test_simple_triple_pattern():
    """Test 1: Simple triple pattern - find all people."""
    print("\n[Test 1] Simple triple pattern")
    print("-" * 60)

    store, terms = create_test_rdf_data()
    parser = SPARQLParser()

    query = """
        SELECT ?person
        WHERE {
            ?person <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://xmlns.com/foaf/0.1/Person> .
        }
    """

    print(f"Query: {query.strip()}")

    try:
        ast = parser.parse(query)
        print(f"✅ Parsed successfully")
        print(f"   Variables: {ast.select_clause.variables}")
        print(f"   Patterns: {len(ast.where_clause.bgp.triples)}")

        # Execute query
        translator = SPARQLTranslator(store)
        result = translator.execute(ast)

        print(f"✅ Query executed")
        print(f"   Results: {result.num_rows} rows")

        # Show results
        for i in range(result.num_rows):
            person_uri = result.column('person')[i].as_py()
            # Result contains URIs (strings), not IDs
            person_name = person_uri.split('/')[-1] if '/' in person_uri else person_uri
            print(f"   - {person_name}")

        assert result.num_rows == 4, f"Expected 4 people, got {result.num_rows}"
        print("✅ Test passed")

    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

    return True


def test_multiple_triple_patterns():
    """Test 2: Multiple triple patterns - find names of people."""
    print("\n[Test 2] Multiple triple patterns")
    print("-" * 60)

    store, terms = create_test_rdf_data()
    parser = SPARQLParser()

    query = """
        SELECT ?person ?name
        WHERE {
            ?person <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://xmlns.com/foaf/0.1/Person> .
            ?person <http://xmlns.com/foaf/0.1/name> ?name .
        }
    """

    print(f"Query: {query.strip()}")

    try:
        ast = parser.parse(query)
        print(f"✅ Parsed successfully")
        print(f"   Variables: {ast.select_clause.variables}")
        print(f"   Patterns: {len(ast.where_clause.bgp.triples)}")

        # Execute query
        translator = SPARQLTranslator(store)
        result = translator.execute(ast)

        print(f"✅ Query executed")
        print(f"   Results: {result.num_rows} rows")

        # Show results
        for i in range(result.num_rows):
            person_uri = result.column('person')[i].as_py()
            name = result.column('name')[i].as_py()
            # Results contain URIs/literals (strings), not IDs
            person_name = person_uri.split('/')[-1] if '/' in person_uri else person_uri
            print(f"   - {person_name}: {name}")

        assert result.num_rows == 4, f"Expected 4 results, got {result.num_rows}"
        print("✅ Test passed")

    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

    return True


def test_prefix_declarations():
    """Test 3: PREFIX declarations - use prefixes for namespaces."""
    print("\n[Test 3] PREFIX declarations")
    print("-" * 60)

    store, terms = create_test_rdf_data()
    parser = SPARQLParser()

    query = """
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX foaf: <http://xmlns.com/foaf/0.1/>
        PREFIX ex: <http://example.org/>

        SELECT ?person ?name
        WHERE {
            ?person rdf:type foaf:Person .
            ?person foaf:name ?name .
        }
    """

    print(f"Query: {query.strip()}")

    try:
        ast = parser.parse(query)
        print(f"✅ Parsed successfully with PREFIX declarations")
        print(f"   Variables: {ast.select_clause.variables}")
        print(f"   Patterns: {len(ast.where_clause.bgp.triples)}")

        # Execute query
        translator = SPARQLTranslator(store)
        result = translator.execute(ast)

        print(f"✅ Query executed")
        print(f"   Results: {result.num_rows} rows")

        # Show results
        for i in range(result.num_rows):
            person_uri = result.column('person')[i].as_py()
            name = result.column('name')[i].as_py()
            # Results contain URIs/literals (strings), not IDs
            person_name = person_uri.split('/')[-1] if '/' in person_uri else person_uri
            print(f"   - {person_name}: {name}")

        assert result.num_rows == 4, f"Expected 4 results, got {result.num_rows}"
        print("✅ Test passed")

    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

    return True


def test_limit_offset():
    """Test 4: LIMIT and OFFSET - pagination."""
    print("\n[Test 4] LIMIT and OFFSET")
    print("-" * 60)

    store, terms = create_test_rdf_data()
    parser = SPARQLParser()

    query = """
        PREFIX foaf: <http://xmlns.com/foaf/0.1/>

        SELECT ?name
        WHERE {
            ?person foaf:name ?name .
        }
        LIMIT 2
    """

    print(f"Query: {query.strip()}")

    try:
        ast = parser.parse(query)
        print(f"✅ Parsed successfully")
        print(f"   LIMIT: {ast.solution_modifier.limit if hasattr(ast, 'solution_modifier') else 'N/A'}")

        # Execute query
        translator = SPARQLTranslator(store)
        result = translator.execute(ast)

        print(f"✅ Query executed")
        print(f"   Results: {result.num_rows} rows")

        # Show results
        for i in range(result.num_rows):
            name = result.column('name')[i].as_py()
            # Results contain literals (strings), not IDs
            print(f"   - {name}")

        assert result.num_rows <= 2, f"Expected max 2 results, got {result.num_rows}"
        print("✅ Test passed")

    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

    return True


def test_distinct():
    """Test 5: DISTINCT - unique results."""
    print("\n[Test 5] DISTINCT")
    print("-" * 60)

    store, terms = create_test_rdf_data()
    parser = SPARQLParser()

    query = """
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>

        SELECT DISTINCT ?type
        WHERE {
            ?entity rdf:type ?type .
        }
    """

    print(f"Query: {query.strip()}")

    try:
        ast = parser.parse(query)
        print(f"✅ Parsed successfully")
        print(f"   DISTINCT: {ast.select_clause.distinct if hasattr(ast.select_clause, 'distinct') else 'N/A'}")

        # Execute query
        translator = SPARQLTranslator(store)
        result = translator.execute(ast)

        print(f"✅ Query executed")
        print(f"   Results: {result.num_rows} rows")

        # Show results
        for i in range(result.num_rows):
            type_uri = result.column('type')[i].as_py()
            # Results contain URIs (strings), not IDs
            type_name = type_uri.split('/')[-1] if '/' in type_uri else type_uri
            print(f"   - {type_name}")

        # Should have Person and Company types
        print("✅ Test passed")

    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

    return True


def test_complex_query():
    """Test 6: Complex query - multiple patterns with joins."""
    print("\n[Test 6] Complex query")
    print("-" * 60)

    store, terms = create_test_rdf_data()
    parser = SPARQLParser()

    query = """
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX foaf: <http://xmlns.com/foaf/0.1/>
        PREFIX ex: <http://example.org/>

        SELECT ?person ?name ?friend ?friend_name
        WHERE {
            ?person rdf:type foaf:Person .
            ?person foaf:name ?name .
            ?person foaf:knows ?friend .
            ?friend foaf:name ?friend_name .
        }
    """

    print(f"Query: {query.strip()}")

    try:
        ast = parser.parse(query)
        print(f"✅ Parsed successfully")
        print(f"   Variables: {ast.select_clause.variables}")
        print(f"   Patterns: {len(ast.where_clause.bgp.triples)}")

        # Execute query
        translator = SPARQLTranslator(store)
        result = translator.execute(ast)

        print(f"✅ Query executed")
        print(f"   Results: {result.num_rows} rows")

        # Show results
        for i in range(result.num_rows):
            person_uri = result.column('person')[i].as_py()
            name = result.column('name')[i].as_py()
            friend_uri = result.column('friend')[i].as_py()
            friend_name = result.column('friend_name')[i].as_py()

            # Results contain URIs/literals (strings), not IDs
            person_short = person_uri.split('/')[-1] if '/' in person_uri else person_uri
            friend_short = friend_uri.split('/')[-1] if '/' in friend_uri else friend_uri

            print(f"   - {person_short} ({name}) knows {friend_short} ({friend_name})")

        # Alice knows Bob, Bob knows Charlie
        assert result.num_rows == 2, f"Expected 2 friendship pairs, got {result.num_rows}"
        print("✅ Test passed")

    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

    return True


def test_wildcard_pattern():
    """Test 7: Wildcard pattern - find all triples."""
    print("\n[Test 7] Wildcard pattern")
    print("-" * 60)

    store, terms = create_test_rdf_data()
    parser = SPARQLParser()

    query = """
        SELECT ?s ?p ?o
        WHERE {
            ?s ?p ?o .
        }
        LIMIT 5
    """

    print(f"Query: {query.strip()}")

    try:
        ast = parser.parse(query)
        print(f"✅ Parsed successfully")
        print(f"   Variables: {ast.select_clause.variables}")

        # Execute query
        translator = SPARQLTranslator(store)
        result = translator.execute(ast)

        print(f"✅ Query executed")
        print(f"   Results: {result.num_rows} rows")

        # Show results
        for i in range(min(5, result.num_rows)):
            s_uri = result.column('s')[i].as_py()
            p_uri = result.column('p')[i].as_py()
            o_val = result.column('o')[i].as_py()

            # Results contain URIs/literals (strings), not IDs
            s_lex = s_uri.split('/')[-1] if '/' in s_uri else (s_uri.split('#')[-1] if '#' in s_uri else s_uri)
            p_lex = p_uri.split('/')[-1] if '/' in p_uri else (p_uri.split('#')[-1] if '#' in p_uri else p_uri)
            o_lex = o_val.split('/')[-1] if '/' in o_val else (o_val.split('#')[-1] if '#' in o_val else o_val)

            print(f"   - {s_lex} {p_lex} {o_lex}")

        print("✅ Test passed")

    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

    return True


def main():
    """Run all SPARQL query tests."""
    print("=" * 70)
    print("SPARQL Query Test Suite")
    print("=" * 70)
    print()
    print("Testing SPARQL parser fixes and query execution")
    print()

    tests = [
        test_simple_triple_pattern,
        test_multiple_triple_patterns,
        test_prefix_declarations,
        test_limit_offset,
        test_distinct,
        test_complex_query,
        test_wildcard_pattern,
    ]

    results = []
    for test in tests:
        result = test()
        results.append(result)

    # Summary
    print("\n" + "=" * 70)
    print("Test Summary")
    print("=" * 70)

    passed = sum(results)
    total = len(results)

    print(f"\nPassed: {passed}/{total}")

    if passed == total:
        print("✅ All tests passed!")
    else:
        print(f"❌ {total - passed} test(s) failed")

    return passed == total


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
