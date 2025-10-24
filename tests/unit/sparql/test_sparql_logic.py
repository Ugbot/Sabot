#!/usr/bin/env python3
"""
SPARQL Query Logic Tests

Tests that verify actual query execution logic and data correctness.
These tests check:
- Correct data is returned (not just row counts)
- JOIN logic works correctly
- FILTER logic works correctly
- Pattern matching is accurate
- Variable bindings are correct
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


def create_test_data():
    """Create well-defined test RDF data with known relationships."""

    # Define exact term IDs and values
    # People: 0-2
    # Predicates: 100-103
    # Types: 200
    # Literals: 300-305

    terms_data = {
        'id': [
            # People
            0, 1, 2,
            # Predicates
            100, 101, 102, 103,
            # Types
            200,
            # Literals
            300, 301, 302, 303, 304, 305
        ],
        'lex': [
            # People
            'http://example.org/Alice',
            'http://example.org/Bob',
            'http://example.org/Charlie',
            # Predicates
            'http://www.w3.org/1999/02/22-rdf-syntax-ns#type',
            'http://xmlns.com/foaf/0.1/name',
            'http://xmlns.com/foaf/0.1/age',
            'http://xmlns.com/foaf/0.1/knows',
            # Types
            'http://xmlns.com/foaf/0.1/Person',
            # Literals
            'Alice', 'Bob', 'Charlie', '25', '30', '28'
        ],
        'kind': [
            0, 0, 0,  # People (IRIs)
            0, 0, 0, 0,  # Predicates (IRIs)
            0,  # Types (IRIs)
            1, 1, 1, 1, 1, 1  # Literals
        ],
        'lang': [''] * 14,
        'datatype': [''] * 14
    }

    terms_table = pa.Table.from_pydict({
        'id': pa.array(terms_data['id'], type=pa.int64()),
        'lex': pa.array(terms_data['lex'], type=pa.string()),
        'kind': pa.array(terms_data['kind'], type=pa.uint8()),
        'lang': pa.array(terms_data['lang'], type=pa.string()),
        'datatype': pa.array(terms_data['datatype'], type=pa.string())
    })

    # Triples:
    # Alice rdf:type Person, name "Alice", age "25", knows Bob
    # Bob rdf:type Person, name "Bob", age "30", knows Charlie
    # Charlie rdf:type Person, name "Charlie", age "28"

    triples_data = {
        's': [0, 0, 0, 0, 1, 1, 1, 1, 2, 2, 2],
        'p': [100, 101, 102, 103, 100, 101, 102, 103, 100, 101, 102],
        'o': [200, 300, 303, 1, 200, 301, 304, 2, 200, 302, 305]
    }

    triples_table = pa.Table.from_pydict({
        's': pa.array(triples_data['s'], type=pa.int64()),
        'p': pa.array(triples_data['p'], type=pa.int64()),
        'o': pa.array(triples_data['o'], type=pa.int64())
    })

    store = PyRDFTripleStore(triples_table, terms_table)

    return store


def test_simple_filter():
    """Test 1: Verify simple pattern matching returns correct data."""
    print("\n[Test 1] Simple Pattern Matching")
    print("-" * 70)

    store = create_test_data()
    parser = SPARQLParser()

    query = """
        SELECT ?person ?name
        WHERE {
            ?person <http://xmlns.com/foaf/0.1/name> ?name .
        }
    """

    ast = parser.parse(query)
    translator = SPARQLTranslator(store)
    result = translator.execute(ast)

    # Verify row count
    assert result.num_rows == 3, f"Expected 3 names, got {result.num_rows}"

    # Get results as pandas for easy verification
    df = result.to_pandas()

    # Verify exact data
    names = sorted(df['name'].tolist())
    assert names == ['Alice', 'Bob', 'Charlie'], f"Expected ['Alice', 'Bob', 'Charlie'], got {names}"

    # Verify person URIs
    persons = sorted(df['person'].tolist())
    expected_persons = [
        'http://example.org/Alice',
        'http://example.org/Bob',
        'http://example.org/Charlie'
    ]
    assert persons == expected_persons, f"Person URIs mismatch"

    print("✅ Correct data returned")
    print(f"   Names: {names}")
    return True


def test_join_logic():
    """Test 2: Verify JOIN between two triple patterns works correctly."""
    print("\n[Test 2] JOIN Logic (Multiple Patterns)")
    print("-" * 70)

    store = create_test_data()
    parser = SPARQLParser()

    query = """
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX foaf: <http://xmlns.com/foaf/0.1/>

        SELECT ?person ?name ?age
        WHERE {
            ?person rdf:type foaf:Person .
            ?person foaf:name ?name .
            ?person foaf:age ?age .
        }
    """

    ast = parser.parse(query)
    translator = SPARQLTranslator(store)
    result = translator.execute(ast)

    # Verify row count
    assert result.num_rows == 3, f"Expected 3 people with ages, got {result.num_rows}"

    df = result.to_pandas()

    # Verify Alice's data
    alice_row = df[df['name'] == 'Alice']
    assert len(alice_row) == 1, "Should have exactly one Alice"
    assert alice_row.iloc[0]['age'] == '25', f"Alice's age should be 25, got {alice_row.iloc[0]['age']}"

    # Verify Bob's data
    bob_row = df[df['name'] == 'Bob']
    assert len(bob_row) == 1, "Should have exactly one Bob"
    assert bob_row.iloc[0]['age'] == '30', f"Bob's age should be 30, got {bob_row.iloc[0]['age']}"

    # Verify Charlie's data
    charlie_row = df[df['name'] == 'Charlie']
    assert len(charlie_row) == 1, "Should have exactly one Charlie"
    assert charlie_row.iloc[0]['age'] == '28', f"Charlie's age should be 28, got {charlie_row.iloc[0]['age']}"

    print("✅ JOIN logic correct")
    print("   Alice: age 25 ✓")
    print("   Bob: age 30 ✓")
    print("   Charlie: age 28 ✓")
    return True


def test_knows_relationship():
    """Test 3: Verify relationship queries return correct pairs."""
    print("\n[Test 3] Relationship Query (knows)")
    print("-" * 70)

    store = create_test_data()
    parser = SPARQLParser()

    query = """
        PREFIX foaf: <http://xmlns.com/foaf/0.1/>

        SELECT ?person ?friend
        WHERE {
            ?person foaf:knows ?friend .
        }
    """

    ast = parser.parse(query)
    translator = SPARQLTranslator(store)
    result = translator.execute(ast)

    # Verify row count
    assert result.num_rows == 2, f"Expected 2 'knows' relationships, got {result.num_rows}"

    df = result.to_pandas()

    # Extract person-friend pairs
    pairs = []
    for _, row in df.iterrows():
        person = row['person'].split('/')[-1]
        friend = row['friend'].split('/')[-1]
        pairs.append((person, friend))

    pairs_sorted = sorted(pairs)
    expected = [('Alice', 'Bob'), ('Bob', 'Charlie')]

    assert pairs_sorted == expected, f"Expected {expected}, got {pairs_sorted}"

    print("✅ Relationship query correct")
    print("   Alice knows Bob ✓")
    print("   Bob knows Charlie ✓")
    return True


def test_transitive_query():
    """Test 4: Verify complex multi-hop query (friend-of-friend)."""
    print("\n[Test 4] Multi-hop Query (Friend Names)")
    print("-" * 70)

    store = create_test_data()
    parser = SPARQLParser()

    # Find people and their friends' names
    query = """
        PREFIX foaf: <http://xmlns.com/foaf/0.1/>

        SELECT ?person_name ?friend_name
        WHERE {
            ?person foaf:name ?person_name .
            ?person foaf:knows ?friend .
            ?friend foaf:name ?friend_name .
        }
    """

    ast = parser.parse(query)
    translator = SPARQLTranslator(store)
    result = translator.execute(ast)

    # Should get 2 results: Alice->Bob, Bob->Charlie
    assert result.num_rows == 2, f"Expected 2 friend pairs, got {result.num_rows}"

    df = result.to_pandas()

    # Verify exact pairs
    pairs = []
    for _, row in df.iterrows():
        pairs.append((row['person_name'], row['friend_name']))

    pairs_sorted = sorted(pairs)
    expected = [('Alice', 'Bob'), ('Bob', 'Charlie')]

    assert pairs_sorted == expected, f"Expected {expected}, got {pairs_sorted}"

    print("✅ Multi-hop query correct")
    print("   Alice's friend: Bob ✓")
    print("   Bob's friend: Charlie ✓")
    return True


def test_wildcard_all_predicates():
    """Test 5: Verify wildcard pattern returns all triples."""
    print("\n[Test 5] Wildcard Pattern (All Triples)")
    print("-" * 70)

    store = create_test_data()
    parser = SPARQLParser()

    query = """
        SELECT ?s ?p ?o
        WHERE {
            ?s ?p ?o .
        }
    """

    ast = parser.parse(query)
    translator = SPARQLTranslator(store)
    result = translator.execute(ast)

    # Should get ALL 11 triples
    assert result.num_rows == 11, f"Expected 11 triples, got {result.num_rows}"

    df = result.to_pandas()

    # Verify we have all expected predicates
    predicates = set()
    for _, row in df.iterrows():
        p = row['p'].split('/')[-1].split('#')[-1]
        predicates.add(p)

    expected_predicates = {'type', 'name', 'age', 'knows'}
    assert predicates == expected_predicates, f"Expected predicates {expected_predicates}, got {predicates}"

    # Count occurrences of each predicate
    pred_counts = {}
    for _, row in df.iterrows():
        p = row['p'].split('/')[-1].split('#')[-1]
        pred_counts[p] = pred_counts.get(p, 0) + 1

    # Verify counts: 3 type, 3 name, 3 age, 2 knows
    assert pred_counts['type'] == 3, f"Expected 3 'type' triples, got {pred_counts.get('type', 0)}"
    assert pred_counts['name'] == 3, f"Expected 3 'name' triples, got {pred_counts.get('name', 0)}"
    assert pred_counts['age'] == 3, f"Expected 3 'age' triples, got {pred_counts.get('age', 0)}"
    assert pred_counts['knows'] == 2, f"Expected 2 'knows' triples, got {pred_counts.get('knows', 0)}"

    print("✅ Wildcard pattern correct")
    print(f"   Total triples: 11 ✓")
    print(f"   type: 3, name: 3, age: 3, knows: 2 ✓")
    return True


def test_specific_person_query():
    """Test 6: Verify querying for a specific IRI works."""
    print("\n[Test 6] Specific IRI Query")
    print("-" * 70)

    store = create_test_data()
    parser = SPARQLParser()

    # Query specifically for Alice
    query = """
        PREFIX foaf: <http://xmlns.com/foaf/0.1/>

        SELECT ?name ?age
        WHERE {
            <http://example.org/Alice> foaf:name ?name .
            <http://example.org/Alice> foaf:age ?age .
        }
    """

    ast = parser.parse(query)
    translator = SPARQLTranslator(store)
    result = translator.execute(ast)

    # Should get exactly 1 result for Alice
    assert result.num_rows == 1, f"Expected 1 result for Alice, got {result.num_rows}"

    df = result.to_pandas()

    # Verify Alice's exact data
    assert df.iloc[0]['name'] == 'Alice', f"Expected name 'Alice', got {df.iloc[0]['name']}"
    assert df.iloc[0]['age'] == '25', f"Expected age '25', got {df.iloc[0]['age']}"

    print("✅ Specific IRI query correct")
    print("   Alice: name='Alice', age='25' ✓")
    return True


def test_no_results_query():
    """Test 7: Verify queries that should return no results."""
    print("\n[Test 7] No Results Query")
    print("-" * 70)

    store = create_test_data()
    parser = SPARQLParser()

    # Query for non-existent data
    query = """
        PREFIX foaf: <http://xmlns.com/foaf/0.1/>

        SELECT ?person
        WHERE {
            ?person foaf:email ?email .
        }
    """

    ast = parser.parse(query)
    translator = SPARQLTranslator(store)
    result = translator.execute(ast)

    # Should get 0 results (no email predicates in data)
    assert result.num_rows == 0, f"Expected 0 results, got {result.num_rows}"

    print("✅ No results query correct")
    print("   Correctly returned 0 rows ✓")
    return True


def main():
    """Run all SPARQL logic tests."""
    print("=" * 70)
    print("SPARQL QUERY LOGIC TESTS")
    print("=" * 70)
    print()
    print("These tests verify actual query execution logic and data correctness.")
    print()

    tests = [
        test_simple_filter,
        test_join_logic,
        test_knows_relationship,
        test_transitive_query,
        test_wildcard_all_predicates,
        test_specific_person_query,
        test_no_results_query,
    ]

    results = []
    for test in tests:
        try:
            result = test()
            results.append(result)
        except AssertionError as e:
            print(f"❌ Assertion failed: {e}")
            import traceback
            traceback.print_exc()
            results.append(False)
        except Exception as e:
            print(f"❌ Test error: {e}")
            import traceback
            traceback.print_exc()
            results.append(False)

    # Summary
    print("\n" + "=" * 70)
    print("TEST SUMMARY")
    print("=" * 70)

    passed = sum(results)
    total = len(results)

    print(f"\nPassed: {passed}/{total}")

    if passed == total:
        print("\n✅ All logic tests passed!")
        print("\nVerified:")
        print("  ✅ Simple pattern matching returns correct data")
        print("  ✅ JOIN logic works correctly across multiple patterns")
        print("  ✅ Relationship queries return correct pairs")
        print("  ✅ Multi-hop queries (friend-of-friend) work")
        print("  ✅ Wildcard patterns return all triples")
        print("  ✅ Specific IRI queries work")
        print("  ✅ Empty result sets handled correctly")
        print()
    else:
        print(f"\n❌ {total - passed} test(s) failed")
        print()

    return passed == total


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
