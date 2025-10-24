#!/usr/bin/env python3
"""
Test suite for sabot.rdf user-friendly API

Tests the high-level RDFStore class for:
- Triple addition
- SPARQL queries
- Pattern matching
- Vocabulary management
"""

import sys
from pathlib import Path

# Add sabot to path
sabot_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(sabot_root))

from sabot.rdf import RDFStore, SPARQLEngine, create_rdf_store


def test_create_store():
    """Test 1: Create empty store."""
    print("\n[Test 1] Create empty store")
    print("-" * 60)

    store = RDFStore()
    assert store.count() == 0, "New store should have 0 triples"
    assert store.count_terms() == 0, "New store should have 0 terms"
    assert len(store.prefixes) > 0, "Should have default prefixes"

    print("✓ Empty store created successfully")
    print(f"  Default prefixes: {list(store.prefixes.keys())}")
    return True


def test_add_single_triple():
    """Test 2: Add single triple."""
    print("\n[Test 2] Add single triple")
    print("-" * 60)

    store = RDFStore()
    store.add("http://example.org/Alice",
              "http://xmlns.com/foaf/0.1/name",
              "Alice", obj_is_literal=True)

    assert store.count() == 1, "Should have 1 triple"
    assert store.count_terms() == 3, "Should have 3 terms (subject, predicate, object)"

    print("✓ Single triple added successfully")
    print(f"  Triples: {store.count()}, Terms: {store.count_terms()}")
    return True


def test_add_many_triples():
    """Test 3: Add multiple triples efficiently."""
    print("\n[Test 3] Add multiple triples")
    print("-" * 60)

    store = RDFStore()

    triples = [
        ("http://example.org/Alice", "http://xmlns.com/foaf/0.1/name", "Alice", True),
        ("http://example.org/Alice", "http://xmlns.com/foaf/0.1/age", "25", True),
        ("http://example.org/Bob", "http://xmlns.com/foaf/0.1/name", "Bob", True),
    ]

    store.add_many(triples)

    assert store.count() == 3, f"Should have 3 triples, got {store.count()}"
    # Alice, Bob, foaf:name, foaf:age, "Alice", "25", "Bob" = 7 unique terms
    assert store.count_terms() == 7, f"Should have 7 terms, got {store.count_terms()}"

    print("✓ Multiple triples added successfully")
    print(f"  Triples: {store.count()}, Terms: {store.count_terms()}")
    return True


def test_simple_sparql_query():
    """Test 4: Simple SPARQL query."""
    print("\n[Test 4] Simple SPARQL query")
    print("-" * 60)

    store = RDFStore()
    store.add("http://example.org/Alice",
              "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
              "http://xmlns.com/foaf/0.1/Person")
    store.add("http://example.org/Bob",
              "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
              "http://xmlns.com/foaf/0.1/Person")

    query = """
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX foaf: <http://xmlns.com/foaf/0.1/>
        SELECT ?person
        WHERE { ?person rdf:type foaf:Person . }
    """

    result = store.query(query)
    assert result.num_rows == 2, f"Expected 2 results, got {result.num_rows}"

    print("✓ Simple SPARQL query executed")
    print(f"  Results: {result.num_rows} rows")
    return True


def test_multi_pattern_query():
    """Test 5: Multi-pattern SPARQL query with JOIN."""
    print("\n[Test 5] Multi-pattern query with JOIN")
    print("-" * 60)

    store = RDFStore()
    store.add("http://example.org/Alice",
              "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
              "http://xmlns.com/foaf/0.1/Person")
    store.add("http://example.org/Alice",
              "http://xmlns.com/foaf/0.1/name",
              "Alice", obj_is_literal=True)
    store.add("http://example.org/Alice",
              "http://xmlns.com/foaf/0.1/age",
              "25", obj_is_literal=True)

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

    result = store.query(query)
    assert result.num_rows == 1, f"Expected 1 result, got {result.num_rows}"

    df = result.to_pandas()
    assert df.iloc[0]['name'] == 'Alice', "Name should be Alice"
    assert df.iloc[0]['age'] == '25', "Age should be 25"

    print("✓ Multi-pattern query with JOIN executed")
    print(f"  Results: {result.num_rows} row with 3 columns")
    return True


def test_relationship_query():
    """Test 6: Relationship query."""
    print("\n[Test 6] Relationship query")
    print("-" * 60)

    store = RDFStore()
    store.add("http://example.org/Alice",
              "http://xmlns.com/foaf/0.1/knows",
              "http://example.org/Bob")
    store.add("http://example.org/Bob",
              "http://xmlns.com/foaf/0.1/knows",
              "http://example.org/Charlie")

    query = """
        PREFIX foaf: <http://xmlns.com/foaf/0.1/>
        SELECT ?person ?friend
        WHERE { ?person foaf:knows ?friend . }
    """

    result = store.query(query)
    assert result.num_rows == 2, f"Expected 2 relationships, got {result.num_rows}"

    print("✓ Relationship query executed")
    print(f"  Found {result.num_rows} friendships")
    return True


def test_direct_pattern_matching():
    """Test 7: Direct pattern matching (bypass SPARQL)."""
    print("\n[Test 7] Direct pattern matching")
    print("-" * 60)

    store = RDFStore()
    store.add("http://example.org/Alice",
              "http://xmlns.com/foaf/0.1/name",
              "Alice", obj_is_literal=True)
    store.add("http://example.org/Bob",
              "http://xmlns.com/foaf/0.1/name",
              "Bob", obj_is_literal=True)
    store.add("http://example.org/Charlie",
              "http://xmlns.com/foaf/0.1/age",
              "30", obj_is_literal=True)

    # Find all triples with foaf:name predicate
    result = store.filter_triples(predicate="http://xmlns.com/foaf/0.1/name")

    assert result.num_rows == 2, f"Expected 2 name triples, got {result.num_rows}"

    print("✓ Direct pattern matching executed")
    print(f"  Found {result.num_rows} matching triples")
    return True


def test_add_prefix():
    """Test 8: Add custom PREFIX."""
    print("\n[Test 8] Add custom PREFIX")
    print("-" * 60)

    store = RDFStore()
    store.add_prefix('ex', 'http://example.org/')

    assert 'ex' in store.prefixes, "Custom prefix should be registered"
    assert store.prefixes['ex'] == 'http://example.org/', "Prefix URI should match"

    # Use custom prefix in query
    store.add("http://example.org/Alice",
              "http://example.org/email",
              "alice@example.org", obj_is_literal=True)

    # Note: PREFIX expansion happens in SPARQL parser, but we've registered it
    print("✓ Custom PREFIX registered")
    print(f"  Registered: {store.prefixes['ex']}")
    return True


def test_empty_query():
    """Test 9: Query on empty store."""
    print("\n[Test 9] Query on empty store")
    print("-" * 60)

    store = RDFStore()

    try:
        query = "SELECT ?s ?p ?o WHERE { ?s ?p ?o . }"
        result = store.query(query)
        print("❌ Should have raised ValueError")
        return False
    except ValueError as e:
        print("✓ Correctly raised ValueError for empty store")
        print(f"  Error: {str(e)[:50]}...")
        return True


def test_invalid_query():
    """Test 10: Invalid SPARQL query."""
    print("\n[Test 10] Invalid SPARQL query")
    print("-" * 60)

    store = RDFStore()
    store.add("http://example.org/Alice",
              "http://xmlns.com/foaf/0.1/name",
              "Alice", obj_is_literal=True)

    try:
        query = "INVALID SPARQL SYNTAX"
        result = store.query(query)
        print("❌ Should have raised ValueError")
        return False
    except ValueError as e:
        print("✓ Correctly raised ValueError for invalid query")
        print(f"  Error: {str(e)[:50]}...")
        return True


def test_limit_query():
    """Test 11: LIMIT in SPARQL query."""
    print("\n[Test 11] LIMIT in SPARQL query")
    print("-" * 60)

    store = RDFStore()
    for i in range(10):
        store.add(f"http://example.org/Person{i}",
                  "http://xmlns.com/foaf/0.1/name",
                  f"Person{i}", obj_is_literal=True)

    query = """
        PREFIX foaf: <http://xmlns.com/foaf/0.1/>
        SELECT ?person ?name
        WHERE { ?person foaf:name ?name . }
        LIMIT 3
    """

    result = store.query(query)
    assert result.num_rows <= 3, f"Expected max 3 results, got {result.num_rows}"

    print("✓ LIMIT query executed")
    print(f"  Limited to {result.num_rows} rows")
    return True


def test_distinct_query():
    """Test 12: DISTINCT in SPARQL query."""
    print("\n[Test 12] DISTINCT in SPARQL query")
    print("-" * 60)

    store = RDFStore()
    store.add("http://example.org/Alice",
              "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
              "http://xmlns.com/foaf/0.1/Person")
    store.add("http://example.org/Bob",
              "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
              "http://xmlns.com/foaf/0.1/Person")

    query = """
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        SELECT DISTINCT ?type
        WHERE { ?entity rdf:type ?type . }
    """

    result = store.query(query)
    assert result.num_rows == 1, f"Expected 1 distinct type, got {result.num_rows}"

    print("✓ DISTINCT query executed")
    print(f"  Found {result.num_rows} distinct type(s)")
    return True


def test_stats():
    """Test 13: Store statistics."""
    print("\n[Test 13] Store statistics")
    print("-" * 60)

    store = RDFStore()
    store.add("http://example.org/Alice",
              "http://xmlns.com/foaf/0.1/name",
              "Alice", obj_is_literal=True)

    stats = store.stats()
    assert stats['num_triples'] == 1
    assert stats['num_terms'] == 3
    assert stats['has_store'] == True

    print("✓ Statistics retrieved")
    print(f"  {stats}")
    return True


def test_create_rdf_store_function():
    """Test 14: Convenience function."""
    print("\n[Test 14] Convenience function create_rdf_store()")
    print("-" * 60)

    store = create_rdf_store()
    assert isinstance(store, RDFStore)
    assert store.count() == 0

    print("✓ Convenience function works")
    return True


def main():
    """Run all tests."""
    print("=" * 70)
    print("Sabot RDF API Test Suite")
    print("=" * 70)
    print()
    print("Testing high-level sabot.rdf API")
    print()

    tests = [
        test_create_store,
        test_add_single_triple,
        test_add_many_triples,
        test_simple_sparql_query,
        test_multi_pattern_query,
        test_relationship_query,
        test_direct_pattern_matching,
        test_add_prefix,
        test_empty_query,
        test_invalid_query,
        test_limit_query,
        test_distinct_query,
        test_stats,
        test_create_rdf_store_function,
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
    print("Test Summary")
    print("=" * 70)

    passed = sum(results)
    total = len(results)

    print(f"\nPassed: {passed}/{total}")

    if passed == total:
        print("\n✅ All tests passed!")
        print("\nVerified:")
        print("  ✅ Store creation and management")
        print("  ✅ Single and batch triple addition")
        print("  ✅ SPARQL queries (simple, multi-pattern, relationships)")
        print("  ✅ Direct pattern matching")
        print("  ✅ PREFIX management")
        print("  ✅ Query modifiers (LIMIT, DISTINCT)")
        print("  ✅ Error handling")
        print("  ✅ Statistics and diagnostics")
        print()
    else:
        print(f"\n❌ {total - passed} test(s) failed")
        print()

    return passed == total


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
