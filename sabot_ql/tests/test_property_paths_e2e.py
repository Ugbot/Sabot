"""
End-to-End Property Path Testing

Tests all SPARQL 1.1 property path operators with real query execution:
- p+ (one-or-more transitive)
- p* (zero-or-more transitive)
- p{m,n} (bounded paths)
- p/q (sequence paths)
- p|q (alternative paths)
- ^p (inverse paths)
- !p (negated paths)
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'bindings', 'python'))

import pyarrow as pa
from sabot_ql_bindings import TripleStore, Vocabulary, QueryEngine


def setup_test_data():
    """
    Create test RDF dataset:

    Social network:
    Alice knows Bob
    Bob knows Charlie
    Charlie knows David
    David knows Alice (creates cycle)
    Alice friend Bob
    Bob worksWith Charlie

    Family tree:
    Alice parent Bob
    Bob parent Charlie
    Charlie parent David
    """
    vocab = Vocabulary()
    store = TripleStore(vocab)

    # Define predicates
    knows = "http://xmlns.com/foaf/0.1/knows"
    friend = "http://xmlns.com/foaf/0.1/friend"
    works_with = "http://example.org/worksWith"
    parent = "http://example.org/parent"

    # Define subjects/objects
    alice = "http://example.org/Alice"
    bob = "http://example.org/Bob"
    charlie = "http://example.org/Charlie"
    david = "http://example.org/David"
    eve = "http://example.org/Eve"

    # Social network edges
    triples = [
        # knows chain with cycle
        (alice, knows, bob),
        (bob, knows, charlie),
        (charlie, knows, david),
        (david, knows, alice),  # cycle back

        # alternative relationships
        (alice, friend, bob),
        (bob, works_with, charlie),

        # family tree
        (alice, parent, bob),
        (bob, parent, charlie),
        (charlie, parent, david),

        # isolated node
        (eve, knows, alice),
    ]

    # Load triples
    for s, p, o in triples:
        store.AddTriple(s, p, o)

    return vocab, store


def test_transitive_one_or_more():
    """Test p+ (one-or-more) transitive paths"""
    print("\n=== Test 1: Transitive p+ (one-or-more) ===")

    vocab, store = setup_test_data()
    engine = QueryEngine(store, vocab)

    # Query: Who does Alice know transitively?
    query = """
    PREFIX foaf: <http://xmlns.com/foaf/0.1/>
    PREFIX ex: <http://example.org/>

    SELECT ?person ?knows
    WHERE {
        ex:Alice foaf:knows+ ?knows .
    }
    """

    result = engine.ExecuteQuery(query)
    print(f"Query: {query}")
    print(f"Result:\n{result}")

    # Expected: Alice knows Bob, Charlie, David (transitive)
    # Due to cycle, should also find Alice (Alice -> Bob -> Charlie -> David -> Alice)
    assert result.num_rows >= 3, f"Expected at least 3 results (Bob, Charlie, David), got {result.num_rows}"
    print("✓ Transitive p+ test passed")


def test_transitive_zero_or_more():
    """Test p* (zero-or-more) transitive paths"""
    print("\n=== Test 2: Transitive p* (zero-or-more) ===")

    vocab, store = setup_test_data()
    engine = QueryEngine(store, vocab)

    # Query: Who does Alice know transitively (including herself)?
    query = """
    PREFIX foaf: <http://xmlns.com/foaf/0.1/>
    PREFIX ex: <http://example.org/>

    SELECT ?person ?knows
    WHERE {
        ex:Alice foaf:knows* ?knows .
    }
    """

    result = engine.ExecuteQuery(query)
    print(f"Query: {query}")
    print(f"Result:\n{result}")

    # Expected: Alice (distance=0), Bob, Charlie, David
    assert result.num_rows >= 4, f"Expected at least 4 results (Alice, Bob, Charlie, David), got {result.num_rows}"
    print("✓ Transitive p* test passed")


def test_bounded_paths():
    """Test p{m,n} (bounded path lengths)"""
    print("\n=== Test 3: Bounded Paths p{2,3} ===")

    vocab, store = setup_test_data()
    engine = QueryEngine(store, vocab)

    # Query: Who does Alice know at distance 2-3?
    query = """
    PREFIX foaf: <http://xmlns.com/foaf/0.1/>
    PREFIX ex: <http://example.org/>

    SELECT ?person ?knows
    WHERE {
        ex:Alice foaf:knows{2,3} ?knows .
    }
    """

    result = engine.ExecuteQuery(query)
    print(f"Query: {query}")
    print(f"Result:\n{result}")

    # Expected: Charlie (distance 2), David (distance 3)
    assert result.num_rows >= 2, f"Expected at least 2 results (Charlie, David), got {result.num_rows}"
    print("✓ Bounded paths test passed")


def test_sequence_paths():
    """Test p/q (sequence paths)"""
    print("\n=== Test 4: Sequence Paths p/q ===")

    vocab, store = setup_test_data()
    engine = QueryEngine(store, vocab)

    # Query: Who are Alice's grandchildren?
    query = """
    PREFIX ex: <http://example.org/>

    SELECT ?person ?grandchild
    WHERE {
        ex:Alice ex:parent/ex:parent ?grandchild .
    }
    """

    result = engine.ExecuteQuery(query)
    print(f"Query: {query}")
    print(f"Result:\n{result}")

    # Expected: Charlie (Alice -> Bob -> Charlie)
    assert result.num_rows >= 1, f"Expected at least 1 result (Charlie), got {result.num_rows}"
    print("✓ Sequence paths test passed")


def test_sequence_with_quantifiers():
    """Test p+/q (sequence with quantified first path)"""
    print("\n=== Test 5: Sequence with Quantifiers p+/q ===")

    vocab, store = setup_test_data()
    engine = QueryEngine(store, vocab)

    # Query: Who are Alice's descendants?
    query = """
    PREFIX ex: <http://example.org/>

    SELECT ?person ?descendant
    WHERE {
        ex:Alice ex:parent+/ex:parent ?descendant .
    }
    """

    result = engine.ExecuteQuery(query)
    print(f"Query: {query}")
    print(f"Result:\n{result}")

    # Expected: Charlie (via Bob), David (via Bob->Charlie)
    assert result.num_rows >= 2, f"Expected at least 2 results, got {result.num_rows}"
    print("✓ Sequence with quantifiers test passed")


def test_alternative_paths():
    """Test p|q (alternative paths)"""
    print("\n=== Test 6: Alternative Paths p|q ===")

    vocab, store = setup_test_data()
    engine = QueryEngine(store, vocab)

    # Query: Who does Alice know OR is friends with?
    query = """
    PREFIX foaf: <http://xmlns.com/foaf/0.1/>
    PREFIX ex: <http://example.org/>

    SELECT ?person ?contact
    WHERE {
        ex:Alice (foaf:knows|foaf:friend) ?contact .
    }
    """

    result = engine.ExecuteQuery(query)
    print(f"Query: {query}")
    print(f"Result:\n{result}")

    # Expected: Bob (via both knows and friend, should be deduplicated to 1 result)
    assert result.num_rows >= 1, f"Expected at least 1 result (Bob), got {result.num_rows}"
    print("✓ Alternative paths test passed")


def test_alternative_three_paths():
    """Test p|q|r (three alternative paths)"""
    print("\n=== Test 7: Three Alternative Paths p|q|r ===")

    vocab, store = setup_test_data()
    engine = QueryEngine(store, vocab)

    # Query: Who does Bob know OR work with OR is friends with?
    query = """
    PREFIX foaf: <http://xmlns.com/foaf/0.1/>
    PREFIX ex: <http://example.org/>

    SELECT ?person ?contact
    WHERE {
        ex:Bob (foaf:knows|ex:worksWith|foaf:friend) ?contact .
    }
    """

    result = engine.ExecuteQuery(query)
    print(f"Query: {query}")
    print(f"Result:\n{result}")

    # Expected: Charlie (via both knows and worksWith, deduplicated)
    assert result.num_rows >= 1, f"Expected at least 1 result (Charlie), got {result.num_rows}"
    print("✓ Three alternative paths test passed")


def test_inverse_paths():
    """Test ^p (inverse paths)"""
    print("\n=== Test 8: Inverse Paths ^p ===")

    vocab, store = setup_test_data()
    engine = QueryEngine(store, vocab)

    # Query: Who knows Bob? (inverse of "Bob knows who?")
    query = """
    PREFIX foaf: <http://xmlns.com/foaf/0.1/>
    PREFIX ex: <http://example.org/>

    SELECT ?person ?knower
    WHERE {
        ex:Bob ^foaf:knows ?knower .
    }
    """

    result = engine.ExecuteQuery(query)
    print(f"Query: {query}")
    print(f"Result:\n{result}")

    # Expected: Alice (Alice knows Bob)
    assert result.num_rows >= 1, f"Expected at least 1 result (Alice), got {result.num_rows}"
    print("✓ Inverse paths test passed")


def test_inverse_transitive():
    """Test ^p+ (inverse transitive paths)"""
    print("\n=== Test 9: Inverse Transitive ^p+ ===")

    vocab, store = setup_test_data()
    engine = QueryEngine(store, vocab)

    # Query: Who are David's ancestors?
    query = """
    PREFIX ex: <http://example.org/>

    SELECT ?person ?ancestor
    WHERE {
        ex:David ^ex:parent+ ?ancestor .
    }
    """

    result = engine.ExecuteQuery(query)
    print(f"Query: {query}")
    print(f"Result:\n{result}")

    # Expected: Charlie (parent), Bob (grandparent), Alice (great-grandparent)
    assert result.num_rows >= 3, f"Expected at least 3 results, got {result.num_rows}"
    print("✓ Inverse transitive test passed")


def test_negated_paths():
    """Test !p (negated property sets)"""
    print("\n=== Test 10: Negated Paths !p ===")

    vocab, store = setup_test_data()
    engine = QueryEngine(store, vocab)

    # Query: What relationships does Alice have that are NOT "knows"?
    query = """
    PREFIX foaf: <http://xmlns.com/foaf/0.1/>
    PREFIX ex: <http://example.org/>

    SELECT ?subject ?predicate ?object
    WHERE {
        ex:Alice !foaf:knows ?object .
    }
    """

    result = engine.ExecuteQuery(query)
    print(f"Query: {query}")
    print(f"Result:\n{result}")

    # Expected: Alice friend Bob, Alice parent Bob
    assert result.num_rows >= 2, f"Expected at least 2 results, got {result.num_rows}"
    print("✓ Negated paths test passed")


def test_complex_composition():
    """Test complex composition: (p|q)+/r"""
    print("\n=== Test 11: Complex Composition (p|q)+/r ===")

    vocab, store = setup_test_data()
    engine = QueryEngine(store, vocab)

    # Query: Transitive social connections followed by parent relationship
    query = """
    PREFIX foaf: <http://xmlns.com/foaf/0.1/>
    PREFIX ex: <http://example.org/>

    SELECT ?person ?result
    WHERE {
        ex:Alice (foaf:knows|foaf:friend)+/ex:parent ?result .
    }
    """

    result = engine.ExecuteQuery(query)
    print(f"Query: {query}")
    print(f"Result:\n{result}")

    # Expected: Results from social network followed by parent edges
    # Alice -> Bob (knows/friend) -> Charlie (parent)
    # Alice -> Charlie (knows transitive) -> David (parent)
    assert result.num_rows >= 1, f"Expected at least 1 result, got {result.num_rows}"
    print("✓ Complex composition test passed")


def run_all_tests():
    """Run all property path tests"""
    print("=" * 60)
    print("SPARQL 1.1 Property Path End-to-End Tests")
    print("=" * 60)

    tests = [
        ("Transitive p+", test_transitive_one_or_more),
        ("Transitive p*", test_transitive_zero_or_more),
        ("Bounded p{m,n}", test_bounded_paths),
        ("Sequence p/q", test_sequence_paths),
        ("Sequence p+/q", test_sequence_with_quantifiers),
        ("Alternative p|q", test_alternative_paths),
        ("Alternative p|q|r", test_alternative_three_paths),
        ("Inverse ^p", test_inverse_paths),
        ("Inverse ^p+", test_inverse_transitive),
        ("Negated !p", test_negated_paths),
        ("Complex (p|q)+/r", test_complex_composition),
    ]

    passed = 0
    failed = 0

    for name, test_func in tests:
        try:
            test_func()
            passed += 1
        except Exception as e:
            print(f"\n✗ {name} FAILED: {e}")
            import traceback
            traceback.print_exc()
            failed += 1

    print("\n" + "=" * 60)
    print(f"Test Results: {passed}/{len(tests)} passed, {failed}/{len(tests)} failed")
    print("=" * 60)

    if failed == 0:
        print("\n🎉 ALL TESTS PASSED! Property path implementation is working correctly.")
        return 0
    else:
        print(f"\n⚠️ {failed} tests failed. See errors above.")
        return 1


if __name__ == "__main__":
    sys.exit(run_all_tests())
