#!/usr/bin/env python3
"""
Test BIND functionality with Arrow expressions
"""
import sys
sys.path.insert(0, '/Users/bengamble/Sabot/sabot_ql/bindings/python')

# Set up environment for vendored Arrow
import os
os.environ['DYLD_LIBRARY_PATH'] = '/Users/bengamble/Sabot/vendor/arrow/cpp/build/install/lib:/Users/bengamble/Sabot/MarbleDB/build:/Users/bengamble/Sabot/sabot_ql/build'

# Import after setting environment
from sabot_ql_impl import TripleStore, Vocabulary, QueryEngine, Term

def test_bind_arithmetic():
    """Test BIND with arithmetic expression: ?age + 1"""
    print("=" * 60)
    print("Test BIND Functionality")
    print("=" * 60)

    # Create store and vocabulary
    print("\n1. Creating triple store and vocabulary...")
    store = TripleStore.create_memory()
    vocab = Vocabulary.create_memory()

    # Add test data
    print("2. Adding test data...")
    # <Alice> <age> 30
    alice_term = Term.iri("http://example.org/Alice")
    age_pred_term = Term.iri("http://example.org/age")
    age_30_term = Term.literal("30")

    alice_id = vocab.add_term(alice_term)
    age_pred_id = vocab.add_term(age_pred_term)
    age_30_id = vocab.add_term(age_30_term)

    # Insert triple
    store.insert_triple(alice_id.get_bits(), age_pred_id.get_bits(), age_30_id.get_bits())
    store.flush()

    print("   Inserted: <Alice> <age> 30")

    # Create query engine
    print("\n3. Creating query engine...")
    engine = QueryEngine(store, vocab)

    # Test query with BIND
    query = """
    SELECT ?person ?age ?next_year_age WHERE {
        ?person <http://example.org/age> ?age .
        BIND(?age + 1 AS ?next_year_age)
    }
    """

    print("\n4. Executing BIND query:")
    print(f"   {query}")

    try:
        results = engine.execute(query)

        print("\n5. Results:")
        print(f"   Number of rows: {results.num_rows}")
        print(f"   Number of columns: {results.num_columns}")
        print(f"   Column names: {results.column_names}")

        if results.num_rows > 0:
            print("\n   Data:")
            for i in range(results.num_rows):
                person = results.column(0)[i].as_py()
                age = results.column(1)[i].as_py()
                next_year = results.column(2)[i].as_py()
                print(f"     Row {i}: person={person}, age={age}, next_year_age={next_year}")

            # Verify arithmetic worked
            age_val = results.column(1)[0].as_py()
            next_year_val = results.column(2)[0].as_py()

            if next_year_val == age_val + 1:
                print("\n✅ BIND arithmetic expression works correctly!")
                print(f"   {age_val} + 1 = {next_year_val}")
                return True
            else:
                print(f"\n❌ BIND arithmetic failed: Expected {age_val + 1}, got {next_year_val}")
                return False
        else:
            print("\n❌ No results returned")
            return False

    except Exception as e:
        print(f"\n❌ Error executing query: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = test_bind_arithmetic()
    sys.exit(0 if success else 1)
