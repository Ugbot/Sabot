#!/usr/bin/env python3
"""Test Lark parser with openCypher grammar."""

from lark import Lark
from pathlib import Path

# Try to load the Lark grammar
grammar_path = Path(__file__).parent.parent.parent / "grammar" / "cypher.lark"

print(f"Loading grammar from: {grammar_path}")
print(f"Grammar exists: {grammar_path.exists()}")

# Try to create Lark parser
try:
    parser = Lark.open(str(grammar_path), parser='earley')
    print("\n✅ Lark parser created successfully!")

    # Try parsing simple queries
    test_queries = [
        "MATCH (a) RETURN a",
        "MATCH (a)-[r]->(b) RETURN a, b",
        "MATCH (a:Person {name: 'Alice'}) RETURN a",
        "MATCH (a) WHERE a.age > 18 RETURN a",  # Simpler property test
        "MATCH (a) RETURN count(*)",  # Simpler count test
        "MATCH (a)-[r:KNOWS]->(b) WHERE a.age > 18 RETURN a.name, b.name",
        "MATCH (a) WITH a, count(*) as c RETURN a, c",
    ]

    for query in test_queries:
        print(f"\n{'='*60}")
        print(f"Query: {query}")
        try:
            tree = parser.parse(query)
            print("✅ Parse successful!")
            print(f"Parse tree (first 500 chars):")
            pretty = tree.pretty()
            print(pretty[:500] + ("..." if len(pretty) > 500 else ""))
        except Exception as e:
            print(f"❌ Parse failed: {e}")

except Exception as e:
    print(f"\n❌ Error creating parser: {e}")
    print(f"Error type: {type(e).__name__}")
    import traceback
    traceback.print_exc()
