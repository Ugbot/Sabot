#!/usr/bin/env python3
"""Test Lark transformer with Sabot AST."""

from lark import Lark
from pathlib import Path
import sys

# Add sabot to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from sabot._cython.graph.compiler.lark_transformer import LarkToSabotTransformer
from sabot._cython.graph.compiler.cypher_ast import *

# Load grammar and create parser
grammar_path = Path(__file__).parent.parent.parent / "grammar" / "cypher.lark"
parser = Lark.open(str(grammar_path), parser='earley')
transformer = LarkToSabotTransformer()

# Test queries
test_queries = [
    ("MATCH (a) RETURN a", "Simple MATCH"),
    ("MATCH (a)-[r]->(b) RETURN a, b", "MATCH with relationship"),
    ("MATCH (a:Person {name: 'Alice'}) RETURN a", "MATCH with label and properties"),
    ("MATCH (a) WHERE a.age > 18 RETURN a", "WHERE clause"),
    ("MATCH (a) RETURN count(*)", "COUNT function"),
    ("MATCH (a) RETURN a.name", "Property access"),
    ("MATCH (a) WITH a, count(*) as c RETURN a, c", "WITH clause"),
]

print("Testing Lark → Sabot AST Transformation")
print("=" * 70)

for query, description in test_queries:
    print(f"\n{description}:")
    print(f"Query: {query}")

    try:
        # Parse with Lark
        tree = parser.parse(query)

        # Transform to Sabot AST
        ast = transformer.transform(tree)

        # Verify it's a CypherQuery
        if isinstance(ast, CypherQuery):
            print(f"✅ Transformed successfully!")
            print(f"   AST type: {type(ast).__name__}")
            print(f"   Match clauses: {len(ast.match_clauses)}")
            print(f"   With clauses: {len(ast.with_clauses)}")
            print(f"   Has return: {ast.return_clause is not None}")

            # Show first match pattern
            if ast.match_clauses:
                match = ast.match_clauses[0]
                if match.pattern and match.pattern.elements:
                    elem = match.pattern.elements[0]
                    print(f"   Pattern: {len(elem.nodes)} nodes, {len(elem.edges)} edges")
        else:
            print(f"❌ Unexpected AST type: {type(ast)}")

    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

print("\n" + "=" * 70)
print("Transformation test complete!")
