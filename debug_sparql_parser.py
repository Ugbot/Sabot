#!/usr/bin/env python3
"""Debug SPARQL Parser - inspect parse results"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from sabot._cython.graph.compiler.sparql_parser import SPARQLParser

# Simple query
query = """
SELECT ?person
WHERE {
    ?person <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://xmlns.com/foaf/0.1/Person> .
}
"""

print("=" * 70)
print("SPARQL Parser Debug")
print("=" * 70)
print(f"\nQuery:\n{query}")

parser = SPARQLParser()

try:
    ast = parser.parse(query)

    print("\n" + "=" * 70)
    print("Parse Result:")
    print("=" * 70)

    print(f"\nAST Type: {type(ast)}")
    print(f"AST: {ast}")

    print(f"\nSELECT Clause:")
    print(f"  Type: {type(ast.select_clause)}")
    print(f"  Variables: {ast.select_clause.variables}")
    print(f"  Distinct: {ast.select_clause.distinct}")

    print(f"\nWHERE Clause:")
    print(f"  Type: {type(ast.where_clause)}")
    print(f"  WHERE: {ast.where_clause}")

    print(f"\nBGP:")
    print(f"  Type: {type(ast.where_clause.bgp)}")
    print(f"  BGP: {ast.where_clause.bgp}")

    if ast.where_clause.bgp:
        print(f"\nBGP Triples:")
        print(f"  Num triples: {len(ast.where_clause.bgp.triples)}")
        for i, triple in enumerate(ast.where_clause.bgp.triples):
            print(f"  Triple {i}: {triple}")
    else:
        print("\n❌ BGP is None!")

    print(f"\nFilters:")
    if ast.where_clause.filters:
        for i, f in enumerate(ast.where_clause.filters):
            print(f"  Filter {i}: {f}")
    else:
        print("  (no filters)")

except Exception as e:
    print(f"\n❌ Parse failed: {e}")
    import traceback
    traceback.print_exc()
