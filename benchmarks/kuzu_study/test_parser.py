#!/usr/bin/env python3
"""Test Cypher parser with aggregations."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from sabot._cython.graph.compiler.cypher_parser import CypherParser
from sabot._cython.graph.compiler.cypher_ast import FunctionCall

parser = CypherParser()

# Test 1: COUNT(*)
query1 = "MATCH (a)-[r]->(b) RETURN count(*)"
ast1 = parser.parse(query1)
print("Test 1: COUNT(*)")
print(f"  Query: {query1}")
print(f"  AST: {ast1}")
print(f"  Return items: {ast1.return_clause.items}")
first_item = ast1.return_clause.items[0]
print(f"  First item expression: {first_item.expression}")
print(f"  Is FunctionCall? {isinstance(first_item.expression, FunctionCall)}")
if isinstance(first_item.expression, FunctionCall):
    print(f"  Function name: {first_item.expression.name}")
    print(f"  Args: {first_item.expression.args}")
print()

# Test 2: count(DISTINCT a.name)
query2 = "MATCH (a)-[r]->(b) RETURN count(DISTINCT a.name)"
ast2 = parser.parse(query2)
print("Test 2: count(DISTINCT a.name)")
print(f"  Query: {query2}")
print(f"  AST: {ast2}")
print(f"  Return items: {ast2.return_clause.items}")
first_item = ast2.return_clause.items[0]
print(f"  First item expression: {first_item.expression}")
print(f"  Is FunctionCall? {isinstance(first_item.expression, FunctionCall)}")
if isinstance(first_item.expression, FunctionCall):
    print(f"  Function name: {first_item.expression.name}")
    print(f"  Args: {first_item.expression.args}")
    print(f"  Distinct: {first_item.expression.distinct}")
print()

# Test 3: AVG(a.age)
query3 = "MATCH (a) RETURN avg(a.age)"
ast3 = parser.parse(query3)
print("Test 3: avg(a.age)")
print(f"  Query: {query3}")
print(f"  AST: {ast3}")
print(f"  Return items: {ast3.return_clause.items}")
first_item = ast3.return_clause.items[0]
print(f"  First item expression: {first_item.expression}")
print(f"  Is FunctionCall? {isinstance(first_item.expression, FunctionCall)}")
if isinstance(first_item.expression, FunctionCall):
    print(f"  Function name: {first_item.expression.name}")
    print(f"  Args: {first_item.expression.args}")
print()

print("✅ Parser tests complete!")
