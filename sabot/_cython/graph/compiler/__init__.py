"""
Sabot Graph Query Compiler

Cypher and SPARQL query compilers for graph queries.
Adapted from KuzuDB (MIT License, Copyright 2022-2025 Kùzu Inc.)

This module provides:
- Cypher query parser and compiler
- SPARQL query parser and compiler
- AST node definitions
- Query translation to Sabot pattern matching
"""

# Cypher imports
from .cypher_ast import (
    # Pattern nodes
    NodePattern,
    RelPattern,
    PatternElement,
    Pattern,
    # Query clauses
    MatchClause,
    WhereClause,
    ReturnClause,
    # Expressions
    PropertyAccess,
    Literal,
    BinaryOp,
    Comparison,
    Variable,
    # Top-level
    CypherQuery,
)

from .cypher_parser import CypherParser
from .cypher_translator import CypherTranslator

# SPARQL imports
from .sparql_ast import (
    # RDF Terms
    IRI,
    Literal as SPARQLLiteral,
    Variable as SPARQLVariable,
    BlankNode,
    # Triple patterns
    TriplePattern,
    BasicGraphPattern,
    # Expressions
    Expression,
    VariableExpr,
    LiteralExpr,
    Comparison as SPARQLComparison,
    BooleanExpr,
    FunctionCall,
    # Query clauses
    Filter,
    WhereClause as SPARQLWhereClause,
    SelectClause,
    SolutionModifier,
    # Top-level
    SPARQLQuery,
)

from .sparql_parser import SPARQLParser, parse_sparql
from .sparql_translator import SPARQLTranslator, execute_sparql

__all__ = [
    # Cypher AST nodes
    'NodePattern',
    'RelPattern',
    'PatternElement',
    'Pattern',
    'MatchClause',
    'WhereClause',
    'ReturnClause',
    'PropertyAccess',
    'Literal',
    'BinaryOp',
    'Comparison',
    'Variable',
    'CypherQuery',
    # Cypher Compiler
    'CypherParser',
    'CypherTranslator',
    # SPARQL AST nodes
    'IRI',
    'SPARQLLiteral',
    'SPARQLVariable',
    'BlankNode',
    'TriplePattern',
    'BasicGraphPattern',
    'Expression',
    'VariableExpr',
    'LiteralExpr',
    'SPARQLComparison',
    'BooleanExpr',
    'FunctionCall',
    'Filter',
    'SPARQLWhereClause',
    'SelectClause',
    'SolutionModifier',
    'SPARQLQuery',
    # SPARQL Compiler
    'SPARQLParser',
    'SPARQLTranslator',
    'parse_sparql',
    'execute_sparql',
]
