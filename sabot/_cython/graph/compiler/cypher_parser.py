"""
Cypher Query Parser

Parses Cypher queries into AST using official openCypher grammar.

Based on:
- Official openCypher EBNF grammar (https://s3.amazonaws.com/artifacts.opencypher.org/M23/cypher.ebnf)
- Lark parser generator (https://github.com/lark-parser/lark)
- openCypher M23 specification

This parser supports full Cypher syntax including:
- MATCH clauses with node and relationship patterns
- WITH clauses for multi-stage pipelines
- WHERE clauses with boolean expressions
- RETURN clauses with projection
- ORDER BY, LIMIT, SKIP
- Variable-length paths (*1..3)
- Property access, functions, aggregations
"""

from pathlib import Path
from lark import Lark
from .cypher_ast import CypherQuery
from .lark_transformer import LarkToSabotTransformer


# Load grammar once at module level for better performance
# Path: /Users/bengamble/Sabot/sabot/_cython/graph/compiler/cypher_parser.py
#  -> parent = compiler, parent.parent = graph, parent.parent.parent = _cython
#  -> parent.parent.parent.parent = sabot, parent.parent.parent.parent.parent = Sabot (repo root)
_GRAMMAR_PATH = Path(__file__).parent.parent.parent.parent.parent / "grammar" / "cypher.lark"
_PARSER = None
_TRANSFORMER = LarkToSabotTransformer()


def _get_parser():
    """Get or create the Lark parser (lazy initialization)."""
    global _PARSER
    if _PARSER is None:
        _PARSER = Lark.open(str(_GRAMMAR_PATH), parser='earley', start='start')
    return _PARSER


class CypherParser:
    """
    Cypher query parser using official openCypher grammar.

    Parses Cypher query strings into CypherQuery AST objects using:
    - Lark parser generator with Earley algorithm
    - Official openCypher EBNF grammar converted to Lark format
    - Custom transformer to convert parse tree to Sabot AST

    Example:
        >>> parser = CypherParser()
        >>> query = parser.parse("MATCH (a:Person)-[:KNOWS]->(b) RETURN a.name")
        >>> print(query)
        CypherQuery(MATCH (1 patterns), RETURN (1 items))

    Supports WITH clause for multi-stage pipelines:
        >>> query = parser.parse("MATCH (a) WITH a, count(*) as c RETURN c")
        >>> print(len(query.with_clauses))
        1
    """

    def __init__(self):
        """Initialize parser (uses shared grammar for performance)."""
        self.parser = _get_parser()
        self.transformer = _TRANSFORMER

    def parse(self, query_string: str) -> CypherQuery:
        """
        Parse Cypher query string into AST.

        Args:
            query_string: Cypher query string

        Returns:
            CypherQuery AST object

        Raises:
            LarkError: If query syntax is invalid

        Example:
            >>> parser = CypherParser()
            >>> ast = parser.parse("MATCH (a:Person)-[:KNOWS]->(b) RETURN a.name")
            >>> print(ast)
            CypherQuery(MATCH (1 patterns), RETURN (1 items))
        """
        query_string = query_string.strip()

        # Parse query string into parse tree
        tree = self.parser.parse(query_string)

        # Transform parse tree to Sabot AST
        ast = self.transformer.transform(tree)

        return ast
