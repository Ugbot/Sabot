"""
Cypher Query Parser

Parses Cypher queries into AST.
Grammar inspired by KuzuDB's Cypher.g4 (ANTLR4 grammar)
but implemented in Python using pyparsing for simplicity.

Reference: vendor/kuzu/src/antlr4/Cypher.g4
(MIT License, Copyright 2022-2025 Kùzu Inc.)

This parser supports a subset of Cypher:
- MATCH clauses with node and relationship patterns
- WHERE clauses with boolean expressions
- RETURN clauses with projection
- ORDER BY, LIMIT, SKIP
- Variable-length paths (*1..3)
"""

from pyparsing import (
    Word, alphas, alphanums, nums,
    Literal as L, Optional as Opt, ZeroOrMore, OneOrMore,
    Group, Suppress, Forward, Keyword,
    CaselessKeyword, QuotedString, Regex,
    delimitedList, pyparsing_common,
    ParserElement,
)

from .cypher_ast import *

# Enable packrat parsing for better performance
ParserElement.enablePackrat()


class CypherParser:
    """
    Cypher query parser.

    Parses Cypher query strings into CypherQuery AST objects.

    Example:
        >>> parser = CypherParser()
        >>> query = parser.parse("MATCH (a:Person)-[:KNOWS]->(b) RETURN a.name")
        >>> print(query)
        CypherQuery(MATCH (1 patterns), RETURN (1 items))
    """

    def __init__(self):
        """Initialize parser grammar."""
        self._build_grammar()

    def _build_grammar(self):
        """
        Build pyparsing grammar for Cypher.
        Inspired by KuzuDB's Cypher.g4.
        """
        # ====================================================================
        # Lexical Elements
        # ====================================================================

        # Keywords (case-insensitive)
        MATCH = CaselessKeyword("MATCH")
        OPTIONAL = CaselessKeyword("OPTIONAL")
        WHERE = CaselessKeyword("WHERE")
        RETURN = CaselessKeyword("RETURN")
        AS = CaselessKeyword("AS")
        ORDER = CaselessKeyword("ORDER")
        BY = CaselessKeyword("BY")
        LIMIT = CaselessKeyword("LIMIT")
        SKIP = CaselessKeyword("SKIP")
        DISTINCT = CaselessKeyword("DISTINCT")
        ASC = CaselessKeyword("ASC") | CaselessKeyword("ASCENDING")
        DESC = CaselessKeyword("DESC") | CaselessKeyword("DESCENDING")
        AND = CaselessKeyword("AND")
        OR = CaselessKeyword("OR")
        XOR = CaselessKeyword("XOR")
        NOT = CaselessKeyword("NOT")

        # Identifiers (variables, labels, properties)
        identifier = Word(alphas + "_", alphanums + "_")

        # Literals
        integer = pyparsing_common.signed_integer()
        number = pyparsing_common.number()
        string_literal = (QuotedString('"', escChar='\\') |
                         QuotedString("'", escChar='\\'))
        boolean = (CaselessKeyword("true") | CaselessKeyword("false"))
        null = CaselessKeyword("null")

        # ====================================================================
        # Expression Grammar
        # ====================================================================

        expression = Forward()

        # Literal values
        literal_expr = (
            boolean.setParseAction(lambda t: Literal(t[0].lower() == 'true')) |
            null.setParseAction(lambda t: Literal(None)) |
            number.setParseAction(lambda t: Literal(t[0])) |
            string_literal.setParseAction(lambda t: Literal(t[0]))
        )

        # Variable reference
        variable_expr = identifier.copy().setParseAction(
            lambda t: Variable(t[0])
        )

        # Property access: variable.property
        property_expr = (
            identifier + Suppress(".") + identifier
        ).setParseAction(
            lambda t: PropertyAccess(t[0], t[1])
        )

        # Primary expression
        primary_expr = (
            property_expr |
            literal_expr |
            variable_expr |
            Suppress("(") + expression + Suppress(")")
        )

        # Comparison operators
        comp_op = (
            L("<=") | L(">=") | L("<>") |
            L("<") | L(">") | L("=")
        )

        # Comparison expression
        comparison_expr = (
            primary_expr + Opt(comp_op + primary_expr)
        ).setParseAction(self._make_comparison)

        # NOT expression
        not_expr = (
            ZeroOrMore(NOT) + comparison_expr
        ).setParseAction(self._make_not)

        # AND expression
        and_expr = (
            not_expr + ZeroOrMore(AND + not_expr)
        ).setParseAction(self._make_binary_op)

        # OR/XOR expression
        or_expr = (
            and_expr + ZeroOrMore((OR | XOR) + and_expr)
        ).setParseAction(self._make_binary_op)

        expression <<= or_expr

        # ====================================================================
        # Pattern Grammar (inspired by Cypher.g4)
        # ====================================================================

        # Node labels: :Label or :Label1|Label2
        node_labels = (
            Suppress(":") + delimitedList(identifier, delim="|")
        )

        # Properties: {key: value, ...}
        property_pair = (identifier + Suppress(":") + expression)
        properties = (
            Suppress("{") +
            Opt(delimitedList(Group(property_pair))) +
            Suppress("}")
        )

        # Node pattern: (variable:Label {prop: val})
        node_pattern = (
            Suppress("(") +
            Opt(identifier)("variable") +
            Opt(node_labels)("labels") +
            Opt(properties)("properties") +
            Suppress(")")
        ).setParseAction(self._make_node_pattern)

        # Variable-length spec: *1..3 or *2 or *
        var_length = (
            Suppress("*") +
            Opt(
                (integer + Opt(Suppress("..") + Opt(integer))) |
                (Suppress("..") + integer)
            )
        ).setParseAction(self._make_recursive_info)

        # Relationship types: :TYPE or :TYPE1|TYPE2
        rel_types = (
            Suppress(":") + delimitedList(identifier, delim="|")
        )

        # Relationship detail: [variable:TYPE {prop: val}]
        rel_detail = (
            Suppress("[") +
            Opt(identifier)("variable") +
            Opt(rel_types)("types") +
            Opt(var_length)("recursive") +
            Opt(properties)("properties") +
            Suppress("]")
        )

        # Arrow directions
        left_arrow = L("<-")
        right_arrow = L("->")
        dash = L("-")

        # Relationship pattern: -[r:TYPE]->
        rel_pattern_right = (
            dash + Opt(rel_detail) + right_arrow
        ).setParseAction(lambda t: self._make_rel_pattern(t, ArrowDirection.RIGHT))

        rel_pattern_left = (
            left_arrow + Opt(rel_detail) + dash
        ).setParseAction(lambda t: self._make_rel_pattern(t, ArrowDirection.LEFT))

        rel_pattern_both = (
            dash + Opt(rel_detail) + dash
        ).setParseAction(lambda t: self._make_rel_pattern(t, ArrowDirection.BOTH))

        rel_pattern = (
            rel_pattern_right | rel_pattern_left | rel_pattern_both
        )

        # Pattern element: (a)-[r]->(b)-[s]->(c)
        pattern_element = (
            node_pattern +
            ZeroOrMore(Group(rel_pattern + node_pattern))
        ).setParseAction(self._make_pattern_element)

        # Pattern (can have multiple elements separated by comma)
        pattern = delimitedList(
            Group(pattern_element),
            delim=","
        ).setParseAction(self._make_pattern)

        # ====================================================================
        # Clause Grammar
        # ====================================================================

        # MATCH clause
        match_clause = (
            Opt(OPTIONAL)("optional") +
            MATCH +
            pattern("pattern") +
            Opt(WHERE + expression("where"))
        ).setParseAction(self._make_match_clause)

        # Projection item: expression or expression AS alias
        projection_item = (
            expression + Opt(AS + identifier)
        ).setParseAction(self._make_projection_item)

        # ORDER BY
        order_item = (
            expression + Opt(ASC | DESC)
        ).setParseAction(self._make_order_by)

        order_by_clause = (
            ORDER + BY + delimitedList(Group(order_item))
        )

        # RETURN clause
        return_clause = (
            RETURN +
            Opt(DISTINCT)("distinct") +
            delimitedList(Group(projection_item))("items") +
            Opt(order_by_clause)("order_by") +
            Opt(SKIP + integer)("skip") +
            Opt(LIMIT + integer)("limit")
        ).setParseAction(self._make_return_clause)

        # ====================================================================
        # Top-Level Query
        # ====================================================================

        # Complete Cypher query
        cypher_query = (
            OneOrMore(Group(match_clause))("match_clauses") +
            Opt(return_clause)("return_clause")
        ).setParseAction(self._make_cypher_query)

        self.grammar = cypher_query

    # ========================================================================
    # Parse Actions (construct AST nodes)
    # ========================================================================

    def _make_comparison(self, tokens):
        """Create Comparison expression."""
        if len(tokens) == 1:
            return tokens[0]
        elif len(tokens) == 3:
            left, op_str, right = tokens
            op_map = {
                '=': ComparisonOp.EQ,
                '<>': ComparisonOp.NEQ,
                '<': ComparisonOp.LT,
                '<=': ComparisonOp.LTE,
                '>': ComparisonOp.GT,
                '>=': ComparisonOp.GTE,
            }
            return Comparison(left, op_map[op_str], right)
        return tokens[0]

    def _make_not(self, tokens):
        """Create NOT expression."""
        not_count = sum(1 for t in tokens if isinstance(t, str) and t.upper() == 'NOT')
        expr = [t for t in tokens if not (isinstance(t, str) and t.upper() == 'NOT')][-1]

        # Even number of NOTs cancel out
        if not_count % 2 == 0:
            return expr

        # Odd number of NOTs
        return BinaryOp(Literal(True), BinaryOpType.XOR, expr)  # Simulate NOT

    def _make_binary_op(self, tokens):
        """Create binary operation (AND, OR, etc.)."""
        if len(tokens) == 1:
            return tokens[0]

        result = tokens[0]
        i = 1
        while i < len(tokens):
            op_str = tokens[i].upper()
            right = tokens[i + 1]

            op_map = {
                'AND': BinaryOpType.AND,
                'OR': BinaryOpType.OR,
                'XOR': BinaryOpType.XOR,
            }
            result = BinaryOp(result, op_map.get(op_str, BinaryOpType.AND), right)
            i += 2

        return result

    def _make_node_pattern(self, tokens):
        """Create NodePattern from parse tokens."""
        variable = tokens.get('variable', [None])[0] if 'variable' in tokens else None
        labels = list(tokens.get('labels', []))
        properties = dict(tokens.get('properties', []))
        return NodePattern(variable=variable, labels=labels, properties=properties)

    def _make_recursive_info(self, tokens):
        """Create RecursiveInfo for variable-length paths."""
        if len(tokens) == 0:
            # Just * → unbounded
            return RecursiveInfo(min_hops=1, max_hops=None)
        elif len(tokens) == 1:
            # *2 → exactly 2 hops
            return RecursiveInfo(min_hops=tokens[0], max_hops=tokens[0])
        elif len(tokens) == 2:
            # *1..3 → 1 to 3 hops
            return RecursiveInfo(min_hops=tokens[0], max_hops=tokens[1])
        return RecursiveInfo()

    def _make_rel_pattern(self, tokens, direction):
        """Create RelPattern from parse tokens."""
        variable = None
        types = []
        properties = {}
        recursive = None

        # Debug: See what we're receiving
        # print(f"DEBUG _make_rel_pattern: tokens={tokens}, len={len(tokens)}, direction={direction}")
        # print(f"  hasattr variable: {hasattr(tokens, 'variable')}, value: {tokens.variable if hasattr(tokens, 'variable') else 'N/A'}")
        # print(f"  hasattr types: {hasattr(tokens, 'types')}, value: {tokens.types if hasattr(tokens, 'types') else 'N/A'}")

        # Try to access named groups from tokens directly (pyparsing ParseResults)
        if hasattr(tokens, 'variable'):
            var_list = tokens.variable if isinstance(tokens.variable, list) else [tokens.variable]
            variable = var_list[0] if var_list and var_list[0] else None

        if hasattr(tokens, 'types'):
            # types might be a string, list, or ParseResults
            # print(f"  Processing types: {tokens.types}, type={type(tokens.types)}")
            if isinstance(tokens.types, str):
                types = [tokens.types]
            else:
                # It's either a list or ParseResults - both are iterable
                types = list(tokens.types) if tokens.types else []

        if hasattr(tokens, 'properties'):
            props = tokens.properties
            # properties is a list of (key, value) tuples from property_pair
            if props and isinstance(props, list) and len(props) > 0:
                try:
                    properties = dict(props)
                except (ValueError, TypeError):
                    properties = {}
            else:
                properties = {}

        if hasattr(tokens, 'recursive'):
            rec = tokens.recursive if isinstance(tokens.recursive, list) else [tokens.recursive]
            recursive = rec[0] if rec and rec[0] else None

        return RelPattern(
            variable=variable,
            types=types,
            direction=direction,
            properties=properties,
            recursive=recursive
        )

    def _make_pattern_element(self, tokens):
        """Create PatternElement from node-edge-node chain."""
        nodes = []
        edges = []

        # Debug: See what we're receiving
        # print(f"DEBUG _make_pattern_element: tokens={tokens}, len={len(tokens)}")
        # if len(tokens) > 0:
        #     print(f"  First token: type={type(tokens[0])}, value={tokens[0]}")

        # First node (should be a NodePattern object from _make_node_pattern)
        if len(tokens) > 0 and isinstance(tokens[0], NodePattern):
            nodes.append(tokens[0])

        # Subsequent (edge, node) pairs
        # These come from ZeroOrMore(Group(rel_pattern + node_pattern))
        for i, pair in enumerate(tokens[1:]):
            # print(f"  Pair {i}: type={type(pair)}, len={len(pair) if hasattr(pair, '__len__') else 'N/A'}")
            if hasattr(pair, '__len__') and len(pair) >= 2:
                # pair[0] should be RelPattern, pair[1] should be NodePattern
                if isinstance(pair[0], RelPattern):
                    edges.append(pair[0])
                if isinstance(pair[1], NodePattern):
                    nodes.append(pair[1])

        return PatternElement(nodes=nodes, edges=edges)

    def _make_pattern(self, tokens):
        """Create Pattern from elements."""
        # Debug: See what we're receiving
        # print(f"DEBUG _make_pattern: tokens={tokens}, len={len(tokens)}")

        elements = []
        for token in tokens:
            # Unwrap Groups containing PatternElements
            if isinstance(token, PatternElement):
                elements.append(token)
            elif hasattr(token, '__iter__') and not isinstance(token, str):
                # Token might be a Group/ParseResults containing PatternElement
                for item in token:
                    if isinstance(item, PatternElement):
                        elements.append(item)
                        break

        return Pattern(elements=elements)

    def _make_match_clause(self, tokens):
        """Create MatchClause."""
        pattern = tokens.get('pattern')
        where = tokens.get('where', [None])[0]
        optional = 'optional' in tokens and bool(tokens['optional'])

        return MatchClause(
            pattern=pattern,
            where=where,
            optional=optional
        )

    def _make_projection_item(self, tokens):
        """Create ProjectionItem."""
        expr = tokens[0]
        alias = tokens[1] if len(tokens) > 1 else None
        return ProjectionItem(expression=expr, alias=alias)

    def _make_order_by(self, tokens):
        """Create OrderBy."""
        expr = tokens[0]
        ascending = True
        if len(tokens) > 1:
            dir_str = tokens[1].upper()
            ascending = 'ASC' in dir_str

        return OrderBy(expression=expr, ascending=ascending)

    def _make_return_clause(self, tokens):
        """Create ReturnClause."""
        distinct = 'distinct' in tokens and bool(tokens['distinct'])
        items = [item[0] for item in tokens.get('items', [])]

        order_by = []
        if 'order_by' in tokens:
            order_by = [item[0] for item in tokens['order_by'][0]]

        # Extract integers from SKIP/LIMIT (which parse as ["SKIP", int] or ["LIMIT", int])
        skip_tokens = tokens.get('skip', [])
        skip = None
        for token in skip_tokens:
            if isinstance(token, int):
                skip = token
                break

        limit_tokens = tokens.get('limit', [])
        limit = None
        for token in limit_tokens:
            if isinstance(token, int):
                limit = token
                break

        return ReturnClause(
            items=items,
            distinct=distinct,
            order_by=order_by,
            skip=skip,
            limit=limit
        )

    def _make_cypher_query(self, tokens):
        """Create CypherQuery from parsed tokens."""
        match_clauses = [clause[0] for clause in tokens.get('match_clauses', [])]
        return_clause = tokens.get('return_clause', [None])[0]

        return CypherQuery(
            match_clauses=match_clauses,
            return_clause=return_clause
        )

    # ========================================================================
    # Public API
    # ========================================================================

    def parse(self, query_string: str) -> CypherQuery:
        """
        Parse Cypher query string into AST.

        Args:
            query_string: Cypher query string

        Returns:
            CypherQuery AST object

        Raises:
            ParseException: If query syntax is invalid

        Example:
            >>> parser = CypherParser()
            >>> ast = parser.parse("MATCH (a:Person)-[:KNOWS]->(b) RETURN a.name")
        """
        # Remove comments and normalize whitespace
        query_string = query_string.strip()

        # Parse
        result = self.grammar.parseString(query_string, parseAll=True)

        return result[0]
