"""
SPARQL Query Parser

Parses SPARQL 1.1 SELECT queries into AST nodes.
Uses pyparsing for grammar definition (similar to Cypher parser).

Inspired by:
- QLever (https://github.com/ad-freiburg/qlever)
- Apache Jena ARQ
- W3C SPARQL 1.1 Specification

Supported subset:
- SELECT with projection (SELECT * or SELECT ?var1 ?var2)
- WHERE clause with Basic Graph Patterns (BGP)
- Triple patterns with variables
- FILTER expressions (comparison operators)
- LIMIT and OFFSET
- DISTINCT modifier

Example:
    SELECT ?person ?name
    WHERE {
        ?person rdf:type foaf:Person .
        ?person foaf:name ?name .
        FILTER (?name != "")
    }
    LIMIT 10
"""

from pyparsing import (
    Word, Literal, CaselessKeyword, Group, Optional, ZeroOrMore, OneOrMore,
    alphas, alphanums, nums, pyparsing_common, Suppress, Regex,
    ParseException, delimitedList, QuotedString, ParserElement
)

from .sparql_ast import (
    IRI, Literal as RDFLiteral, Variable, BlankNode,
    TriplePattern, BasicGraphPattern,
    Expression, VariableExpr, LiteralExpr, Comparison, BooleanExpr, FunctionCall,
    ComparisonOp, BooleanOp,
    Filter, WhereClause, SelectClause, SolutionModifier, SPARQLQuery
)

# Enable packrat parsing for performance
ParserElement.enablePackrat()


class SPARQLParser:
    """
    SPARQL query parser using pyparsing.

    Architecture:
    1. Grammar definition (pyparsing combinators)
    2. Parse actions (convert tokens to AST nodes)
    3. Top-level parse() method

    Example:
        parser = SPARQLParser()
        query = "SELECT ?s ?p WHERE { ?s ?p ?o . } LIMIT 10"
        ast = parser.parse(query)
        print(ast.select_clause.variables)  # ['s', 'p']
    """

    def __init__(self):
        """Initialize parser with SPARQL grammar."""
        self._build_grammar()

    def _build_grammar(self):
        """Build SPARQL grammar using pyparsing."""

        # ====================================================================
        # Terminals (Tokens)
        # ====================================================================

        # Keywords (case-insensitive)
        SELECT = CaselessKeyword("SELECT")
        DISTINCT = CaselessKeyword("DISTINCT")
        WHERE = CaselessKeyword("WHERE")
        FILTER = CaselessKeyword("FILTER")
        LIMIT = CaselessKeyword("LIMIT")
        OFFSET = CaselessKeyword("OFFSET")
        ORDER = CaselessKeyword("ORDER")
        BY = CaselessKeyword("BY")
        ASC = CaselessKeyword("ASC")
        DESC = CaselessKeyword("DESC")

        # RDF/SPARQL specific keywords
        A = Literal("a")  # Shorthand for rdf:type

        # Punctuation
        LBRACE = Suppress(Literal("{"))
        RBRACE = Suppress(Literal("}"))
        LPAREN = Suppress(Literal("("))
        RPAREN = Suppress(Literal(")"))
        DOT = Suppress(Literal("."))
        SEMICOLON = Suppress(Literal(";"))
        COMMA = Suppress(Literal(","))

        # ====================================================================
        # RDF Terms
        # ====================================================================

        # Variable: ?name or $name
        var_name = Word(alphas + "_", alphanums + "_")
        variable = (Suppress(Literal("?")) | Suppress(Literal("$"))) + var_name
        variable.setParseAction(self._make_variable)

        # IRI: <http://example.org/...> or prefixed name (foaf:Person)
        iri_ref = Suppress(Literal("<")) + Regex(r'[^<>"{}|^`\\\s]+') + Suppress(Literal(">"))
        iri_ref.setParseAction(self._make_iri)

        # Prefixed name: prefix:localName (e.g., foaf:Person, rdf:type)
        prefix = Word(alphas, alphanums + "_")
        local_name = Word(alphas + "_", alphanums + "_")
        prefixed_name = prefix + Suppress(Literal(":")) + local_name
        prefixed_name.setParseAction(self._make_prefixed_iri)

        # IRI can be either <...> or prefix:name
        iri = iri_ref | prefixed_name

        # Literal: "string", "string"@en, "42"^^<http://www.w3.org/2001/XMLSchema#integer>
        string_literal = QuotedString('"', escChar='\\')
        language_tag = Suppress(Literal("@")) + Word(alphas, alphanums + "-")
        datatype_iri = Suppress(Literal("^^")) + iri

        literal = string_literal + Optional(language_tag) + Optional(datatype_iri)
        literal.setParseAction(self._make_literal)

        # Blank node: _:b1 or []
        blank_node_label = Suppress(Literal("_:")) + Word(alphas + "_", alphanums + "_")
        blank_node_label.setParseAction(self._make_blank_node)

        anon_blank_node = Suppress(Literal("[")) + Suppress(Literal("]"))
        anon_blank_node.setParseAction(lambda: BlankNode(id=None))

        blank_node = blank_node_label | anon_blank_node

        # ====================================================================
        # Triple Patterns
        # ====================================================================

        # Subject: IRI, Variable, or BlankNode
        subject = iri | variable | blank_node

        # Predicate: IRI, Variable, or 'a' (rdf:type shorthand)
        predicate_a = A.copy()
        predicate_a.setParseAction(lambda: IRI("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"))
        predicate = iri | variable | predicate_a

        # Object: IRI, Literal, Variable, or BlankNode
        obj = iri | literal | variable | blank_node

        # Triple: subject predicate object .
        triple = Group(subject + predicate + obj)
        triple.setParseAction(self._make_triple_pattern)

        # Triple block: one or more triples separated by '.'
        triple_block = delimitedList(triple, delim=".")

        # Basic Graph Pattern (BGP)
        bgp = Group(triple_block)
        bgp.setParseAction(self._make_bgp)

        # ====================================================================
        # Filter Expressions
        # ====================================================================

        # Comparison operators
        EQ = Literal("=")
        NEQ = Literal("!=")
        LT = Literal("<")
        LTE = Literal("<=")
        GT = Literal(">")
        GTE = Literal(">=")

        comparison_op = EQ | NEQ | LTE | GTE | LT | GT

        # Expression operands
        expr_operand = variable | literal | pyparsing_common.number()

        # Comparison expression: ?x = 10, ?name != ""
        comparison = Group(expr_operand + comparison_op + expr_operand)
        comparison.setParseAction(self._make_comparison)

        # Boolean operators
        AND = CaselessKeyword("&&") | CaselessKeyword("AND")
        OR = CaselessKeyword("||") | CaselessKeyword("OR")
        NOT = CaselessKeyword("!") | CaselessKeyword("NOT")

        # Function call: BOUND(?x), REGEX(?name, "pattern")
        function_name = Word(alphas, alphanums + "_")
        function_args = LPAREN + Optional(delimitedList(expr_operand)) + RPAREN
        function_call = Group(function_name + function_args)
        function_call.setParseAction(self._make_function_call)

        # Expression (simplified - full SPARQL expressions are complex)
        expression = comparison | function_call

        # FILTER clause
        filter_expr = FILTER + LPAREN + expression + RPAREN
        filter_expr.setParseAction(self._make_filter)

        # ====================================================================
        # Query Clauses
        # ====================================================================

        # SELECT clause: SELECT * or SELECT DISTINCT ?var1 ?var2
        select_vars = Literal("*") | OneOrMore(variable)
        select_clause = SELECT + Optional(DISTINCT) + Group(select_vars)
        select_clause.setParseAction(self._make_select_clause)

        # WHERE clause: WHERE { triples filters }
        where_body = bgp + ZeroOrMore(filter_expr)
        where_clause = WHERE + LBRACE + Group(where_body) + RBRACE
        where_clause.setParseAction(self._make_where_clause)

        # Solution modifiers
        order_condition = Optional(ASC | DESC) + variable
        order_by = ORDER + BY + OneOrMore(Group(order_condition))

        limit_clause = LIMIT + pyparsing_common.integer()
        offset_clause = OFFSET + pyparsing_common.integer()

        solution_modifier = Optional(order_by) + Optional(limit_clause) + Optional(offset_clause)
        solution_modifier.setParseAction(self._make_solution_modifier)

        # ====================================================================
        # Top-Level Query
        # ====================================================================

        # SELECT query
        select_query = select_clause + where_clause + Optional(solution_modifier)
        select_query.setParseAction(self._make_sparql_query)

        # Store top-level grammar
        self.query = select_query

    # ========================================================================
    # Parse Actions (Convert tokens to AST nodes)
    # ========================================================================

    def _make_variable(self, tokens):
        """Create Variable from token."""
        return Variable(name=tokens[0])

    def _make_iri(self, tokens):
        """Create IRI from <...> syntax."""
        return IRI(value=tokens[0])

    def _make_prefixed_iri(self, tokens):
        """Create IRI from prefix:name syntax."""
        # Expand prefix to full IRI
        # For now, use simple namespace mapping
        prefix = tokens[0]
        local_name = tokens[1]

        # Common prefixes
        namespaces = {
            'rdf': 'http://www.w3.org/1999/02/22-rdf-syntax-ns#',
            'rdfs': 'http://www.w3.org/2000/01/rdf-schema#',
            'owl': 'http://www.w3.org/2002/07/owl#',
            'xsd': 'http://www.w3.org/2001/XMLSchema#',
            'foaf': 'http://xmlns.com/foaf/0.1/',
            'dc': 'http://purl.org/dc/elements/1.1/',
        }

        if prefix in namespaces:
            full_iri = namespaces[prefix] + local_name
        else:
            # Unknown prefix - use as-is
            full_iri = f"{prefix}:{local_name}"

        return IRI(value=full_iri)

    def _make_literal(self, tokens):
        """Create Literal from string, optionally with language tag or datatype."""
        value = tokens[0]
        language = tokens[1] if len(tokens) > 1 and isinstance(tokens[1], str) and not tokens[1].startswith('http') else None
        datatype = None

        # Check for datatype IRI
        for token in tokens[1:]:
            if isinstance(token, IRI):
                datatype = token.value
                break

        return RDFLiteral(value=value, language=language, datatype=datatype)

    def _make_blank_node(self, tokens):
        """Create BlankNode from _:label."""
        return BlankNode(id=tokens[0])

    def _make_triple_pattern(self, tokens):
        """Create TriplePattern from subject predicate object."""
        triple = tokens[0]
        return TriplePattern(
            subject=triple[0],
            predicate=triple[1],
            object=triple[2]
        )

    def _make_bgp(self, tokens):
        """Create BasicGraphPattern from list of triple patterns."""
        triples = list(tokens[0])
        return BasicGraphPattern(triples=triples)

    def _make_comparison(self, tokens):
        """Create Comparison expression."""
        expr = tokens[0]
        left = expr[0]
        op_str = expr[1]
        right = expr[2]

        # Convert operands to expressions
        if isinstance(left, Variable):
            left = VariableExpr(name=left.name)
        elif not isinstance(left, Expression):
            left = LiteralExpr(value=left)

        if isinstance(right, Variable):
            right = VariableExpr(name=right.name)
        elif not isinstance(right, Expression):
            right = LiteralExpr(value=right)

        # Map operator string to enum
        op_map = {
            '=': ComparisonOp.EQ,
            '!=': ComparisonOp.NEQ,
            '<': ComparisonOp.LT,
            '<=': ComparisonOp.LTE,
            '>': ComparisonOp.GT,
            '>=': ComparisonOp.GTE,
        }
        op = op_map.get(op_str, ComparisonOp.EQ)

        return Comparison(left=left, op=op, right=right)

    def _make_function_call(self, tokens):
        """Create FunctionCall expression."""
        func = tokens[0]
        name = func[0]
        args = [arg if isinstance(arg, Expression) else LiteralExpr(value=arg) for arg in func[1:]]
        return FunctionCall(name=name, args=args)

    def _make_filter(self, tokens):
        """Create Filter from FILTER expression."""
        # tokens[0] is the expression (Comparison or FunctionCall)
        return Filter(expression=tokens[0])

    def _make_select_clause(self, tokens):
        """Create SelectClause from SELECT variables."""
        has_distinct = any(str(t).upper() == 'DISTINCT' for t in tokens)

        # Extract variables
        variables = []
        for token in tokens:
            if isinstance(token, Variable):
                variables.append(token.name)
            elif hasattr(token, '__iter__') and not isinstance(token, str):
                for item in token:
                    if isinstance(item, Variable):
                        variables.append(item.name)
                    elif item == '*':
                        variables = []  # SELECT * means all variables
                        break

        return SelectClause(variables=variables, distinct=has_distinct)

    def _make_where_clause(self, tokens):
        """Create WhereClause from WHERE body."""
        body = tokens[0]

        # Extract BGP (first element)
        bgp = None
        filters = []

        for item in body:
            if isinstance(item, BasicGraphPattern):
                bgp = item
            elif isinstance(item, Filter):
                filters.append(item)

        if bgp is None:
            bgp = BasicGraphPattern(triples=[])

        return WhereClause(bgp=bgp, filters=filters)

    def _make_solution_modifier(self, tokens):
        """Create SolutionModifier from ORDER BY, LIMIT, OFFSET."""
        order_by = []
        limit = None
        offset = None

        for token in tokens:
            if isinstance(token, int):
                # This is either LIMIT or OFFSET value
                # Need to determine which based on context
                # For now, assume first int is LIMIT, second is OFFSET
                if limit is None:
                    limit = token
                else:
                    offset = token

        return SolutionModifier(order_by=order_by, limit=limit, offset=offset)

    def _make_sparql_query(self, tokens):
        """Create SPARQLQuery from SELECT, WHERE, and modifiers."""
        select_clause = None
        where_clause = None
        modifiers = None

        for token in tokens:
            if isinstance(token, SelectClause):
                select_clause = token
            elif isinstance(token, WhereClause):
                where_clause = token
            elif isinstance(token, SolutionModifier):
                modifiers = token

        return SPARQLQuery(
            select_clause=select_clause,
            where_clause=where_clause,
            modifiers=modifiers
        )

    # ========================================================================
    # Public API
    # ========================================================================

    def parse(self, query_string: str) -> SPARQLQuery:
        """
        Parse SPARQL query string into AST.

        Args:
            query_string: SPARQL query text

        Returns:
            SPARQLQuery AST node

        Raises:
            ParseException: If query syntax is invalid

        Example:
            >>> parser = SPARQLParser()
            >>> query = '''
            ... SELECT ?person ?name
            ... WHERE {
            ...     ?person rdf:type foaf:Person .
            ...     ?person foaf:name ?name .
            ...     FILTER (?name != "")
            ... }
            ... LIMIT 10
            ... '''
            >>> ast = parser.parse(query)
            >>> print(ast)
        """
        try:
            result = self.query.parseString(query_string, parseAll=True)
            return result[0]
        except ParseException as e:
            raise ParseException(f"SPARQL parse error at line {e.lineno}, col {e.col}: {e.msg}")


# ============================================================================
# Convenience Functions
# ============================================================================

def parse_sparql(query_string: str) -> SPARQLQuery:
    """
    Convenience function to parse SPARQL query.

    Args:
        query_string: SPARQL query text

    Returns:
        SPARQLQuery AST node

    Example:
        >>> from sabot._cython.graph.compiler import parse_sparql
        >>> ast = parse_sparql("SELECT * WHERE { ?s ?p ?o . } LIMIT 10")
        >>> print(ast.select_clause.variables)
    """
    parser = SPARQLParser()
    return parser.parse(query_string)
