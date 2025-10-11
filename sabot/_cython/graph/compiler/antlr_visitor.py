"""
ANTLR Visitor for Cypher Queries

Converts ANTLR4 parse trees (from antlr4-cypher package) to Sabot AST nodes.

This visitor traverses the ANTLR parse tree and constructs the corresponding
Sabot AST (defined in cypher_ast.py), which can then be translated to Arrow
operations by CypherTranslator.
"""

from typing import List, Optional, Any
from antlr4 import ParserRuleContext
from antlr4_cypher import CypherParser

from .cypher_ast import (
    # Expressions
    Expression, Literal, Variable, PropertyAccess,
    Comparison, BinaryOp, FunctionCall,
    ComparisonOp, BinaryOpType,
    # Patterns
    NodePattern, RelPattern, PatternElement, Pattern,
    ArrowDirection, RecursiveInfo,
    # Clauses
    MatchClause, WhereClause, ProjectionItem, OrderBy,
    ReturnClause, WithClause,
    # Query
    CypherQuery
)


class SabotCypherVisitor:
    """
    Visitor for converting ANTLR parse tree to Sabot AST.

    Usage:
        from antlr4 import InputStream, CommonTokenStream
        from antlr4_cypher import CypherLexer, CypherParser

        input_stream = InputStream(query_string)
        lexer = CypherLexer(input_stream)
        tokens = CommonTokenStream(lexer)
        parser = CypherParser(tokens)
        tree = parser.query()

        visitor = SabotCypherVisitor()
        ast = visitor.visit(tree)
    """

    def visit(self, ctx):
        """Dispatch to appropriate visit method based on context type."""
        if ctx is None:
            return None

        # Get context type name (e.g., "QueryContext" -> "query")
        ctx_type = type(ctx).__name__
        if ctx_type.endswith('Context'):
            ctx_type = ctx_type[:-7]  # Remove "Context" suffix

        # Convert camelCase to snake_case
        method_name = 'visit_' + self._camel_to_snake(ctx_type)

        # Call visit method if it exists, otherwise use default
        method = getattr(self, method_name, self.visitChildren)
        return method(ctx)

    def visitChildren(self, ctx):
        """Default: visit all children and return first non-None result."""
        if ctx is None:
            return None

        # Check if this is a terminal node (has no children attribute)
        if not hasattr(ctx, 'children') or ctx.children is None:
            return None

        results = []
        for child in ctx.children:
            if isinstance(child, ParserRuleContext):
                result = self.visit(child)
                if result is not None:
                    results.append(result)

        # Return single result if only one, otherwise list
        if len(results) == 0:
            return None
        elif len(results) == 1:
            return results[0]
        else:
            return results

    def _camel_to_snake(self, name: str) -> str:
        """Convert camelCase to snake_case."""
        result = []
        for i, char in enumerate(name):
            if char.isupper() and i > 0:
                result.append('_')
            result.append(char.lower())
        return ''.join(result)

    # ========================================================================
    # Top-level Query
    # ========================================================================

    def visit_query(self, ctx) -> CypherQuery:
        """Visit query (top-level entry point)."""
        # Query contains regularQuery
        return self.visit(ctx.regularQuery())

    def visit_regular_query(self, ctx) -> CypherQuery:
        """Visit regularQuery (contains singleQuery)."""
        # For now, handle single query (not UNION)
        return self.visit(ctx.singleQuery())

    def visit_single_query(self, ctx) -> CypherQuery:
        """
        Visit singleQuery.

        Two cases:
        1. singlePartQ: Simple query (MATCH ... RETURN)
        2. multiPartQ: Multi-stage query (MATCH ... WITH ... RETURN)
        """
        match_clauses = []
        with_clauses = []
        where_clause = None
        return_clause = None

        # Check which type of query
        if ctx.singlePartQ():
            # Simple query: MATCH ... RETURN
            result = self.visit_single_part_q(ctx.singlePartQ())
            match_clauses = result.get('match_clauses', [])
            return_clause = result.get('return_clause')
        elif ctx.multiPartQ():
            # Multi-stage query: MATCH ... WITH ... RETURN
            result = self.visit_multi_part_q(ctx.multiPartQ())
            match_clauses = result.get('match_clauses', [])
            with_clauses = result.get('with_clauses', [])
            return_clause = result.get('return_clause')

        return CypherQuery(
            match_clauses=match_clauses,
            with_clauses=with_clauses,
            where_clause=where_clause,
            return_clause=return_clause
        )

    def visit_single_part_q(self, ctx) -> dict:
        """
        Visit singlePartQ (simple query without WITH).

        Contains: readingStatement* returnSt
        """
        match_clauses = []
        return_clause = None

        for child in ctx.children:
            if child is None:
                continue

            result = self.visit(child)

            if isinstance(result, MatchClause):
                match_clauses.append(result)
            elif isinstance(result, ReturnClause):
                return_clause = result

        return {
            'match_clauses': match_clauses,
            'return_clause': return_clause
        }

    def visit_multi_part_q(self, ctx) -> dict:
        """
        Visit multiPartQ (multi-stage query with WITH).

        Contains: readingStatement* withSt+ singlePartQ
        """
        match_clauses = []
        with_clauses = []
        return_clause = None

        for child in ctx.children:
            if child is None:
                continue

            result = self.visit(child)

            if isinstance(result, MatchClause):
                match_clauses.append(result)
            elif isinstance(result, WithClause):
                with_clauses.append(result)
            elif isinstance(result, dict) and 'return_clause' in result:
                # This is from nested singlePartQ (final RETURN)
                return_clause = result['return_clause']
                # May also have more MATCH clauses before RETURN
                if result.get('match_clauses'):
                    match_clauses.extend(result['match_clauses'])

        return {
            'match_clauses': match_clauses,
            'with_clauses': with_clauses,
            'return_clause': return_clause
        }

    # ========================================================================
    # Clauses
    # ========================================================================

    def visit_reading_statement(self, ctx) -> MatchClause:
        """Visit readingStatement (contains matchSt)."""
        # ReadingStatement wraps MatchSt
        return self.visitChildren(ctx)

    def visit_match_st(self, ctx) -> MatchClause:
        """Visit matchSt (MATCH clause)."""
        # Get pattern from patternWhere
        pattern = None
        where_expr = None

        if ctx.patternWhere():
            pw_ctx = ctx.patternWhere()
            # PatternWhere contains Pattern as first child
            if pw_ctx.getChildCount() > 0:
                pattern_ctx = pw_ctx.getChild(0)
                pattern = self.visit(pattern_ctx)

        return MatchClause(pattern=pattern or Pattern(elements=[]), where=where_expr)

    def visit_with_st(self, ctx) -> WithClause:
        """Visit withSt (WITH clause)."""
        # Get projection body
        projection = self.visit(ctx.projectionBody())

        # Get WHERE if present
        where_expr = None
        if ctx.where():
            where_expr = self.visit(ctx.where())

        # Extract components from projection dict
        return WithClause(
            items=projection.get('items', []),
            distinct=projection.get('distinct', False),
            where=where_expr,
            order_by=projection.get('order_by', []),
            skip=projection.get('skip'),
            limit=projection.get('limit')
        )

    def visit_return_st(self, ctx) -> ReturnClause:
        """Visit returnSt (RETURN clause)."""
        # Get projection body
        projection = self.visit(ctx.projectionBody())

        return ReturnClause(
            items=projection.get('items', []),
            distinct=projection.get('distinct', False),
            order_by=projection.get('order_by', []),
            skip=projection.get('skip'),
            limit=projection.get('limit')
        )

    def visit_projection_body(self, ctx) -> dict:
        """
        Visit projectionBody (RETURN/WITH items + ORDER BY + SKIP + LIMIT).

        Returns dict with:
            items: List[ProjectionItem]
            distinct: bool
            order_by: List[OrderBy]
            skip: Optional[int]
            limit: Optional[int]
        """
        # Get DISTINCT flag
        distinct = ctx.DISTINCT() is not None

        # Get projection items
        items = []
        if ctx.projectionItems():
            items = self.visit(ctx.projectionItems())

        # Get ORDER BY
        order_by = []
        if ctx.orderSt():
            order_by = self.visit(ctx.orderSt())

        # Get SKIP
        skip = None
        if ctx.skipSt():
            skip = self.visit(ctx.skipSt())

        # Get LIMIT
        limit = None
        if ctx.limitSt():
            limit = self.visit(ctx.limitSt())

        return {
            'items': items,
            'distinct': distinct,
            'order_by': order_by,
            'skip': skip,
            'limit': limit
        }

    def visit_projection_items(self, ctx) -> List[ProjectionItem]:
        """Visit projectionItems (list of projection items)."""
        items = []

        # Check for MULT (*) - return all variables
        if ctx.MULT():
            items.append(ProjectionItem(expression=Variable('*'), alias=None))

        # Get individual projection items
        for item_ctx in ctx.projectionItem():
            item = self.visit(item_ctx)
            if item:
                items.append(item)

        return items

    def visit_projection_item(self, ctx) -> ProjectionItem:
        """Visit projectionItem (single item in RETURN/WITH)."""
        # Get expression
        expr = self.visit(ctx.expression())

        # Get alias (AS ...)
        alias = None
        if ctx.variable():
            alias = self.visit(ctx.variable())

        return ProjectionItem(expression=expr, alias=alias)

    def visit_where(self, ctx) -> Expression:
        """Visit where (WHERE clause)."""
        return self.visit(ctx.expression())

    def visit_order_st(self, ctx) -> List[OrderBy]:
        """Visit orderSt (ORDER BY clause)."""
        order_items = []

        for item_ctx in ctx.orderItem():
            expr = self.visit(item_ctx.expression())

            # Check for DESC
            ascending = item_ctx.DESC() is None

            order_items.append(OrderBy(expression=expr, ascending=ascending))

        return order_items

    def visit_skip_st(self, ctx) -> int:
        """Visit skipSt (SKIP clause)."""
        return int(ctx.intLit().getText())

    def visit_limit_st(self, ctx) -> int:
        """Visit limitSt (LIMIT clause)."""
        return int(ctx.intLit().getText())

    # ========================================================================
    # Patterns
    # ========================================================================

    def visit_pattern(self, ctx) -> Pattern:
        """Visit pattern."""
        # Pattern contains patternPart children
        elements = []

        for i in range(ctx.getChildCount()):
            child = ctx.getChild(i)
            if type(child).__name__ == 'PatternPartContext':
                element = self.visit(child)
                if element:
                    elements.append(element)

        return Pattern(elements=elements)

    def visit_pattern_part(self, ctx) -> PatternElement:
        """Visit patternPart."""
        # PatternPart contains patternElem
        return self.visitChildren(ctx)

    def visit_pattern_elem(self, ctx) -> PatternElement:
        """Visit patternElem (node-edge-node chain)."""
        nodes = []
        edges = []

        # First node
        if ctx.nodePattern():
            nodes.append(self.visit(ctx.nodePattern()))

        # Pattern element chains (edges and nodes)
        if ctx.patternElemChain():
            for chain_ctx in ctx.patternElemChain():
                edge, node = self.visit(chain_ctx)
                edges.append(edge)
                nodes.append(node)

        return PatternElement(nodes=nodes, edges=edges)

    def visit_pattern_elem_chain(self, ctx) -> tuple:
        """Visit patternElemChain (edge + node pair)."""
        edge = self.visit(ctx.relationshipPattern())
        node = self.visit(ctx.nodePattern())
        return (edge, node)

    def visit_node_pattern(self, ctx) -> NodePattern:
        """Visit nodePattern."""
        variable = None
        labels = []
        properties = {}

        # Get variable (symbol)
        if ctx.symbol():
            symbol_ctx = ctx.symbol()
            variable = symbol_ctx.getText()

        # Get labels
        if ctx.nodeLabels():
            labels = self.visit(ctx.nodeLabels())

        # Get properties (TODO: implement if needed)

        return NodePattern(variable=variable, labels=labels, properties=properties)

    def visit_node_labels(self, ctx) -> List[str]:
        """Visit nodeLabels."""
        labels = []

        # NodeLabels contains nodeLabel children
        for i in range(ctx.getChildCount()):
            child = ctx.getChild(i)
            if type(child).__name__ == 'NodeLabelContext':
                label_ctx = child
                # NodeLabel contains symbol
                if label_ctx.symbol():
                    labels.append(label_ctx.symbol().getText())

        return labels

    def visit_relationship_pattern(self, ctx) -> RelPattern:
        """Visit relationshipPattern."""
        # Determine direction from children (terminal nodes '<' and '>')
        has_left_arrow = False
        has_right_arrow = False

        for i in range(ctx.getChildCount()):
            child = ctx.getChild(i)
            text = child.getText() if hasattr(child, 'getText') else ''
            if text == '<':
                has_left_arrow = True
            elif text == '>':
                has_right_arrow = True

        if has_left_arrow and has_right_arrow:
            direction = ArrowDirection.BOTH
        elif has_left_arrow:
            direction = ArrowDirection.LEFT
        else:
            direction = ArrowDirection.RIGHT

        # Get relationship detail
        variable = None
        types = []
        properties = {}
        recursive = None

        if ctx.relationDetail():
            detail_ctx = ctx.relationDetail()

            # Get variable (symbol)
            if detail_ctx.symbol():
                variable = detail_ctx.symbol().getText()

            # TODO: handle relationship types and properties

        return RelPattern(
            variable=variable,
            types=types,
            direction=direction,
            properties=properties,
            recursive=recursive
        )

    # ========================================================================
    # Expressions
    # ========================================================================

    def visit_expression(self, ctx) -> Expression:
        """Visit expression (top-level expression)."""
        # Expression grammar varies - just use visitChildren
        return self.visitChildren(ctx)

    def visit_expression12(self, ctx) -> Expression:
        """Visit expression12 (OR)."""
        if len(ctx.expression11()) == 1:
            return self.visit(ctx.expression11(0))

        # Multiple expressions with OR
        left = self.visit(ctx.expression11(0))
        for i in range(1, len(ctx.expression11())):
            right = self.visit(ctx.expression11(i))
            left = BinaryOp(left=left, op=BinaryOpType.OR, right=right)
        return left

    def visit_expression11(self, ctx) -> Expression:
        """Visit expression11 (XOR)."""
        if len(ctx.expression10()) == 1:
            return self.visit(ctx.expression10(0))

        # Multiple expressions with XOR
        left = self.visit(ctx.expression10(0))
        for i in range(1, len(ctx.expression10())):
            right = self.visit(ctx.expression10(i))
            left = BinaryOp(left=left, op=BinaryOpType.XOR, right=right)
        return left

    def visit_expression10(self, ctx) -> Expression:
        """Visit expression10 (AND)."""
        if len(ctx.expression9()) == 1:
            return self.visit(ctx.expression9(0))

        # Multiple expressions with AND
        left = self.visit(ctx.expression9(0))
        for i in range(1, len(ctx.expression9())):
            right = self.visit(ctx.expression9(i))
            left = BinaryOp(left=left, op=BinaryOpType.AND, right=right)
        return left

    def visit_expression9(self, ctx) -> Expression:
        """Visit expression9 (NOT)."""
        # For now, pass through to next level
        return self.visitChildren(ctx)

    def visit_expression8(self, ctx) -> Expression:
        """Visit expression8 (comparison)."""
        if len(ctx.expression7()) == 1:
            return self.visit(ctx.expression7(0))

        # Comparison expression
        left = self.visit(ctx.expression7(0))
        op = self.visit(ctx.comparisonSigns())
        right = self.visit(ctx.expression7(1))

        return Comparison(left=left, op=op, right=right)

    def visit_comparison_signs(self, ctx) -> ComparisonOp:
        """Visit comparisonSigns."""
        text = ctx.getText()

        if text == '=':
            return ComparisonOp.EQ
        elif text == '<>':
            return ComparisonOp.NEQ
        elif text == '<':
            return ComparisonOp.LT
        elif text == '<=':
            return ComparisonOp.LTE
        elif text == '>':
            return ComparisonOp.GT
        elif text == '>=':
            return ComparisonOp.GTE
        else:
            return ComparisonOp.EQ  # Default

    def visit_expression7(self, ctx) -> Expression:
        """Visit expression7 (addition/subtraction)."""
        # For now, pass through to next level
        return self.visitChildren(ctx)

    def visit_expression6(self, ctx) -> Expression:
        """Visit expression6 (multiplication/division)."""
        # For now, pass through to next level
        return self.visitChildren(ctx)

    def visit_expression5(self, ctx) -> Expression:
        """Visit expression5 (power)."""
        # For now, pass through to next level
        return self.visitChildren(ctx)

    def visit_expression4(self, ctx) -> Expression:
        """Visit expression4 (unary +/-)."""
        # For now, pass through to next level
        return self.visitChildren(ctx)

    def visit_expression3a(self, ctx) -> Expression:
        """Visit expression3a (string operators)."""
        # For now, pass through to next level
        return self.visitChildren(ctx)

    def visit_expression3b(self, ctx) -> Expression:
        """Visit expression3b (list operators)."""
        # For now, pass through to next level
        return self.visitChildren(ctx)

    def visit_expression2a(self, ctx) -> Expression:
        """Visit expression2a (IS NULL, etc.)."""
        # For now, pass through to next level
        return self.visitChildren(ctx)

    def visit_expression2b(self, ctx) -> Expression:
        """Visit expression2b."""
        # For now, pass through to next level
        return self.visitChildren(ctx)

    def visit_expression1(self, ctx) -> Expression:
        """Visit expression1 (property lookup, labels)."""
        # Start with atomic expression
        expr = self.visit(ctx.expression1b())

        # Handle property lookup chains
        if ctx.propertyLookup():
            for prop_ctx in ctx.propertyLookup():
                prop_name = prop_ctx.propertyKeyName().getText()

                # Extract variable name from current expression
                var_name = None
                if isinstance(expr, Variable):
                    var_name = expr.name
                elif isinstance(expr, PropertyAccess):
                    var_name = expr.variable
                else:
                    var_name = str(expr)

                expr = PropertyAccess(variable=var_name, property_name=prop_name)

        return expr

    def visit_expression1b(self, ctx) -> Expression:
        """Visit expression1b."""
        return self.visitChildren(ctx)

    def visit_atom(self, ctx) -> Expression:
        """Visit atom (literal, variable, function call, etc.)."""
        return self.visitChildren(ctx)

    def visit_literal(self, ctx) -> Literal:
        """Visit literal."""
        # Try different literal types
        if ctx.numberLit():
            return self.visit(ctx.numberLit())
        elif ctx.stringLit():
            return self.visit(ctx.stringLit())
        elif ctx.boolLit():
            return self.visit(ctx.boolLit())
        elif ctx.NULL():
            return Literal(value=None)
        else:
            return self.visitChildren(ctx)

    def visit_number_lit(self, ctx) -> Literal:
        """Visit numberLit."""
        text = ctx.getText()

        # Try int first, then float
        try:
            value = int(text)
        except ValueError:
            value = float(text)

        return Literal(value=value)

    def visit_string_lit(self, ctx) -> Literal:
        """Visit stringLit."""
        text = ctx.getText()
        # Remove quotes
        value = text[1:-1]
        return Literal(value=value)

    def visit_bool_lit(self, ctx) -> Literal:
        """Visit boolLit."""
        text = ctx.getText().upper()
        value = text == 'TRUE'
        return Literal(value=value)

    def visit_variable(self, ctx) -> str:
        """Visit variable (returns string, not Variable node)."""
        return ctx.symbolicName().getText()

    def visit_function_invocation(self, ctx) -> FunctionCall:
        """Visit functionInvocation."""
        # Get function name
        func_name = ctx.functionName().getText().lower()

        # Get DISTINCT flag
        distinct = ctx.DISTINCT() is not None

        # Get arguments
        args = []
        if ctx.expression():
            for expr_ctx in ctx.expression():
                arg = self.visit(expr_ctx)
                args.append(arg)

        return FunctionCall(name=func_name, args=args, distinct=distinct)

    def visit_count_all(self, ctx) -> FunctionCall:
        """Visit countAll (COUNT(*))."""
        return FunctionCall(name='count', args=[Variable('*')], distinct=False)


def parse_cypher(query_string: str) -> CypherQuery:
    """
    Parse a Cypher query string into Sabot AST.

    Args:
        query_string: Cypher query string

    Returns:
        CypherQuery AST

    Example:
        >>> ast = parse_cypher("MATCH (a)-[r]->(b) RETURN a.name")
        >>> print(ast)
        CypherQuery(MATCH (1 patterns), RETURN (1 items))
    """
    from antlr4 import InputStream, CommonTokenStream
    from antlr4_cypher import CypherLexer, CypherParser

    input_stream = InputStream(query_string)
    lexer = CypherLexer(input_stream)
    tokens = CommonTokenStream(lexer)
    parser = CypherParser(tokens)
    tree = parser.query()

    visitor = SabotCypherVisitor()
    ast = visitor.visit(tree)

    return ast
