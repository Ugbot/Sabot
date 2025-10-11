"""
Lark → Sabot AST Transformer

Converts Lark parse trees from openCypher grammar into Sabot AST nodes.
Uses Lark's visitor pattern to traverse bottom-up.
"""

from lark import Transformer, Token
from typing import List, Optional, Any

from .cypher_ast import (
    # Top-level
    CypherQuery,

    # Clauses
    MatchClause,
    WithClause,
    WhereClause,
    ReturnClause,
    ProjectionItem,
    OrderBy,

    # Patterns
    Pattern,
    PatternElement,
    NodePattern,
    RelPattern,
    RecursiveInfo,
    ArrowDirection,

    # Expressions
    Expression,
    Variable,
    Literal,
    PropertyAccess,
    Comparison,
    BinaryOp,
    FunctionCall,
    ComparisonOp,
    BinaryOpType,
)


class LarkToSabotTransformer(Transformer):
    """Transform Lark parse tree to Sabot AST."""

    # ========================================================================
    # Top-level Query Structure
    # ========================================================================

    def start(self, items):
        """Entry point: start → cypher"""
        return items[0]

    def cypher(self, items):
        """cypher → statement"""
        return items[0]

    def statement(self, items):
        """statement → query"""
        return items[0]

    def query(self, items):
        """query → regular_query | standalone_call"""
        return items[0]

    def regular_query(self, items):
        """regular_query → single_query union*"""
        # For now, ignore UNION (just return first query)
        return items[0]

    def single_query(self, items):
        """single_query → single_part_query | multi_part_query"""
        return items[0]

    def single_part_query(self, items):
        """single_part_query → reading_clause* return_clause"""
        reading_clauses = [i for i in items if isinstance(i, MatchClause)]
        return_clause = next((i for i in items if isinstance(i, ReturnClause)), None)

        return CypherQuery(
            match_clauses=reading_clauses,
            return_clause=return_clause
        )

    def multi_part_query(self, items):
        """multi_part_query → (reading_clause* with_clause)+ single_part_query"""
        # Collect all reading clauses (MATCH)
        match_clauses = []
        with_clauses = []

        for item in items:
            if isinstance(item, MatchClause):
                match_clauses.append(item)
            elif isinstance(item, WithClause):
                with_clauses.append(item)
            elif isinstance(item, CypherQuery):
                # Final single_part_query
                match_clauses.extend(item.match_clauses)
                return_clause = item.return_clause

        return CypherQuery(
            match_clauses=match_clauses,
            with_clauses=with_clauses,
            return_clause=return_clause
        )

    # ========================================================================
    # Clauses
    # ========================================================================

    def reading_clause(self, items):
        """reading_clause → match_clause | unwind_clause | in_query_call"""
        return items[0]

    def match_clause(self, items):
        """match_clause → (OPTIONAL)? MATCH pattern (where_clause)?"""
        pattern = next((i for i in items if isinstance(i, Pattern)), None)
        where = next((i for i in items if isinstance(i, WhereClause)), None)
        optional = any(isinstance(i, Token) and i.value == "OPTIONAL" for i in items)

        return MatchClause(pattern=pattern, where=where.expression if where else None, optional=optional)

    def with_clause(self, items):
        """with_clause → WITH projection_body (where_clause)?"""
        # projection_body returns dict with 'items', 'distinct', 'order_by', 'skip', 'limit'
        proj_body = next((i for i in items if isinstance(i, dict)), None)
        where = next((i for i in items if isinstance(i, WhereClause)), None)

        return WithClause(
            items=proj_body.get('items', []),
            distinct=proj_body.get('distinct', False),
            where=where.expression if where else None,
            order_by=proj_body.get('order_by', []),
            skip=proj_body.get('skip'),
            limit=proj_body.get('limit')
        )

    def return_clause(self, items):
        """return_clause → RETURN projection_body"""
        proj_body = items[0] if items else {}

        return ReturnClause(
            items=proj_body.get('items', []),
            distinct=proj_body.get('distinct', False),
            order_by=proj_body.get('order_by', []),
            skip=proj_body.get('skip'),
            limit=proj_body.get('limit')
        )

    def projection_body(self, items):
        """projection_body → (DISTINCT)? projection_items (order_clause)? (skip_clause)? (limit_clause)?"""
        result = {
            'items': [],
            'distinct': False,
            'order_by': [],
            'skip': None,
            'limit': None
        }

        for item in items:
            if isinstance(item, list) and all(isinstance(i, ProjectionItem) for i in item):
                result['items'] = item
            elif isinstance(item, Token) and item.value == "DISTINCT":
                result['distinct'] = True
            elif isinstance(item, list) and all(isinstance(i, OrderBy) for i in item):
                result['order_by'] = item
            elif isinstance(item, dict):
                if 'skip' in item:
                    result['skip'] = item['skip']
                if 'limit' in item:
                    result['limit'] = item['limit']

        return result

    def projection_items(self, items):
        """projection_items → projection_item (,projection_item)*"""
        return [i for i in items if isinstance(i, ProjectionItem)]

    def projection_item(self, items):
        """projection_item → expression AS variable | expression"""
        if len(items) == 2:  # expression AS variable
            return ProjectionItem(expression=items[0], alias=items[1].name)
        else:  # expression only
            return ProjectionItem(expression=items[0])

    def order_clause(self, items):
        """order_clause → ORDER BY sort_item (, sort_item)*"""
        return [i for i in items if isinstance(i, OrderBy)]

    def sort_item(self, items):
        """sort_item → expression (ASCENDING|ASC|DESCENDING|DESC)?"""
        expr = items[0]
        ascending = True
        if len(items) > 1:
            direction = items[1].value.upper() if isinstance(items[1], Token) else None
            if direction in ('DESCENDING', 'DESC'):
                ascending = False
        return OrderBy(expression=expr, ascending=ascending)

    def skip_clause(self, items):
        """skip_clause → SKIP expression"""
        expr = items[0]
        # Extract integer value from Literal expression
        value = expr.value if isinstance(expr, Literal) else expr
        return {'skip': value}

    def limit_clause(self, items):
        """limit_clause → LIMIT expression"""
        expr = items[0]
        # Extract integer value from Literal expression
        value = expr.value if isinstance(expr, Literal) else expr
        return {'limit': value}

    def where_clause(self, items):
        """where_clause → WHERE expression"""
        return WhereClause(expression=items[0])

    # ========================================================================
    # Patterns
    # ========================================================================

    def pattern(self, items):
        """pattern → pattern_part (, pattern_part)*"""
        elements = []
        for item in items:
            if isinstance(item, PatternElement):
                elements.append(item)
        return Pattern(elements=elements)

    def pattern_part(self, items):
        """pattern_part → (variable = anonymous_pattern_part) | anonymous_pattern_part"""
        # For now, ignore variable binding
        return next((i for i in items if isinstance(i, PatternElement)), None)

    def anonymous_pattern_part(self, items):
        """anonymous_pattern_part → pattern_element"""
        return items[0]

    def pattern_element(self, items):
        """pattern_element → node_pattern (pattern_element_chain)*"""
        nodes = []
        edges = []

        for item in items:
            if isinstance(item, NodePattern):
                nodes.append(item)
            elif isinstance(item, dict) and 'edge' in item:
                edges.append(item['edge'])
                nodes.append(item['node'])

        return PatternElement(nodes=nodes, edges=edges)

    def pattern_element_chain(self, items):
        """pattern_element_chain → relationship_pattern node_pattern"""
        edge = items[0]
        node = items[1]
        return {'edge': edge, 'node': node}

    def node_pattern(self, items):
        """node_pattern → ( (variable)? (node_labels)? (properties)? )"""
        variable = None
        labels = []
        properties = {}

        for item in items:
            if isinstance(item, Variable):
                variable = item.name
            elif isinstance(item, list):  # labels
                labels = item
            elif isinstance(item, dict):  # properties
                properties = item

        return NodePattern(variable=variable, labels=labels, properties=properties)

    def relationship_pattern(self, items):
        """Various relationship patterns with direction"""
        # Items contain relationship_detail and direction info
        variable = None
        types = []
        properties = {}
        direction = ArrowDirection.RIGHT
        recursive = None

        # Determine direction from rule name
        if hasattr(items, '__self__'):
            rule_name = items.__self__.__name__
            if 'left' in rule_name:
                direction = ArrowDirection.LEFT
            elif 'both' in rule_name:
                direction = ArrowDirection.BOTH

        for item in items:
            if isinstance(item, dict):
                variable = item.get('variable')
                types = item.get('types', [])
                properties = item.get('properties', {})
                recursive = item.get('recursive')

        return RelPattern(
            variable=variable,
            types=types,
            direction=direction,
            properties=properties,
            recursive=recursive
        )

    # Relationship pattern variants
    def rel_both_arrows(self, items):
        return self.relationship_pattern(items)

    def rel_left_arrow(self, items):
        return self.relationship_pattern(items)

    def rel_right_arrow(self, items):
        return self.relationship_pattern(items)

    def rel_no_arrow(self, items):
        return self.relationship_pattern(items)

    def relationship_detail(self, items):
        """relationship_detail → [ (variable)? (relationship_types)? (range_literal)? (properties)? ]"""
        result = {
            'variable': None,
            'types': [],
            'properties': {},
            'recursive': None
        }

        for item in items:
            if isinstance(item, Variable):
                result['variable'] = item.name
            elif isinstance(item, list):  # types
                result['types'] = item
            elif isinstance(item, dict):
                if 'min_hops' in item:  # range_literal
                    result['recursive'] = RecursiveInfo(**item)
                else:  # properties
                    result['properties'] = item

        return result

    def relationship_types(self, items):
        """relationship_types → : rel_type_name (| rel_type_name)*"""
        return [i for i in items if isinstance(i, str)]

    def node_labels(self, items):
        """node_labels → node_label (node_label)*"""
        return [i for i in items if isinstance(i, str)]

    def node_label(self, items):
        """node_label → : label_name"""
        return items[0]

    def range_literal(self, items):
        """range_literal → * (integer_literal)? (.. (integer_literal)?)?"""
        min_hops = 1
        max_hops = None

        if len(items) >= 1 and isinstance(items[0], int):
            min_hops = items[0]
            max_hops = items[0]
        if len(items) >= 2 and isinstance(items[1], int):
            max_hops = items[1]

        return {'min_hops': min_hops, 'max_hops': max_hops}

    # ========================================================================
    # Expressions
    # ========================================================================

    def expression(self, items):
        """expression → or_expression"""
        return items[0]

    def or_expression(self, items):
        """or_expression → xor_expression (OR xor_expression)*"""
        if len(items) == 1:
            return items[0]

        result = items[0]
        for i in range(1, len(items)):
            result = BinaryOp(left=result, op=BinaryOpType.OR, right=items[i])
        return result

    def xor_expression(self, items):
        """xor_expression → and_expression (XOR and_expression)*"""
        if len(items) == 1:
            return items[0]

        result = items[0]
        for i in range(1, len(items)):
            result = BinaryOp(left=result, op=BinaryOpType.XOR, right=items[i])
        return result

    def and_expression(self, items):
        """and_expression → not_expression (AND not_expression)*"""
        if len(items) == 1:
            return items[0]

        result = items[0]
        for i in range(1, len(items)):
            result = BinaryOp(left=result, op=BinaryOpType.AND, right=items[i])
        return result

    def not_expression(self, items):
        """not_expression → (NOT)* comparison_expression"""
        # Count NOT tokens
        not_count = sum(1 for i in items if isinstance(i, Token) and i.value == "NOT")
        expr = next((i for i in items if isinstance(i, Expression)), items[-1])

        # Apply NOT operations
        for _ in range(not_count):
            expr = FunctionCall(name="NOT", args=[expr])

        return expr

    def comparison_expression(self, items):
        """comparison_expression → add_or_subtract_expression (partial_comparison_expression)*"""
        if len(items) == 1:
            return items[0]

        left = items[0]
        for i in range(1, len(items)):
            if isinstance(items[i], dict):
                op = items[i]['op']
                right = items[i]['right']
                left = Comparison(left=left, op=op, right=right)

        return left

    def partial_comparison_expression(self, items):
        """partial_comparison_expression → = expr | <> expr | < expr | > expr | ..."""
        # First item is operator token or string, second is expression
        op_map = {
            '=': ComparisonOp.EQ,
            '<>': ComparisonOp.NEQ,
            '<': ComparisonOp.LT,
            '>': ComparisonOp.GT,
            '<=': ComparisonOp.LTE,
            '>=': ComparisonOp.GTE,
        }

        # Handle various token types defensively
        if isinstance(items[0], str):
            op_str = items[0]
            right = items[1] if len(items) > 1 else items[0]
        elif isinstance(items[0], Token):
            op_str = items[0].value
            right = items[1] if len(items) > 1 else items[0]
        elif isinstance(items[0], Expression):
            # Unexpected but can happen - assume single expression with no operator
            # This means comparison_expression should handle it
            op_str = '='
            right = items[0]
        else:
            op_str = str(items[0])
            right = items[1] if len(items) > 1 else items[0]

        op = op_map.get(op_str, ComparisonOp.EQ)

        return {'op': op, 'right': right}

    def add_or_subtract_expression(self, items):
        """add_or_subtract_expression → multiply_divide_modulo_expression ((+|-) ...)*"""
        if len(items) == 1:
            return items[0]

        result = items[0]
        i = 1
        while i < len(items):
            op_token = items[i]
            if isinstance(op_token, str) or (isinstance(op_token, Token) and op_token.value in ('+', '-')):
                op = BinaryOpType.PLUS if op_token == '+' or (isinstance(op_token, Token) and op_token.value == '+') else BinaryOpType.MINUS
                result = BinaryOp(left=result, op=op, right=items[i+1])
                i += 2
            else:
                i += 1

        return result

    def multiply_divide_modulo_expression(self, items):
        """multiply_divide_modulo_expression → power_of_expression ((*|/|%) ...)*"""
        if len(items) == 1:
            return items[0]

        result = items[0]
        i = 1
        while i < len(items):
            op_token = items[i]
            if isinstance(op_token, str) or (isinstance(op_token, Token) and op_token.value in ('*', '/', '%')):
                op_map = {'*': BinaryOpType.MULTIPLY, '/': BinaryOpType.DIVIDE, '%': BinaryOpType.MODULO}
                op_str = op_token if isinstance(op_token, str) else op_token.value
                op = op_map.get(op_str, BinaryOpType.MULTIPLY)
                result = BinaryOp(left=result, op=op, right=items[i+1])
                i += 2
            else:
                i += 1

        return result

    def power_of_expression(self, items):
        """power_of_expression → unary_add_or_subtract_expression (^ ...)*"""
        if len(items) == 1:
            return items[0]

        # Right-associative
        result = items[-1]
        for i in range(len(items) - 2, -1, -1):
            if not isinstance(items[i], Token):
                result = FunctionCall(name="POW", args=[items[i], result])

        return result

    def unary_add_or_subtract_expression(self, items):
        """unary_add_or_subtract_expression → atom_with_suffix | (+|-) atom_with_suffix"""
        if len(items) == 1:
            return items[0]

        op = items[0]
        expr = items[1]

        if op == '-' or (isinstance(op, Token) and op.value == '-'):
            return BinaryOp(left=Literal(0), op=BinaryOpType.MINUS, right=expr)
        else:
            return expr

    def atom_with_suffix(self, items):
        """atom_with_suffix → atom property_or_list_access* node_labels?"""
        result = items[0]

        for item in items[1:]:
            if isinstance(item, dict) and 'property' in item:
                result = PropertyAccess(variable=result.name if isinstance(result, Variable) else str(result),
                                      property_name=item['property'])
            # Ignore list access and node labels for now

        return result

    def property_or_list_access(self, items):
        """property_or_list_access → property_lookup | list_operator_expression"""
        return items[0]

    def property_lookup(self, items):
        """property_lookup → . (symbolic_name | reserved_word)"""
        prop_name = items[0]
        if isinstance(prop_name, Variable):
            prop_name = prop_name.name
        elif isinstance(prop_name, Token):
            prop_name = prop_name.value

        return {'property': prop_name}

    def atom(self, items):
        """atom → literal | parameter | case_expression | count_star | ... | variable"""
        return items[0]

    def count_star(self, items):
        """count_star → COUNT ( * )"""
        return FunctionCall(name="count", args=[Variable("*")])

    def function_invocation(self, items):
        """function_invocation → function_name ( (DISTINCT)? (expression (, expression)*)? )"""
        func_name = items[0]
        distinct = False
        args = []

        for item in items[1:]:
            if isinstance(item, Token) and item.value == "DISTINCT":
                distinct = True
            elif isinstance(item, Expression):
                args.append(item)

        return FunctionCall(name=func_name, args=args, distinct=distinct)

    def function_name(self, items):
        """function_name → namespace symbolic_name"""
        # Combine namespace + name
        parts = [i for i in items if isinstance(i, str) or isinstance(i, Variable)]
        return '.'.join(str(p) if isinstance(p, str) else p.name for p in parts)

    def namespace(self, items):
        """namespace → (symbolic_name .)*"""
        return '.'.join(str(i) for i in items if not isinstance(i, Token))

    def parenthesized_expression(self, items):
        """parenthesized_expression → ( expression )"""
        return items[0]

    # ========================================================================
    # Variables and Literals
    # ========================================================================

    def variable(self, items):
        """variable → symbolic_name"""
        name = items[0]
        if isinstance(name, Token):
            name = name.value
        elif isinstance(name, Variable):
            name = name.name
        return Variable(name=str(name))

    def symbolic_name(self, items):
        """symbolic_name → unescaped_symbolic_name | escaped_symbolic_name | ..."""
        token = items[0]
        if isinstance(token, Token):
            return token.value.strip('`')  # Remove backticks if escaped
        return str(token)

    def unescaped_symbolic_name(self, items):
        """unescaped_symbolic_name → /[a-zA-Z_][a-zA-Z0-9_]*/"""
        return items[0].value

    def escaped_symbolic_name(self, items):
        """escaped_symbolic_name → /`[^`]+`/"""
        return items[0].value.strip('`')

    def label_name(self, items):
        """label_name → schema_name"""
        return items[0]

    def rel_type_name(self, items):
        """rel_type_name → schema_name"""
        return items[0]

    def schema_name(self, items):
        """schema_name → symbolic_name | reserved_word"""
        if isinstance(items[0], Token):
            return items[0].value
        return items[0]

    def reserved_word(self, items):
        """reserved_word → ALL | ASC | ..."""
        return items[0].value if isinstance(items[0], Token) else str(items[0])

    def literal(self, items):
        """literal → boolean_literal | NULL | number_literal | string_literal | ..."""
        return items[0]

    def boolean_literal(self, items):
        """boolean_literal → TRUE | FALSE | true | false"""
        token = items[0]
        value = token.value if isinstance(token, Token) else str(token)
        return Literal(value=value.upper() == 'TRUE')

    def number_literal(self, items):
        """number_literal → double_literal | integer_literal"""
        return items[0]

    def integer_literal(self, items):
        """integer_literal → hex_integer | octal_integer | decimal_integer"""
        # Check if already transformed
        if isinstance(items[0], Literal):
            return items[0]

        token = items[0]
        value = token.value if isinstance(token, Token) else str(token)

        if value.startswith('0x') or value.startswith('0X'):
            return Literal(value=int(value, 16))
        elif value.startswith('0o') or value.startswith('0O'):
            return Literal(value=int(value, 8))
        else:
            return Literal(value=int(value))

    def decimal_integer(self, items):
        """decimal_integer → /0|[1-9][0-9]*/"""
        return Literal(value=int(items[0].value))

    def hex_integer(self, items):
        """hex_integer → /0[xX][0-9a-fA-F]+/"""
        return Literal(value=int(items[0].value, 16))

    def octal_integer(self, items):
        """octal_integer → /0[oO][0-7]+/"""
        return Literal(value=int(items[0].value, 8))

    def double_literal(self, items):
        """double_literal → /[0-9]+\.[0-9]+([eE][-+]?[0-9]+)?/ | ..."""
        token = items[0]
        value = token.value if isinstance(token, Token) else str(token)
        return Literal(value=float(value))

    def string_literal(self, items):
        """string_literal → /"..."/ | /'...'/"""
        token = items[0]
        value = token.value if isinstance(token, Token) else str(token)
        # Remove quotes and handle escape sequences
        value = value[1:-1]  # Remove outer quotes
        # TODO: Handle escape sequences properly
        return Literal(value=value)

    def map_literal(self, items):
        """map_literal → { property_key_name : expression (, ...)* }"""
        result = {}
        i = 0
        while i < len(items) - 1:
            key = items[i]
            if isinstance(key, str):
                result[key] = items[i + 1]
                i += 2
            else:
                i += 1
        return result

    def property_key_name(self, items):
        """property_key_name → schema_name"""
        return items[0]

    def properties(self, items):
        """properties → map_literal | parameter"""
        return items[0] if items else {}
