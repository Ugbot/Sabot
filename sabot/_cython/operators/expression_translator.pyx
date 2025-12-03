# cython: language_level=3
# distutils: language=c++

"""
Expression Translator

Translates SQL/DuckDB expression strings to Arrow compute expressions.
Supports the core expression types needed for filter, projection, and aggregation.
"""

cimport cython
from typing import Dict, List, Any, Optional, Tuple, Union
import re


cdef class ExpressionToken:
    """Token from expression parsing."""
    cdef public str type
    cdef public str value
    cdef public int position

    def __init__(self, str token_type, str value, int position):
        self.type = token_type
        self.value = value
        self.position = position

    def __repr__(self):
        return f"Token({self.type}, {self.value!r}, {self.position})"


cdef class ExpressionNode:
    """AST node for parsed expressions."""
    cdef public str node_type
    cdef public object value
    cdef public list children

    def __init__(self, str node_type, object value=None, list children=None):
        self.node_type = node_type
        self.value = value
        self.children = children if children else []

    def __repr__(self):
        if self.children:
            return f"{self.node_type}({self.value}, {self.children})"
        return f"{self.node_type}({self.value})"


cdef class ExpressionLexer:
    """Lexer for SQL expressions."""

    cdef str _input
    cdef int _pos
    cdef int _length

    # Token patterns
    cdef list _patterns

    def __init__(self):
        self._patterns = [
            ('WHITESPACE', r'\s+'),
            ('NUMBER', r'\d+(?:\.\d+)?'),
            ('STRING', r"'[^']*'"),
            ('IDENTIFIER', r'[a-zA-Z_][a-zA-Z0-9_]*'),
            ('COMPARISON', r'>=|<=|!=|<>|>|<|='),
            ('OPERATOR', r'\+|-|\*|/|%'),
            ('LPAREN', r'\('),
            ('RPAREN', r'\)'),
            ('COMMA', r','),
            ('DOT', r'\.'),
        ]

    cpdef list tokenize(self, str expression):
        """Tokenize an expression string."""
        self._input = expression
        self._pos = 0
        self._length = len(expression)

        cdef list tokens = []
        cdef str remaining
        cdef bint matched

        while self._pos < self._length:
            remaining = self._input[self._pos:]
            matched = False

            for token_type, pattern in self._patterns:
                match = re.match(pattern, remaining)
                if match:
                    value = match.group(0)
                    if token_type != 'WHITESPACE':
                        tokens.append(ExpressionToken(token_type, value, self._pos))
                    self._pos += len(value)
                    matched = True
                    break

            if not matched:
                raise ValueError(f"Unexpected character at position {self._pos}: {remaining[0]}")

        return tokens


cdef class ExpressionParser:
    """Parser for SQL expressions into AST."""

    cdef list _tokens
    cdef int _pos

    # SQL keywords for special handling
    cdef set _keywords
    cdef set _comparison_ops
    cdef set _logical_ops
    cdef set _aggregate_funcs

    def __init__(self):
        self._keywords = {'AND', 'OR', 'NOT', 'IN', 'BETWEEN', 'LIKE', 'IS', 'NULL', 'TRUE', 'FALSE'}
        self._comparison_ops = {'>', '<', '>=', '<=', '=', '!=', '<>'}
        self._logical_ops = {'AND', 'OR'}
        self._aggregate_funcs = {'SUM', 'COUNT', 'AVG', 'MIN', 'MAX', 'FIRST', 'LAST'}

    cpdef ExpressionNode parse(self, str expression):
        """Parse an expression string into an AST."""
        lexer = ExpressionLexer()
        self._tokens = lexer.tokenize(expression)
        self._pos = 0

        if not self._tokens:
            return ExpressionNode('literal', None)

        return self._parse_expression()

    cdef ExpressionNode _parse_expression(self):
        """Parse a full expression (handles AND/OR)."""
        left = self._parse_comparison()

        while self._pos < len(self._tokens):
            token = self._tokens[self._pos]
            if token.type == 'IDENTIFIER' and token.value.upper() in self._logical_ops:
                op = token.value.upper()
                self._pos += 1
                right = self._parse_comparison()
                left = ExpressionNode('logical', op, [left, right])
            else:
                break

        return left

    cdef ExpressionNode _parse_comparison(self):
        """Parse a comparison expression."""
        left = self._parse_term()

        if self._pos < len(self._tokens):
            token = self._tokens[self._pos]
            if token.type == 'COMPARISON':
                op = token.value
                if op == '<>':
                    op = '!='
                self._pos += 1
                right = self._parse_term()
                return ExpressionNode('comparison', op, [left, right])

        return left

    cdef ExpressionNode _parse_term(self):
        """Parse a term (handles +/-)."""
        left = self._parse_factor()

        while self._pos < len(self._tokens):
            token = self._tokens[self._pos]
            if token.type == 'OPERATOR' and token.value in ('+', '-'):
                op = token.value
                self._pos += 1
                right = self._parse_factor()
                left = ExpressionNode('arithmetic', op, [left, right])
            else:
                break

        return left

    cdef ExpressionNode _parse_factor(self):
        """Parse a factor (handles *//%)."""
        left = self._parse_unary()

        while self._pos < len(self._tokens):
            token = self._tokens[self._pos]
            if token.type == 'OPERATOR' and token.value in ('*', '/', '%'):
                op = token.value
                self._pos += 1
                right = self._parse_unary()
                left = ExpressionNode('arithmetic', op, [left, right])
            else:
                break

        return left

    cdef ExpressionNode _parse_unary(self):
        """Parse unary expressions (NOT, -)."""
        if self._pos < len(self._tokens):
            token = self._tokens[self._pos]
            if token.type == 'OPERATOR' and token.value == '-':
                self._pos += 1
                operand = self._parse_unary()
                return ExpressionNode('unary', '-', [operand])
            elif token.type == 'IDENTIFIER' and token.value.upper() == 'NOT':
                self._pos += 1
                operand = self._parse_unary()
                return ExpressionNode('unary', 'NOT', [operand])

        return self._parse_primary()

    cdef ExpressionNode _parse_primary(self):
        """Parse primary expressions (literals, identifiers, function calls)."""
        if self._pos >= len(self._tokens):
            raise ValueError("Unexpected end of expression")

        token = self._tokens[self._pos]

        if token.type == 'NUMBER':
            self._pos += 1
            if '.' in token.value:
                return ExpressionNode('literal', float(token.value))
            return ExpressionNode('literal', int(token.value))

        elif token.type == 'STRING':
            self._pos += 1
            # Remove quotes
            return ExpressionNode('literal', token.value[1:-1])

        elif token.type == 'IDENTIFIER':
            name = token.value
            self._pos += 1

            # Check for function call
            if self._pos < len(self._tokens) and self._tokens[self._pos].type == 'LPAREN':
                return self._parse_function_call(name)

            # Check for keywords
            upper_name = name.upper()
            if upper_name == 'TRUE':
                return ExpressionNode('literal', True)
            elif upper_name == 'FALSE':
                return ExpressionNode('literal', False)
            elif upper_name == 'NULL':
                return ExpressionNode('literal', None)

            # Column reference
            return ExpressionNode('column', name)

        elif token.type == 'LPAREN':
            self._pos += 1
            expr = self._parse_expression()
            if self._pos >= len(self._tokens) or self._tokens[self._pos].type != 'RPAREN':
                raise ValueError("Expected closing parenthesis")
            self._pos += 1
            return expr

        else:
            raise ValueError(f"Unexpected token: {token}")

    cdef ExpressionNode _parse_function_call(self, str name):
        """Parse a function call."""
        # Skip LPAREN
        self._pos += 1

        cdef list args = []

        while self._pos < len(self._tokens) and self._tokens[self._pos].type != 'RPAREN':
            args.append(self._parse_expression())

            if self._pos < len(self._tokens) and self._tokens[self._pos].type == 'COMMA':
                self._pos += 1

        if self._pos >= len(self._tokens) or self._tokens[self._pos].type != 'RPAREN':
            raise ValueError("Expected closing parenthesis in function call")
        self._pos += 1

        return ExpressionNode('function', name.upper(), args)


cdef class ArrowExpressionBuilder:
    """Builds Arrow compute expressions from AST."""

    cpdef object build(self, ExpressionNode node):
        """Build an Arrow compute expression from an AST node."""
        import pyarrow.compute as pc

        if node.node_type == 'literal':
            return node.value

        elif node.node_type == 'column':
            return pc.field(node.value)

        elif node.node_type == 'comparison':
            left = self.build(node.children[0])
            right = self.build(node.children[1])
            op = node.value

            if op == '>':
                return pc.greater(left, right)
            elif op == '<':
                return pc.less(left, right)
            elif op == '>=':
                return pc.greater_equal(left, right)
            elif op == '<=':
                return pc.less_equal(left, right)
            elif op == '=' or op == '==':
                return pc.equal(left, right)
            elif op == '!=' or op == '<>':
                return pc.not_equal(left, right)

        elif node.node_type == 'logical':
            left = self.build(node.children[0])
            right = self.build(node.children[1])
            op = node.value

            if op == 'AND':
                return pc.and_(left, right)
            elif op == 'OR':
                return pc.or_(left, right)

        elif node.node_type == 'arithmetic':
            left = self.build(node.children[0])
            right = self.build(node.children[1])
            op = node.value

            if op == '+':
                return pc.add(left, right)
            elif op == '-':
                return pc.subtract(left, right)
            elif op == '*':
                return pc.multiply(left, right)
            elif op == '/':
                return pc.divide(left, right)

        elif node.node_type == 'unary':
            operand = self.build(node.children[0])
            op = node.value

            if op == '-':
                return pc.negate(operand)
            elif op == 'NOT':
                return pc.invert(operand)

        elif node.node_type == 'function':
            args = [self.build(child) for child in node.children]
            func_name = node.value.lower()

            # Map common SQL functions to Arrow compute
            if func_name == 'sum':
                return pc.sum(*args)
            elif func_name == 'count':
                return pc.count(*args)
            elif func_name == 'avg' or func_name == 'mean':
                return pc.mean(*args)
            elif func_name == 'min':
                return pc.min(*args)
            elif func_name == 'max':
                return pc.max(*args)
            elif func_name == 'abs':
                return pc.abs(*args)
            elif func_name == 'sqrt':
                return pc.sqrt(*args)
            elif func_name == 'length':
                return pc.utf8_length(*args)
            elif func_name == 'upper':
                return pc.utf8_upper(*args)
            elif func_name == 'lower':
                return pc.utf8_lower(*args)
            elif func_name == 'trim':
                return pc.utf8_trim(*args)
            else:
                # Try generic call
                if hasattr(pc, func_name):
                    return getattr(pc, func_name)(*args)
                raise ValueError(f"Unknown function: {func_name}")

        raise ValueError(f"Unknown node type: {node.node_type}")


class ExpressionTranslator:
    """
    High-level expression translator.

    Translates SQL expression strings to Arrow compute expressions.
    """

    def __init__(self):
        self._parser = ExpressionParser()
        self._builder = ArrowExpressionBuilder()

    def translate(self, expression: str) -> Any:
        """
        Translate an expression string to Arrow compute.

        Args:
            expression: SQL expression string

        Returns:
            Arrow compute expression
        """
        ast = self._parser.parse(expression)
        return self._builder.build(ast)

    def parse(self, expression: str) -> ExpressionNode:
        """
        Parse an expression string to AST.

        Args:
            expression: SQL expression string

        Returns:
            ExpressionNode AST
        """
        return self._parser.parse(expression)

    def create_filter_func(self, expression: str):
        """
        Create a filter function from an expression.

        Args:
            expression: Filter expression string

        Returns:
            Callable that filters a table
        """
        def filter_func(table):
            import pyarrow.compute as pc

            # Parse and translate expression
            ast = self._parser.parse(expression)

            # For comparisons, we need to handle column references specially
            if ast.node_type == 'comparison':
                left_node = ast.children[0]
                right_node = ast.children[1]
                op = ast.value

                # Get left value
                if left_node.node_type == 'column':
                    left = table.column(left_node.value)
                else:
                    left = self._builder.build(left_node)

                # Get right value
                if right_node.node_type == 'column':
                    right = table.column(right_node.value)
                else:
                    right = self._builder.build(right_node)

                # Apply comparison
                if op == '>':
                    mask = pc.greater(left, right)
                elif op == '<':
                    mask = pc.less(left, right)
                elif op == '>=':
                    mask = pc.greater_equal(left, right)
                elif op == '<=':
                    mask = pc.less_equal(left, right)
                elif op == '=' or op == '==':
                    mask = pc.equal(left, right)
                elif op == '!=' or op == '<>':
                    mask = pc.not_equal(left, right)
                else:
                    return table

                return table.filter(mask)

            return table

        return filter_func


# Module-level convenience functions
def translate_expression(expression: str) -> Any:
    """Translate a SQL expression to Arrow compute."""
    translator = ExpressionTranslator()
    return translator.translate(expression)


def create_filter(expression: str):
    """Create a filter function from an expression."""
    translator = ExpressionTranslator()
    return translator.create_filter_func(expression)
