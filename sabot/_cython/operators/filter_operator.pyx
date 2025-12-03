# cython: language_level=3, boundscheck=False, wraparound=False
"""
Cython Filter Operator

High-performance filter operator using Arrow compute.
Supports simple comparisons and AND/OR expressions.
"""

cimport cython
from libc.stdint cimport int32_t, int64_t
import re
import pyarrow as pa
import pyarrow.compute as pc


cdef class CythonFilterOperator:
    """
    Cython-accelerated filter operator for SQL WHERE clauses.

    Parses filter expressions and applies them efficiently using Arrow compute.
    Supports:
    - Simple comparisons: column > value, column = value
    - AND expressions: col1 > 10 AND col2 < 20
    - OR expressions: col1 = 1 OR col1 = 2

    Example:
        filter_op = CythonFilterOperator('amount > 100 AND status = 1')
        result = filter_op.apply(table)
    """

    cdef:
        str _expression
        object _compiled_filter  # Cached compiled filter function

    def __init__(self, str expression):
        """
        Initialize filter operator.

        Args:
            expression: SQL filter expression (e.g., "amount > 100")
        """
        self._expression = expression.strip()
        self._compiled_filter = self._compile_filter(self._expression)

    cdef object _compile_filter(self, str expr):
        """Compile filter expression into a callable."""
        # Return a closure that applies the filter
        return lambda table: self._apply_filter_impl(table, expr)

    cpdef object apply(self, table):
        """
        Apply filter to table.

        Args:
            table: pyarrow.Table

        Returns:
            Filtered pyarrow.Table
        """
        return self._apply_filter_impl(table, self._expression)

    cdef object _apply_filter_impl(self, table, str expr):
        """Internal filter implementation."""
        expr = expr.strip()

        # Handle AND - apply filters sequentially
        if ' AND ' in expr.upper():
            parts = re.split(r'\s+AND\s+', expr, flags=re.IGNORECASE)
            result = table
            for part in parts:
                result = self._apply_filter_impl(result, part.strip())
            return result

        # Handle OR - compute combined mask
        if ' OR ' in expr.upper():
            parts = re.split(r'\s+OR\s+', expr, flags=re.IGNORECASE)
            masks = []
            for part in parts:
                mask = self._get_comparison_mask(table, part.strip())
                if mask is not None:
                    masks.append(mask)
            if masks:
                combined = masks[0]
                for m in masks[1:]:
                    combined = pc.or_(combined, m)
                return table.filter(combined)
            return table

        # Simple comparison
        mask = self._get_comparison_mask(table, expr)
        if mask is not None:
            return table.filter(mask)
        return table

    cdef object _get_comparison_mask(self, table, str expr):
        """
        Get boolean mask for a comparison expression.

        Supports: >, <, >=, <=, =, !=, <>
        """
        cdef str col_name, op
        cdef double float_val
        cdef int64_t int_val

        # Parse comparison: column op value
        match = re.match(
            r'(\w+)\s*(>=|<=|!=|<>|>|<|=)\s*([\'"]?[\w.-]+[\'"]?)',
            expr.strip()
        )
        if not match:
            return None

        col_name = match.group(1)
        op = match.group(2)
        value_str = match.group(3)

        # Check column exists
        if col_name not in table.column_names:
            return None

        col = table.column(col_name)

        # Parse value based on type
        value_str = value_str.strip('\'"')

        # Try numeric first
        try:
            if '.' in value_str:
                value = float(value_str)
            else:
                value = int(value_str)
        except ValueError:
            # String value
            value = value_str

        # Apply comparison
        if op == '>':
            return pc.greater(col, value)
        elif op == '<':
            return pc.less(col, value)
        elif op == '>=':
            return pc.greater_equal(col, value)
        elif op == '<=':
            return pc.less_equal(col, value)
        elif op == '=' or op == '==':
            return pc.equal(col, value)
        elif op == '!=' or op == '<>':
            return pc.not_equal(col, value)

        return None

    @property
    def expression(self):
        """Get the filter expression."""
        return self._expression


def create_filter(expression: str) -> CythonFilterOperator:
    """
    Create a filter operator from an expression.

    Args:
        expression: SQL filter expression

    Returns:
        CythonFilterOperator instance
    """
    return CythonFilterOperator(expression)


def apply_filter(table, expression: str):
    """
    Apply a filter expression to a table.

    Args:
        table: pyarrow.Table
        expression: SQL filter expression

    Returns:
        Filtered table
    """
    op = CythonFilterOperator(expression)
    return op.apply(table)
