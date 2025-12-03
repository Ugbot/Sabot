# cython: language_level=3
# distutils: language=c++

"""
Plan-to-Operators Builder

Builds Sabot Cython operators from OperatorSpec plans.
This is the bridge between the C++ plan representation and
Python-accessible operator instances.
"""

cimport cython
from typing import Dict, List, Any, Optional, Tuple
from sabot import cyarrow as ca


cdef class OperatorBuilder:
    """
    Builds Cython operators from plan specifications.

    Supports Core 5 operators:
    - TableScan: Read from registered tables
    - Filter: Apply predicates using Arrow compute
    - Projection: Column selection/computation
    - HashJoin: Hash-based join with shuffle
    - Aggregate: Group-by aggregation
    """

    cdef dict _table_registry
    cdef int _default_batch_size
    cdef bint _enable_spill

    def __init__(
        self,
        table_registry: Dict[str, ca.Table],
        default_batch_size: int = 65536,
        enable_spill: bool = True
    ):
        """
        Initialize the operator builder.

        Args:
            table_registry: Registered tables by name
            default_batch_size: Default batch size for morsel execution
            enable_spill: Whether to enable spill-to-disk for large operators
        """
        self._table_registry = table_registry
        self._default_batch_size = default_batch_size
        self._enable_spill = enable_spill

    cpdef object build(self, dict spec, object input_data=None):
        """
        Build an operator from a specification dictionary.

        Args:
            spec: OperatorSpec as dictionary
            input_data: Optional input table for the operator

        Returns:
            Operator instance or callable
        """
        cdef str op_type = spec.get('type', '')

        if op_type == 'TableScan':
            return self._build_table_scan(spec)
        elif op_type == 'Filter':
            return self._build_filter(spec, input_data)
        elif op_type == 'Projection':
            return self._build_projection(spec)
        elif op_type == 'HashJoin':
            return self._build_hash_join(spec)
        elif op_type == 'Aggregate':
            return self._build_aggregate(spec)
        elif op_type == 'Sort':
            return self._build_sort(spec)
        elif op_type == 'Limit':
            return self._build_limit(spec)
        else:
            # Unknown operator - return passthrough
            return self._build_passthrough()

    cdef object _build_table_scan(self, dict spec):
        """Build a table scan operator."""
        cdef str table_name = spec.get('table_name', '')
        cdef list projected_columns = spec.get('projected_columns', [])

        if table_name not in self._table_registry:
            raise ValueError(f"Table not found: {table_name}")

        table = self._table_registry[table_name]

        # Create a callable that returns the table (optionally with projection)
        if projected_columns:
            def scan():
                return table.select(projected_columns)
        else:
            def scan():
                return table

        return scan

    cdef object _build_filter(self, dict spec, object input_data):
        """Build a filter operator."""
        cdef str filter_expr = spec.get('filter_expression', '')

        if not filter_expr:
            return self._build_passthrough()

        # Try to use CythonFilterOperator if available
        try:
            from sabot._cython.operators.filter_operator import CythonFilterOperator

            # Parse filter expression and create operator
            return CythonFilterOperator(
                predicate=filter_expr,
                input_schema=input_data.schema if input_data else None
            )
        except ImportError:
            # Fallback to Arrow compute filter
            return self._build_arrow_filter(filter_expr)

    cdef object _build_arrow_filter(self, str filter_expr):
        """Build a filter using Arrow compute."""
        # Parse simple comparison expressions
        # Format: "column op value" e.g., "amount > 100"
        import re

        def filter_func(table):
            # Try to parse and apply the filter
            match = re.match(r'(\w+)\s*(>|<|>=|<=|==|!=)\s*(\d+(?:\.\d+)?)', filter_expr)
            if match:
                col_name, op, value = match.groups()
                value = float(value) if '.' in value else int(value)

                # Get column
                if col_name not in table.column_names:
                    return table

                col = table.column(col_name)

                # Apply comparison
                import pyarrow.compute as pc
                if op == '>':
                    mask = pc.greater(col, value)
                elif op == '<':
                    mask = pc.less(col, value)
                elif op == '>=':
                    mask = pc.greater_equal(col, value)
                elif op == '<=':
                    mask = pc.less_equal(col, value)
                elif op == '==':
                    mask = pc.equal(col, value)
                elif op == '!=':
                    mask = pc.not_equal(col, value)
                else:
                    return table

                return pc.filter(table, mask)

            return table

        return filter_func

    cdef object _build_projection(self, dict spec):
        """Build a projection operator."""
        cdef list columns = spec.get('projected_columns', [])

        if not columns:
            return self._build_passthrough()

        def project(table):
            # Filter to only existing columns
            existing = [c for c in columns if c in table.column_names]
            if existing:
                return table.select(existing)
            return table

        return project

    cdef object _build_hash_join(self, dict spec):
        """Build a hash join operator."""
        cdef list left_keys = spec.get('left_keys', [])
        cdef list right_keys = spec.get('right_keys', [])
        cdef str join_type = spec.get('join_type', 'inner')

        try:
            from sabot._cython.operators.joins import CythonHashJoinOperator

            # Return a partially configured join operator
            # The actual join will need both inputs at execution time
            class JoinOperator:
                def __init__(self, left_keys, right_keys, join_type):
                    self.left_keys = left_keys
                    self.right_keys = right_keys
                    self.join_type = join_type

                def execute(self, left_table, right_table):
                    join_op = CythonHashJoinOperator(
                        left_source=None,
                        right_source=None,
                        left_keys=self.left_keys,
                        right_keys=self.right_keys,
                        join_type=self.join_type
                    )
                    return join_op.execute_tables(left_table, right_table)

            return JoinOperator(left_keys, right_keys, join_type)

        except ImportError:
            # Fallback to simple nested loop join
            return self._build_simple_join(left_keys, right_keys, join_type)

    cdef object _build_simple_join(self, list left_keys, list right_keys, str join_type):
        """Build a simple join using pandas (fallback)."""
        def join_func(left_table, right_table):
            # Convert to pandas for simple join
            left_df = left_table.to_pandas()
            right_df = right_table.to_pandas()

            # Perform join
            result_df = left_df.merge(
                right_df,
                left_on=left_keys,
                right_on=right_keys,
                how=join_type
            )

            # Convert back to Arrow
            return ca.Table.from_pandas(result_df)

        return join_func

    cdef object _build_aggregate(self, dict spec):
        """Build an aggregate operator."""
        cdef list group_keys = spec.get('group_by_keys', [])
        cdef list agg_functions = spec.get('aggregate_functions', [])
        cdef list agg_columns = spec.get('aggregate_columns', [])

        try:
            from sabot._cython.operators.aggregations import CythonGroupByOperator

            # Build aggregation specs
            aggregations = list(zip(agg_functions, agg_columns))

            return CythonGroupByOperator(
                group_keys=group_keys,
                aggregations=aggregations
            )

        except ImportError:
            # Fallback to Arrow compute aggregation
            return self._build_arrow_aggregate(group_keys, agg_functions, agg_columns)

    cdef object _build_arrow_aggregate(self, list group_keys, list agg_functions, list agg_columns):
        """Build aggregation using Arrow compute."""
        def aggregate_func(table):
            import pyarrow.compute as pc

            if not group_keys:
                # Global aggregation
                results = {}
                for func, col in zip(agg_functions, agg_columns):
                    if col in table.column_names:
                        column = table.column(col)
                        if func.upper() == 'SUM':
                            results[f'{func}_{col}'] = [pc.sum(column).as_py()]
                        elif func.upper() == 'COUNT':
                            results[f'{func}_{col}'] = [pc.count(column).as_py()]
                        elif func.upper() == 'AVG':
                            results[f'{func}_{col}'] = [pc.mean(column).as_py()]
                        elif func.upper() == 'MIN':
                            results[f'{func}_{col}'] = [pc.min(column).as_py()]
                        elif func.upper() == 'MAX':
                            results[f'{func}_{col}'] = [pc.max(column).as_py()]

                return ca.table(results)

            else:
                # Group-by aggregation using Arrow
                # Use hash_aggregate from Arrow compute
                agg_specs = []
                for func, col in zip(agg_functions, agg_columns):
                    if col in table.column_names:
                        agg_specs.append((col, func.lower()))

                result = table.group_by(group_keys).aggregate(agg_specs)
                return result

        return aggregate_func

    cdef object _build_sort(self, dict spec):
        """Build a sort operator."""
        cdef list sort_keys = spec.get('sort_keys', [])
        cdef list sort_orders = spec.get('sort_orders', [])  # 'asc' or 'desc'

        def sort_func(table):
            import pyarrow.compute as pc

            if not sort_keys:
                return table

            # Build sort indices
            sort_options = [(key, 'ascending' if order == 'asc' else 'descending')
                           for key, order in zip(sort_keys, sort_orders)]

            indices = pc.sort_indices(table, sort_keys=sort_options)
            return pc.take(table, indices)

        return sort_func

    cdef object _build_limit(self, dict spec):
        """Build a limit operator."""
        cdef int limit = spec.get('int_params', {}).get('limit', 100)
        cdef int offset = spec.get('int_params', {}).get('offset', 0)

        def limit_func(table):
            return table.slice(offset, limit)

        return limit_func

    cdef object _build_passthrough(self):
        """Build a passthrough operator (identity)."""
        def passthrough(table):
            return table
        return passthrough


def build_operator_pipeline(
    specs: List[dict],
    table_registry: Dict[str, ca.Table],
    input_data: Optional[ca.Table] = None
) -> List[Any]:
    """
    Build a pipeline of operators from a list of specs.

    Args:
        specs: List of OperatorSpec dictionaries
        table_registry: Registered tables
        input_data: Optional initial input data

    Returns:
        List of operator instances/callables
    """
    builder = OperatorBuilder(table_registry)
    operators = []

    current_input = input_data
    for spec in specs:
        op = builder.build(spec, current_input)
        operators.append(op)

    return operators


def execute_operator_pipeline(
    operators: List[Any],
    initial_input: ca.Table
) -> ca.Table:
    """
    Execute a pipeline of operators sequentially.

    Args:
        operators: List of operator instances
        initial_input: Initial input table

    Returns:
        Result after all operators
    """
    result = initial_input

    for op in operators:
        if callable(op):
            result = op(result)
        elif hasattr(op, 'execute'):
            result = op.execute(result)
        elif hasattr(op, 'process'):
            result = op.process(result)
        # else: skip non-executable operators

    return result
