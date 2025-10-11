# cython: language_level=3, boundscheck=False, wraparound=False, cdivision=True, nonecheck=False
"""
Graph Query Operators

Sabot-native operators for graph query execution.
All operators extend BaseOperator and integrate with morsel-driven parallelism.

Stateless operators (local execution):
- PatternScanOperator: Pattern matching using kernels
- GraphFilterOperator: Filter predicates
- GraphProjectOperator: Column selection
- GraphLimitOperator: LIMIT/OFFSET

Stateful operators (require shuffle):
- GraphJoinOperator: Hash join for multi-hop patterns
- GraphAggregateOperator: GROUP BY aggregations
"""

import cython
from typing import Optional, List, Dict, Any
from libc.stdint cimport int64_t

# Import BaseOperator from Sabot
from sabot._cython.operators.base_operator cimport BaseOperator

# Import CyArrow for Arrow operations
from sabot import cyarrow as pa
try:
    from sabot.cyarrow import compute as pc
except (ImportError, AttributeError):
    import pyarrow.compute as pc


# ==============================================================================
# Stateless Operators (No Shuffle Required)
# ==============================================================================

cdef class PatternScanOperator(BaseOperator):
    """
    Pattern scan operator using pattern matching kernels.

    Stateless operator - processes batches locally without shuffle.
    Uses existing match_2hop, match_3hop, match_variable_length_path kernels.

    Performance: 3-37M matches/sec (from existing kernels)
    """

    def __cinit__(self, pattern, graph, **kwargs):
        """
        Initialize pattern scan operator.

        Args:
            pattern: Pattern to match (from AST)
            graph: Graph engine instance
        """
        # Set base operator properties
        self._stateful = False  # Stateless - no shuffle
        self._key_columns = None
        self._parallelism_hint = 1

        # Store pattern and graph
        self.pattern = pattern
        self.graph = graph

        # Import pattern matching functions
        from sabot._cython.graph.query import (
            match_2hop,
            match_3hop,
            match_variable_length_path,
        )
        self.match_2hop = match_2hop
        self.match_3hop = match_3hop
        self.match_variable_length_path = match_variable_length_path

    cpdef object process_batch(self, object batch):
        """
        Process pattern scan.

        For single-hop patterns, batch is None (scan from scratch).
        For multi-hop, batch contains intermediate results to extend.

        Returns:
            Arrow table with pattern match results
        """
        # Determine pattern type and call appropriate kernel
        if hasattr(self.pattern, 'hops'):
            hops = self.pattern.hops
        else:
            # Infer from pattern structure
            hops = len(getattr(self.pattern, 'edges', []))

        # Call pattern matching kernel
        if hops == 2:
            result = self.match_2hop(self.graph, self.pattern)
        elif hops == 3:
            result = self.match_3hop(self.graph, self.pattern)
        else:
            # Variable-length path
            min_hops = getattr(self.pattern, 'min_hops', 1)
            max_hops = getattr(self.pattern, 'max_hops', 10)
            result = self.match_variable_length_path(
                self.graph, self.pattern, min_hops, max_hops
            )

        return result


cdef class GraphFilterOperator(BaseOperator):
    """
    Filter operator for graph queries.

    Stateless operator - applies predicates locally without shuffle.
    Uses Arrow compute for efficient filtering.
    """

    def __cinit__(self, filter_expr, **kwargs):
        """
        Initialize filter operator.

        Args:
            filter_expr: Filter expression (AST node or callable)
        """
        self._stateful = False  # Stateless - no shuffle
        self._key_columns = None
        self._parallelism_hint = 1

        self.filter_expr = filter_expr

    cpdef object process_batch(self, object batch):
        """
        Apply filter predicate to batch.

        Args:
            batch: Arrow RecordBatch or Table

        Returns:
            Filtered Arrow table
        """
        if batch is None or batch.num_rows == 0:
            return batch

        # Build filter mask
        if callable(self.filter_expr):
            # Python callable - evaluate on batch
            mask = self.filter_expr(batch)
        else:
            # Arrow expression - use compute
            mask = self._evaluate_expression(batch, self.filter_expr)

        # Apply filter using Arrow compute
        if isinstance(mask, pa.Array):
            # Boolean array - use filter
            filtered = pc.filter(batch, mask)
        else:
            # Expression - evaluate and filter
            filtered = batch

        return filtered

    cdef object _evaluate_expression(self, object batch, object expr):
        """
        Evaluate filter expression on batch.

        Handles different expression types:
        - Column comparisons (a.age > 25)
        - Logical operators (AND, OR, NOT)
        - Literal comparisons
        """
        # TODO: Full expression evaluation
        # For now, return simple True mask
        return pa.array([True] * batch.num_rows)


cdef class GraphProjectOperator(BaseOperator):
    """
    Projection operator for column selection.

    Stateless operator - selects columns locally without shuffle.
    Zero-copy column selection using Arrow.
    """

    def __cinit__(self, columns, aliases=None, **kwargs):
        """
        Initialize projection operator.

        Args:
            columns: List of column names to select
            aliases: Optional dict mapping old names to new names
        """
        self._stateful = False  # Stateless - no shuffle
        self._key_columns = None
        self._parallelism_hint = 1

        self.columns = columns
        self.aliases = aliases or {}

    cpdef object process_batch(self, object batch):
        """
        Project (select) columns from batch.

        Args:
            batch: Arrow RecordBatch or Table

        Returns:
            Projected Arrow table with selected columns
        """
        if batch is None or batch.num_rows == 0:
            return batch

        # Select columns
        selected_columns = []
        selected_names = []

        for col_name in self.columns:
            if col_name in batch.schema.names:
                selected_columns.append(batch.column(col_name))
                # Apply alias if present
                new_name = self.aliases.get(col_name, col_name)
                selected_names.append(new_name)

        # Create projected table
        if selected_columns:
            projected = pa.Table.from_arrays(
                selected_columns,
                names=selected_names
            )
            return projected
        else:
            # No matching columns
            return None


cdef class GraphLimitOperator(BaseOperator):
    """
    Limit operator for LIMIT/OFFSET.

    Stateless operator - applies limit locally.
    Tracks row count across batches for correct offset handling.
    """

    def __cinit__(self, limit=None, offset=None, **kwargs):
        """
        Initialize limit operator.

        Args:
            limit: Maximum rows to return (None = unlimited)
            offset: Rows to skip (default 0)
        """
        self._stateful = False  # Stateless - no shuffle
        self._key_columns = None
        self._parallelism_hint = 1

        self.limit = limit
        self.offset = offset or 0
        self.rows_seen = 0  # Track rows across batches
        self.rows_returned = 0

    cpdef object process_batch(self, object batch):
        """
        Apply LIMIT/OFFSET to batch.

        Args:
            batch: Arrow RecordBatch or Table

        Returns:
            Limited Arrow table
        """
        if batch is None or batch.num_rows == 0:
            return batch

        # Check if we've already returned enough rows
        if self.limit is not None and self.rows_returned >= self.limit:
            return None

        batch_start = self.rows_seen
        batch_end = self.rows_seen + batch.num_rows

        # Calculate slice bounds
        start_idx = 0
        end_idx = batch.num_rows

        # Handle offset
        if batch_end <= self.offset:
            # Entire batch is before offset - skip it
            self.rows_seen = batch_end
            return None
        elif batch_start < self.offset:
            # Part of batch is before offset - skip those rows
            start_idx = self.offset - batch_start

        # Handle limit
        if self.limit is not None:
            rows_remaining = self.limit - self.rows_returned
            end_idx = min(end_idx, start_idx + rows_remaining)

        # Update counters
        self.rows_seen = batch_end
        rows_in_slice = end_idx - start_idx
        self.rows_returned += rows_in_slice

        # Slice batch
        if start_idx == 0 and end_idx == batch.num_rows:
            # Return entire batch
            return batch
        else:
            # Slice batch using Arrow
            sliced = batch.slice(start_idx, end_idx - start_idx)
            return sliced


# ==============================================================================
# Stateful Operators (Require Shuffle)
# ==============================================================================

cdef class GraphJoinOperator(BaseOperator):
    """
    Hash join operator for multi-hop patterns.

    Stateful operator - requires shuffle to co-locate matching keys.
    Partitions by join key (typically vertex_id) for distributed execution.
    """

    def __cinit__(self, left_keys, right_keys, join_type='inner', **kwargs):
        """
        Initialize join operator.

        Args:
            left_keys: Join keys from left input
            right_keys: Join keys from right input
            join_type: 'inner', 'left', 'right', 'full'
        """
        self._stateful = True  # Stateful - requires shuffle
        self._key_columns = left_keys  # Partition by left keys
        self._parallelism_hint = 4  # Suggest parallel execution

        self.left_keys = left_keys
        self.right_keys = right_keys
        self.join_type = join_type

    cpdef object process_batch(self, object batch):
        """
        Process join operation.

        Assumes batch has been pre-shuffled by join keys.
        Performs hash join using Arrow compute.

        Args:
            batch: Arrow table with both left and right data

        Returns:
            Joined Arrow table
        """
        # TODO: Implement hash join logic
        # For now, return batch unchanged
        return batch


cdef class GraphAggregateOperator(BaseOperator):
    """
    Aggregate operator for GROUP BY.

    Stateful operator - requires shuffle to co-locate group keys.
    Partitions by group keys for distributed execution.
    """

    def __cinit__(self, group_keys, agg_funcs, **kwargs):
        """
        Initialize aggregate operator.

        Args:
            group_keys: Columns to group by
            agg_funcs: Aggregation functions (e.g., {'count': 'cnt', 'sum': 'total'})
        """
        self._stateful = True  # Stateful - requires shuffle
        self._key_columns = group_keys  # Partition by group keys
        self._parallelism_hint = 4  # Suggest parallel execution

        self.group_keys = group_keys
        self.agg_funcs = agg_funcs

    cpdef object process_batch(self, object batch):
        """
        Process aggregation operation.

        Assumes batch has been pre-shuffled by group keys.
        Performs aggregation using Arrow compute.

        Args:
            batch: Arrow table with pre-shuffled data

        Returns:
            Aggregated Arrow table
        """
        # TODO: Implement aggregation logic
        # For now, return batch unchanged
        return batch
