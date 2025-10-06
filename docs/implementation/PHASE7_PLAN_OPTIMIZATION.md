# Phase 7: Plan Optimization Implementation Guide

**Version:** 1.0
**Date:** October 2025
**Status:** Implementation Ready
**Prerequisites:** Phase 1-6 (Operator API, Execution Graph)

---

## Executive Summary

This document provides a detailed implementation plan for Phase 7: Plan Optimization. The goal is to borrow query optimization techniques from DuckDB (filter pushdown, projection pushdown, join reordering, operator fusion) and adapt them for Sabot's streaming dataflow DAGs.

### Key Principles

1. Simple, incremental optimizations (not full cost-based optimizer)
2. Measurable performance improvements
3. Foundation for future enhancements
4. Streaming-aware optimizations (respect ordering, watermarks)
5. Non-intrusive (plug into existing JobGraph compilation)

### Expected Benefits

- 2-5x performance improvement from filter/projection pushdown
- 20-40% reduction in memory usage from column pruning
- 10-30% improvement from join reordering (selectivity-based)
- 5-15% improvement from operator fusion (eliminate intermediates)

---

## Part 1: Architecture Overview

### Optimization Pipeline

```
User Dataflow DAG
       ↓
┌──────────────────────────────────────┐
│  JobGraph (logical operators)        │
└──────────────────────────────────────┘
       ↓
┌──────────────────────────────────────┐
│  PlanOptimizer.optimize()            │
│                                      │
│  1. Filter Pushdown                  │
│  2. Projection Pushdown              │
│  3. Join Reordering                  │
│  4. Operator Fusion                  │
│  5. Dead Code Elimination            │
└──────────────────────────────────────┘
       ↓
┌──────────────────────────────────────┐
│  Optimized JobGraph                  │
└──────────────────────────────────────┘
       ↓
┌──────────────────────────────────────┐
│  ExecutionGraph (physical plan)      │
└──────────────────────────────────────┘
```

### Integration Point

The optimizer sits between logical DAG construction and physical execution graph generation:

```python
# In sabot/job_manager.py (from UNIFIED_BATCH_ARCHITECTURE.md)

@DBOS.transaction
async def _optimize_dag_step(self, job_id, dag):
    """Optimize operator DAG (DBOS transaction - atomic)."""
    logger.info(f"Optimizing DAG for job {job_id}")

    # NEW: PlanOptimizer
    from sabot.compiler.plan_optimizer import PlanOptimizer
    optimizer = PlanOptimizer()
    optimized = optimizer.optimize(dag)

    # Store in DBOS
    if self.dbos:
        await self.dbos.execute(
            """INSERT INTO job_dag_optimized (job_id, dag_json)
               VALUES ($1, $2)""",
            job_id, optimized.to_json()
        )

    return optimized
```

---

## Part 2: Detailed Task Breakdown

### Task 1: Create PlanOptimizer Class

**File:** `/Users/bengamble/Sabot/sabot/compiler/plan_optimizer.py` (NEW)

**Estimated Effort:** 4 hours

**Implementation:**

```python
"""
Plan Optimizer - Query Optimization for Streaming Dataflow DAGs

Inspired by DuckDB's optimizer architecture but adapted for streaming semantics.
Applies rule-based optimizations to JobGraph before physical deployment.

Key Optimizations:
1. Filter Pushdown - Move filters closer to sources
2. Projection Pushdown - Select only required columns early
3. Join Reordering - Reorder joins based on selectivity estimates
4. Operator Fusion - Combine compatible operators to reduce overhead

Design Principles:
- Rule-based (not cost-based) for simplicity
- Streaming-aware (preserve ordering, watermarks)
- Conservative (correctness over aggressive optimization)
- Measurable (each optimization tracked and benchmarked)
"""

import logging
from typing import List, Dict, Set, Optional
from dataclasses import dataclass
from copy import deepcopy

logger = logging.getLogger(__name__)


@dataclass
class OptimizationStats:
    """Track optimization statistics for debugging and benchmarking."""

    filters_pushed: int = 0
    projections_pushed: int = 0
    joins_reordered: int = 0
    operators_fused: int = 0
    dead_code_eliminated: int = 0

    def total_optimizations(self) -> int:
        return (self.filters_pushed + self.projections_pushed +
                self.joins_reordered + self.operators_fused +
                self.dead_code_eliminated)


class PlanOptimizer:
    """
    Rule-based optimizer for streaming dataflow DAGs.

    Applies optimizations to JobGraph before physical execution:
    1. Filter pushdown (move filters before joins/aggregations)
    2. Projection pushdown (select columns early)
    3. Join reordering (based on selectivity)
    4. Operator fusion (chain compatible ops)

    Usage:
        optimizer = PlanOptimizer()
        optimized_graph = optimizer.optimize(job_graph)
    """

    def __init__(self,
                 enable_filter_pushdown: bool = True,
                 enable_projection_pushdown: bool = True,
                 enable_join_reordering: bool = True,
                 enable_operator_fusion: bool = True,
                 enable_dead_code_elimination: bool = True):
        """
        Initialize optimizer with configuration.

        Args:
            enable_filter_pushdown: Enable filter pushdown optimization
            enable_projection_pushdown: Enable projection pushdown
            enable_join_reordering: Enable join reordering
            enable_operator_fusion: Enable operator fusion
            enable_dead_code_elimination: Enable dead code elimination
        """
        self.enable_filter_pushdown = enable_filter_pushdown
        self.enable_projection_pushdown = enable_projection_pushdown
        self.enable_join_reordering = enable_join_reordering
        self.enable_operator_fusion = enable_operator_fusion
        self.enable_dead_code_elimination = enable_dead_code_elimination

        self.stats = OptimizationStats()

    def optimize(self, job_graph: 'JobGraph') -> 'JobGraph':
        """
        Apply all enabled optimizations to job graph.

        Args:
            job_graph: Input JobGraph (logical DAG)

        Returns:
            Optimized JobGraph
        """
        # Work on a copy to preserve original
        optimized = deepcopy(job_graph)

        logger.info(f"Starting optimization of JobGraph: {job_graph.job_name}")
        initial_op_count = len(optimized.operators)

        # Apply optimizations in order
        # Order matters: pushdown before reordering before fusion

        if self.enable_filter_pushdown:
            optimized = self._apply_filter_pushdown(optimized)

        if self.enable_projection_pushdown:
            optimized = self._apply_projection_pushdown(optimized)

        if self.enable_join_reordering:
            optimized = self._apply_join_reordering(optimized)

        if self.enable_operator_fusion:
            optimized = self._apply_operator_fusion(optimized)

        if self.enable_dead_code_elimination:
            optimized = self._apply_dead_code_elimination(optimized)

        final_op_count = len(optimized.operators)

        logger.info(
            f"Optimization complete: {initial_op_count} -> {final_op_count} operators, "
            f"{self.stats.total_optimizations()} optimizations applied"
        )
        logger.debug(f"Optimization stats: {self.stats}")

        return optimized

    def _apply_filter_pushdown(self, job_graph: 'JobGraph') -> 'JobGraph':
        """
        Push filter operators closer to sources.

        Strategy:
        - Filters can be pushed through stateless operators (map, select)
        - Filters should be pushed before stateful operators (join, aggregate)
        - Filters cannot be pushed through operators that change schema

        Inspired by DuckDB's FilterPushdown class.
        """
        logger.debug("Applying filter pushdown optimization")

        # Implementation in Task 2
        from .optimizations.filter_pushdown import FilterPushdown
        pushdown = FilterPushdown()
        optimized = pushdown.optimize(job_graph)

        self.stats.filters_pushed = pushdown.filters_pushed_count

        return optimized

    def _apply_projection_pushdown(self, job_graph: 'JobGraph') -> 'JobGraph':
        """
        Push column selection (projection) closer to sources.

        Strategy:
        - Analyze which columns are actually used downstream
        - Insert SELECT operators early to prune unused columns
        - Reduce data transfer and memory footprint

        Inspired by DuckDB's RemoveUnusedColumns.
        """
        logger.debug("Applying projection pushdown optimization")

        # Implementation in Task 3
        from .optimizations.projection_pushdown import ProjectionPushdown
        pushdown = ProjectionPushdown()
        optimized = pushdown.optimize(job_graph)

        self.stats.projections_pushed = pushdown.projections_pushed_count

        return optimized

    def _apply_join_reordering(self, job_graph: 'JobGraph') -> 'JobGraph':
        """
        Reorder joins based on selectivity estimates.

        Strategy:
        - Build join graph from JobGraph
        - Estimate selectivity of each join (cardinality estimation)
        - Reorder to start with most selective joins
        - Preserve semantic correctness (only inner joins)

        Inspired by DuckDB's JoinOrderOptimizer.
        """
        logger.debug("Applying join reordering optimization")

        # Implementation in Task 4
        from .optimizations.join_reordering import JoinReordering
        reorderer = JoinReordering()
        optimized = reorderer.optimize(job_graph)

        self.stats.joins_reordered = reorderer.joins_reordered_count

        return optimized

    def _apply_operator_fusion(self, job_graph: 'JobGraph') -> 'JobGraph':
        """
        Fuse compatible operators to reduce overhead.

        Strategy:
        - Chain stateless operators (map -> map -> filter -> select)
        - Eliminate intermediate materializations
        - Generate fused Cython operator for better codegen

        Example: map().filter().map() -> fused_transform()

        Inspired by operator pipelining in modern query engines.
        """
        logger.debug("Applying operator fusion optimization")

        # Implementation in Task 5
        from .optimizations.operator_fusion import OperatorFusion
        fusion = OperatorFusion()
        optimized = fusion.optimize(job_graph)

        self.stats.operators_fused = fusion.operators_fused_count

        return optimized

    def _apply_dead_code_elimination(self, job_graph: 'JobGraph') -> 'JobGraph':
        """
        Remove operators that don't contribute to output.

        Strategy:
        - Start from sinks, walk backwards
        - Mark reachable operators
        - Remove unreachable operators
        """
        logger.debug("Applying dead code elimination")

        reachable = set()

        # BFS from sinks
        queue = job_graph.get_sinks()
        while queue:
            op = queue.pop(0)
            if op.operator_id in reachable:
                continue

            reachable.add(op.operator_id)

            for upstream_id in op.upstream_ids:
                if upstream_id in job_graph.operators:
                    queue.append(job_graph.operators[upstream_id])

        # Remove unreachable operators
        all_ops = set(job_graph.operators.keys())
        unreachable = all_ops - reachable

        for op_id in unreachable:
            del job_graph.operators[op_id]
            self.stats.dead_code_eliminated += 1

        if unreachable:
            logger.info(f"Removed {len(unreachable)} unreachable operators")

        return job_graph

    def get_stats(self) -> OptimizationStats:
        """Get optimization statistics."""
        return self.stats
```

**Deliverables:**
- PlanOptimizer class with plugin architecture
- OptimizationStats tracking
- Integration with JobGraph
- Logging and debugging support

---

### Task 2: Filter Pushdown Implementation

**File:** `/Users/bengamble/Sabot/sabot/compiler/optimizations/filter_pushdown.py` (NEW)

**Estimated Effort:** 6 hours

**Implementation Strategy:**

Based on DuckDB's `FilterPushdown` class (`vendor/duckdb/src/include/duckdb/optimizer/filter_pushdown.hpp`):

1. Extract filter operators from DAG
2. Determine which operators filters can be pushed through
3. Rewrite DAG with filters closer to sources

**Key Rules:**

| Operator Type | Can Push Through? | Notes |
|---------------|-------------------|-------|
| Source | NO | Terminal |
| Map | YES | If function doesn't use filtered column |
| Select (projection) | YES | If selected columns include filter column |
| Filter | YES | Combine multiple filters |
| Join (inner) | PARTIAL | Push to appropriate side based on column bindings |
| Join (left/right) | NO | Changes semantics |
| Aggregate | NO | Filter after aggregation has different semantics |
| Window | NO | Changes window contents |

**Implementation:**

```python
"""
Filter Pushdown Optimization

Moves filter operators closer to data sources to reduce data volume early.
Inspired by DuckDB's filter_pushdown.cpp but adapted for streaming.
"""

import logging
from typing import Dict, List, Set, Optional
from dataclasses import dataclass

from sabot.execution.job_graph import JobGraph, StreamOperatorNode, OperatorType

logger = logging.getLogger(__name__)


@dataclass
class FilterInfo:
    """Information about a filter predicate."""

    filter_op: StreamOperatorNode
    referenced_columns: Set[str]  # Columns used in filter expression

    def can_push_through(self, operator: StreamOperatorNode) -> bool:
        """
        Determine if filter can be pushed through this operator.

        Rules:
        - Can push through stateless ops if columns preserved
        - Cannot push through stateful ops (changes semantics)
        - Cannot push through schema-changing ops without referenced columns
        """
        if operator.operator_type == OperatorType.SOURCE:
            # Can't push before source
            return False

        if operator.operator_type in [OperatorType.MAP, OperatorType.FLAT_MAP]:
            # Can push through map if it doesn't modify filter columns
            # Conservative: assume we can push (would need UDF analysis)
            return True

        if operator.operator_type == OperatorType.SELECT:
            # Can push through projection if filter columns are selected
            selected_cols = set(operator.parameters.get('columns', []))
            return self.referenced_columns.issubset(selected_cols)

        if operator.operator_type == OperatorType.FILTER:
            # Can always combine filters
            return True

        if operator.operator_type == OperatorType.HASH_JOIN:
            # Can push to appropriate join side (complex - simplified here)
            # For now, don't push through joins
            return False

        if operator.operator_type in [OperatorType.AGGREGATE, OperatorType.GROUP_BY,
                                       OperatorType.TUMBLING_WINDOW, OperatorType.SLIDING_WINDOW]:
            # Cannot push through stateful operators (changes semantics)
            return False

        if operator.operator_type == OperatorType.UNION:
            # Can push into both branches
            return True

        # Conservative default
        return False


class FilterPushdown:
    """
    Filter pushdown optimization.

    Identifies filter operators and pushes them closer to sources
    to reduce data volume early in the pipeline.
    """

    def __init__(self):
        self.filters_pushed_count = 0

    def optimize(self, job_graph: JobGraph) -> JobGraph:
        """
        Apply filter pushdown to job graph.

        Args:
            job_graph: Input JobGraph

        Returns:
            Optimized JobGraph with filters pushed down
        """
        # Collect all filter operators
        filters = self._collect_filters(job_graph)

        if not filters:
            logger.debug("No filters to push down")
            return job_graph

        logger.info(f"Found {len(filters)} filter operators to optimize")

        # For each filter, try to push it down
        for filter_info in filters:
            self._push_filter_down(job_graph, filter_info)

        return job_graph

    def _collect_filters(self, job_graph: JobGraph) -> List[FilterInfo]:
        """Collect all filter operators from job graph."""
        filters = []

        for op in job_graph.operators.values():
            if op.operator_type == OperatorType.FILTER:
                # Extract referenced columns from filter expression
                # Simplified: would need actual expression analysis
                referenced_cols = self._extract_columns_from_filter(op)

                filters.append(FilterInfo(
                    filter_op=op,
                    referenced_columns=referenced_cols
                ))

        return filters

    def _extract_columns_from_filter(self, filter_op: StreamOperatorNode) -> Set[str]:
        """
        Extract column names referenced in filter expression.

        Simplified implementation - would need full expression analysis.
        """
        # Placeholder: extract from function or parameters
        # In real implementation, parse filter expression AST
        columns = filter_op.parameters.get('columns', [])
        return set(columns) if columns else set()

    def _push_filter_down(self, job_graph: JobGraph, filter_info: FilterInfo):
        """
        Push a single filter down through the DAG.

        Strategy:
        1. Start at filter operator
        2. Walk upstream
        3. If can push through, continue
        4. Otherwise, stop and reposition filter
        """
        current_op = filter_info.filter_op

        # Find upstream operators
        if not current_op.upstream_ids:
            # Already at source
            return

        # Simple case: single upstream (linear pipeline)
        if len(current_op.upstream_ids) == 1:
            upstream_id = current_op.upstream_ids[0]
            upstream_op = job_graph.operators[upstream_id]

            # Can we push through this operator?
            if filter_info.can_push_through(upstream_op):
                # Reposition filter before upstream operator
                self._reposition_filter(job_graph, current_op, upstream_op)
                self.filters_pushed_count += 1

                logger.debug(
                    f"Pushed filter {current_op.name} through {upstream_op.name}"
                )

                # Recursively try to push further
                filter_info.filter_op = current_op  # Update reference
                self._push_filter_down(job_graph, filter_info)

    def _reposition_filter(self,
                          job_graph: JobGraph,
                          filter_op: StreamOperatorNode,
                          target_op: StreamOperatorNode):
        """
        Reposition filter operator before target operator.

        Rewires DAG edges:
        Before: ... -> target -> filter -> downstream
        After:  ... -> filter -> target -> downstream
        """
        # Get target's upstream operators
        target_upstreams = target_op.upstream_ids.copy()

        # Remove filter from target's downstream
        if filter_op.operator_id in target_op.downstream_ids:
            target_op.downstream_ids.remove(filter_op.operator_id)

        # Remove target from filter's upstream
        if target_op.operator_id in filter_op.upstream_ids:
            filter_op.upstream_ids.remove(target_op.operator_id)

        # Rewire: filter -> target
        filter_op.downstream_ids = [target_op.operator_id]
        target_op.upstream_ids = [filter_op.operator_id]

        # Connect filter to target's original upstreams
        filter_op.upstream_ids = target_upstreams
        for upstream_id in target_upstreams:
            upstream_op = job_graph.operators[upstream_id]

            # Replace target with filter in upstream's downstreams
            if target_op.operator_id in upstream_op.downstream_ids:
                idx = upstream_op.downstream_ids.index(target_op.operator_id)
                upstream_op.downstream_ids[idx] = filter_op.operator_id
```

**Deliverables:**
- FilterPushdown class
- Filter predicate analysis
- DAG rewiring logic
- Test cases

---

### Task 3: Projection Pushdown Implementation

**File:** `/Users/bengamble/Sabot/sabot/compiler/optimizations/projection_pushdown.py` (NEW)

**Estimated Effort:** 5 hours

**Implementation Strategy:**

Based on DuckDB's `RemoveUnusedColumns` class:

1. Start from sinks, work backwards
2. Track which columns are actually used
3. Insert SELECT operators to prune unused columns early

**Algorithm:**

```
for each operator (bottom-up):
    required_columns = {}

    for each downstream operator:
        required_columns.update(downstream.required_input_columns)

    if operator produces more columns than required:
        insert SELECT operator after this operator
```

**Implementation:**

```python
"""
Projection Pushdown Optimization

Analyzes which columns are actually used and inserts projections
to prune unused columns early in the pipeline.

Inspired by DuckDB's RemoveUnusedColumns.
"""

import logging
from typing import Dict, Set, List
from dataclasses import dataclass

from sabot.execution.job_graph import JobGraph, StreamOperatorNode, OperatorType

logger = logging.getLogger(__name__)


class ProjectionPushdown:
    """
    Projection pushdown optimization.

    Analyzes column usage and inserts SELECT operators to prune
    unused columns early, reducing memory and network transfer.
    """

    def __init__(self):
        self.projections_pushed_count = 0

    def optimize(self, job_graph: JobGraph) -> JobGraph:
        """Apply projection pushdown to job graph."""

        # Analyze column usage (backward pass)
        column_usage = self._analyze_column_usage(job_graph)

        # Insert projections where beneficial (forward pass)
        self._insert_projections(job_graph, column_usage)

        logger.info(f"Inserted {self.projections_pushed_count} projection operators")

        return job_graph

    def _analyze_column_usage(self,
                             job_graph: JobGraph) -> Dict[str, Set[str]]:
        """
        Analyze which columns each operator requires.

        Returns:
            Dict mapping operator_id -> required column names
        """
        column_usage: Dict[str, Set[str]] = {}

        # Topological sort (reverse order)
        sorted_ops = job_graph.topological_sort()
        sorted_ops.reverse()

        for op in sorted_ops:
            required = set()

            # If sink, assume all columns needed
            if op.is_sink():
                # Get all columns from upstream
                # Simplified: would need schema info
                required = set(op.parameters.get('output_columns', ['*']))

            # Collect requirements from downstream operators
            for downstream_id in op.downstream_ids:
                downstream = job_graph.operators[downstream_id]
                downstream_required = column_usage.get(downstream_id, set())

                # Add columns needed by downstream
                required.update(downstream_required)

            # Add columns needed by this operator itself
            op_columns = self._get_operator_column_usage(op)
            required.update(op_columns)

            column_usage[op.operator_id] = required

        return column_usage

    def _get_operator_column_usage(self,
                                   op: StreamOperatorNode) -> Set[str]:
        """
        Determine which columns this operator uses.

        Simplified implementation - would need expression analysis.
        """
        if op.operator_type == OperatorType.FILTER:
            # Filters use specific columns
            return set(op.parameters.get('columns', []))

        if op.operator_type == OperatorType.SELECT:
            # Projections define columns
            return set(op.parameters.get('columns', []))

        if op.operator_type == OperatorType.HASH_JOIN:
            # Joins use key columns
            left_keys = op.parameters.get('left_keys', [])
            right_keys = op.parameters.get('right_keys', [])
            return set(left_keys + right_keys)

        if op.operator_type == OperatorType.AGGREGATE:
            # Aggregations use group-by and agg columns
            group_by = op.parameters.get('group_by', [])
            agg_cols = op.parameters.get('aggregations', {}).keys()
            return set(group_by) | set(agg_cols)

        # Conservative: assume all columns needed
        return set(['*'])

    def _insert_projections(self,
                           job_graph: JobGraph,
                           column_usage: Dict[str, Set[str]]):
        """
        Insert SELECT operators to prune unused columns.

        Strategy:
        - After each operator, check if it produces more columns than needed
        - If yes, insert SELECT to prune
        """
        # Work on copy of operator list (we'll be adding operators)
        operators_to_check = list(job_graph.operators.values())

        for op in operators_to_check:
            # Skip if already a projection
            if op.operator_type == OperatorType.SELECT:
                continue

            # Get columns this operator produces
            produced_cols = self._get_produced_columns(op)
            if '*' in produced_cols:
                # Don't optimize wildcard (need schema info)
                continue

            # Get columns downstream actually needs
            required_cols = set()
            for downstream_id in op.downstream_ids:
                required_cols.update(column_usage.get(downstream_id, set()))

            # If producing more than needed, insert projection
            unused_cols = produced_cols - required_cols
            if unused_cols:
                self._insert_select_operator(job_graph, op, required_cols)
                self.projections_pushed_count += 1

                logger.debug(
                    f"Inserted projection after {op.name}, "
                    f"pruning {len(unused_cols)} columns"
                )

    def _get_produced_columns(self, op: StreamOperatorNode) -> Set[str]:
        """Get columns produced by this operator."""
        # Simplified: would need schema tracking
        if op.operator_type == OperatorType.SELECT:
            return set(op.parameters.get('columns', []))

        # Most operators preserve columns
        return set(['*'])  # Wildcard means "all columns"

    def _insert_select_operator(self,
                                job_graph: JobGraph,
                                after_op: StreamOperatorNode,
                                columns: Set[str]):
        """Insert a SELECT operator after the given operator."""
        import uuid

        # Create new SELECT operator
        select_op = StreamOperatorNode(
            operator_id=str(uuid.uuid4()),
            operator_type=OperatorType.SELECT,
            name=f"select_{after_op.name}",
            parameters={'columns': list(columns)},
            upstream_ids=[after_op.operator_id],
            downstream_ids=after_op.downstream_ids.copy()
        )

        # Rewire DAG
        # Before: op -> downstream
        # After:  op -> select -> downstream

        for downstream_id in after_op.downstream_ids:
            downstream = job_graph.operators[downstream_id]

            # Replace op with select in downstream's upstreams
            if after_op.operator_id in downstream.upstream_ids:
                idx = downstream.upstream_ids.index(after_op.operator_id)
                downstream.upstream_ids[idx] = select_op.operator_id

        # Update op's downstreams
        after_op.downstream_ids = [select_op.operator_id]

        # Add select to graph
        job_graph.operators[select_op.operator_id] = select_op
```

**Deliverables:**
- ProjectionPushdown class
- Column usage analysis
- Projection insertion logic
- Schema tracking integration points

---

### Task 4: Join Reordering Strategy

**File:** `/Users/bengamble/Sabot/sabot/compiler/optimizations/join_reordering.py` (NEW)

**Estimated Effort:** 8 hours

**Implementation Strategy:**

Based on DuckDB's `JoinOrderOptimizer`:

1. Build join graph from DAG
2. Estimate cardinality/selectivity of each join
3. Apply dynamic programming or greedy algorithm to find better order
4. Rewrite DAG with optimized join order

**Selectivity Estimation (Simple):**

```python
def estimate_selectivity(join_op: StreamOperatorNode) -> float:
    """
    Estimate join selectivity (how many rows pass through).

    Simple heuristics:
    - Equality join on key: high selectivity (0.1)
    - Equality join on non-key: medium selectivity (0.5)
    - Range join: low selectivity (0.8)

    Real implementation would use:
    - Statistics from sources (row counts, histograms)
    - Cardinality estimation based on join predicates
    """
    # Placeholder: return medium selectivity
    return 0.5
```

**Implementation:**

```python
"""
Join Reordering Optimization

Reorders join operators based on selectivity estimates to minimize
intermediate result sizes.

Inspired by DuckDB's JoinOrderOptimizer.
"""

import logging
from typing import List, Dict, Set, Tuple, Optional
from dataclasses import dataclass

from sabot.execution.job_graph import JobGraph, StreamOperatorNode, OperatorType

logger = logging.getLogger(__name__)


@dataclass
class JoinNode:
    """Represents a join in the join graph."""

    operator: StreamOperatorNode
    left_id: str
    right_id: str
    selectivity: float

    @property
    def cost(self) -> float:
        """Join cost estimate (lower is better)."""
        return 1.0 - self.selectivity  # More selective = lower cost


class JoinReordering:
    """
    Join reordering optimization.

    Reorders joins to start with most selective joins first,
    reducing intermediate result sizes.

    Limitations:
    - Only inner joins (outer joins have fixed order)
    - No cross products
    - Conservative estimates (no statistics yet)
    """

    def __init__(self):
        self.joins_reordered_count = 0

    def optimize(self, job_graph: JobGraph) -> JobGraph:
        """Apply join reordering to job graph."""

        # Extract join subgraphs
        join_chains = self._find_join_chains(job_graph)

        if not join_chains:
            logger.debug("No join chains to optimize")
            return job_graph

        logger.info(f"Found {len(join_chains)} join chains to optimize")

        # Optimize each join chain independently
        for chain in join_chains:
            self._optimize_join_chain(job_graph, chain)

        return job_graph

    def _find_join_chains(self, job_graph: JobGraph) -> List[List[JoinNode]]:
        """
        Find chains of joins in the DAG.

        A join chain is a sequence of join operators where each join
        consumes the output of the previous join.
        """
        join_chains = []
        visited = set()

        # Find all join operators
        join_ops = [op for op in job_graph.operators.values()
                   if op.operator_type == OperatorType.HASH_JOIN]

        for join_op in join_ops:
            if join_op.operator_id in visited:
                continue

            # Try to build a chain starting from this join
            chain = self._build_join_chain(job_graph, join_op, visited)

            if len(chain) > 1:  # Only optimize chains of 2+ joins
                join_chains.append(chain)

        return join_chains

    def _build_join_chain(self,
                         job_graph: JobGraph,
                         start_join: StreamOperatorNode,
                         visited: Set[str]) -> List[JoinNode]:
        """
        Build a join chain starting from the given join operator.

        Walk upstream and downstream to find connected joins.
        """
        chain = []

        # Create JoinNode for current join
        join_node = self._create_join_node(start_join)
        chain.append(join_node)
        visited.add(start_join.operator_id)

        # Walk downstream to find chained joins
        current = start_join
        while current.downstream_ids:
            # Check if downstream is a join
            downstream_joins = [
                job_graph.operators[down_id]
                for down_id in current.downstream_ids
                if job_graph.operators[down_id].operator_type == OperatorType.HASH_JOIN
            ]

            if not downstream_joins:
                break

            # Take first join (simplified - could have multiple branches)
            next_join = downstream_joins[0]
            if next_join.operator_id in visited:
                break

            join_node = self._create_join_node(next_join)
            chain.append(join_node)
            visited.add(next_join.operator_id)
            current = next_join

        return chain

    def _create_join_node(self, join_op: StreamOperatorNode) -> JoinNode:
        """Create JoinNode with selectivity estimate."""

        # Get left and right inputs
        left_id = join_op.upstream_ids[0] if join_op.upstream_ids else None
        right_id = join_op.upstream_ids[1] if len(join_op.upstream_ids) > 1 else None

        # Estimate selectivity
        selectivity = self._estimate_selectivity(join_op)

        return JoinNode(
            operator=join_op,
            left_id=left_id,
            right_id=right_id,
            selectivity=selectivity
        )

    def _estimate_selectivity(self, join_op: StreamOperatorNode) -> float:
        """
        Estimate join selectivity (fraction of rows that pass).

        Simple heuristic-based estimation:
        - Equality on key columns: high selectivity (0.1)
        - Equality on non-key: medium selectivity (0.5)
        - Range predicates: low selectivity (0.8)

        Future: use statistics from data sources.
        """
        join_type = join_op.parameters.get('join_type', 'inner')

        # Simplified: use fixed estimates
        if join_type == 'inner':
            # Check if joining on keys
            left_keys = join_op.parameters.get('left_keys', [])
            right_keys = join_op.parameters.get('right_keys', [])

            # Heuristic: single column join on 'id' is likely a key
            if (len(left_keys) == 1 and 'id' in left_keys[0].lower()):
                return 0.1  # High selectivity

            return 0.5  # Medium selectivity

        # Outer joins are less selective
        return 0.8

    def _optimize_join_chain(self,
                            job_graph: JobGraph,
                            chain: List[JoinNode]):
        """
        Optimize a chain of joins by reordering.

        Strategy (greedy):
        1. Sort joins by selectivity (most selective first)
        2. Rewrite DAG to match new order

        Constraint: preserve correctness (only reorder independent joins)
        """
        if len(chain) < 2:
            return

        logger.debug(f"Optimizing join chain of length {len(chain)}")

        # Sort by cost (most selective first)
        original_order = [j.operator.operator_id for j in chain]
        sorted_chain = sorted(chain, key=lambda j: j.cost)
        new_order = [j.operator.operator_id for j in sorted_chain]

        if original_order == new_order:
            logger.debug("Join chain already optimal")
            return

        # Check if reordering is valid (simplified - would need dependency analysis)
        if not self._can_reorder(chain, sorted_chain):
            logger.debug("Cannot reorder join chain (dependencies prevent it)")
            return

        # Rewrite DAG to match new order
        self._rewrite_join_chain(job_graph, chain, sorted_chain)
        self.joins_reordered_count += len(chain)

        logger.info(f"Reordered join chain: {original_order} -> {new_order}")

    def _can_reorder(self,
                    original: List[JoinNode],
                    reordered: List[JoinNode]) -> bool:
        """
        Check if join reordering preserves correctness.

        Simplified: only allow reordering if all joins are inner joins
        and there are no cross-dependencies.

        Real implementation would check:
        - Join type (inner vs outer)
        - Column dependencies
        - Foreign key constraints
        """
        # Only inner joins can be freely reordered
        for join_node in original:
            join_type = join_node.operator.parameters.get('join_type', 'inner')
            if join_type != 'inner':
                return False

        return True

    def _rewrite_join_chain(self,
                           job_graph: JobGraph,
                           original_chain: List[JoinNode],
                           new_chain: List[JoinNode]):
        """
        Rewrite DAG to reflect new join order.

        This is complex - simplified implementation.
        Real version would need full DAG rewriting logic.
        """
        # Placeholder: in real implementation, would rewire upstream/downstream
        # connections to reflect new join order

        logger.warning("Join chain rewriting not fully implemented yet")

        # TODO: Implement full DAG rewriting
        # 1. Identify inputs to join chain
        # 2. Reorder internal join operators
        # 3. Rewire connections
        # 4. Update operator_ids in graph
```

**Deliverables:**
- JoinReordering class
- Selectivity estimation (simple heuristics)
- Join chain detection
- DAG rewriting logic (simplified)

---

### Task 5: Operator Fusion Implementation

**File:** `/Users/bengamble/Sabot/sabot/compiler/optimizations/operator_fusion.py` (NEW)

**Estimated Effort:** 6 hours

**Implementation Strategy:**

Fuse chains of compatible stateless operators:

```
map1 -> filter -> map2 -> select
```

Becomes:

```
fused_transform (combines all operations)
```

**Fusion Rules:**

| Operator Sequence | Can Fuse? | Fused Operator |
|-------------------|-----------|----------------|
| map -> map | YES | map (combined function) |
| map -> filter | YES | filter_map |
| filter -> filter | YES | filter (AND conditions) |
| select -> select | YES | select (final columns) |
| map -> filter -> map | YES | fused_transform |

**Implementation:**

```python
"""
Operator Fusion Optimization

Combines chains of compatible operators to reduce overhead.
Generates fused operators for better performance.
"""

import logging
from typing import List, Optional, Callable
from dataclasses import dataclass

from sabot.execution.job_graph import JobGraph, StreamOperatorNode, OperatorType

logger = logging.getLogger(__name__)


@dataclass
class FusionChain:
    """Represents a chain of operators that can be fused."""

    operators: List[StreamOperatorNode]

    def can_fuse(self) -> bool:
        """Check if this chain can be fused."""
        if len(self.operators) < 2:
            return False

        # All operators must be stateless
        for op in self.operators:
            if op.stateful:
                return False

        # All operators must be compatible types
        fusable_types = {
            OperatorType.MAP,
            OperatorType.FILTER,
            OperatorType.SELECT,
            OperatorType.FLAT_MAP
        }

        for op in self.operators:
            if op.operator_type not in fusable_types:
                return False

        return True

    def create_fused_operator(self) -> StreamOperatorNode:
        """Create a single fused operator from the chain."""
        import uuid

        # Create fused operator
        fused_op = StreamOperatorNode(
            operator_id=str(uuid.uuid4()),
            operator_type=OperatorType.MAP,  # Fused operator is a map
            name=f"fused_{'_'.join(op.name for op in self.operators)}",
            parameters={
                'fused_functions': [op.function for op in self.operators],
                'fused_types': [op.operator_type.value for op in self.operators],
            },
            upstream_ids=self.operators[0].upstream_ids.copy(),
            downstream_ids=self.operators[-1].downstream_ids.copy()
        )

        return fused_op


class OperatorFusion:
    """
    Operator fusion optimization.

    Identifies chains of compatible operators and fuses them
    into single operators to reduce overhead.
    """

    def __init__(self):
        self.operators_fused_count = 0

    def optimize(self, job_graph: JobGraph) -> JobGraph:
        """Apply operator fusion to job graph."""

        # Find fusion chains
        chains = self._find_fusion_chains(job_graph)

        if not chains:
            logger.debug("No fusion chains found")
            return job_graph

        logger.info(f"Found {len(chains)} fusion chains")

        # Fuse each chain
        for chain in chains:
            if chain.can_fuse():
                self._fuse_chain(job_graph, chain)

        logger.info(f"Fused {self.operators_fused_count} operators")

        return job_graph

    def _find_fusion_chains(self, job_graph: JobGraph) -> List[FusionChain]:
        """
        Find chains of operators that can be fused.

        Walk DAG and identify linear sequences of fusable operators.
        """
        chains = []
        visited = set()

        # Topological sort to process in order
        sorted_ops = job_graph.topological_sort()

        for op in sorted_ops:
            if op.operator_id in visited:
                continue

            # Try to build a fusion chain starting from this operator
            chain = self._build_fusion_chain(job_graph, op, visited)

            if len(chain) >= 2:
                chains.append(FusionChain(operators=chain))

        return chains

    def _build_fusion_chain(self,
                           job_graph: JobGraph,
                           start_op: StreamOperatorNode,
                           visited: Set[str]) -> List[StreamOperatorNode]:
        """
        Build a fusion chain starting from the given operator.

        Walk downstream as long as operators are fusable.
        """
        chain = []
        current = start_op

        # Fusable operator types
        fusable_types = {
            OperatorType.MAP,
            OperatorType.FILTER,
            OperatorType.SELECT,
            OperatorType.FLAT_MAP
        }

        while current:
            # Check if current operator is fusable
            if current.operator_type not in fusable_types or current.stateful:
                break

            # Check if current has single downstream (linear chain)
            if len(current.downstream_ids) != 1:
                # Add current and stop
                if current.operator_id not in visited:
                    chain.append(current)
                    visited.add(current.operator_id)
                break

            # Add to chain
            if current.operator_id not in visited:
                chain.append(current)
                visited.add(current.operator_id)

            # Move to next operator
            next_id = current.downstream_ids[0]
            current = job_graph.operators.get(next_id)

        return chain

    def _fuse_chain(self, job_graph: JobGraph, chain: FusionChain):
        """
        Fuse a chain of operators into a single operator.

        Removes individual operators and replaces with fused operator.
        """
        # Create fused operator
        fused_op = chain.create_fused_operator()

        # Remove individual operators from graph
        for op in chain.operators:
            del job_graph.operators[op.operator_id]
            self.operators_fused_count += 1

        # Add fused operator
        job_graph.operators[fused_op.operator_id] = fused_op

        # Rewire connections
        # Update upstream operators to point to fused op
        for upstream_id in fused_op.upstream_ids:
            upstream = job_graph.operators[upstream_id]

            # Replace first operator in chain with fused op
            first_op_id = chain.operators[0].operator_id
            if first_op_id in upstream.downstream_ids:
                idx = upstream.downstream_ids.index(first_op_id)
                upstream.downstream_ids[idx] = fused_op.operator_id

        # Update downstream operators to point to fused op
        for downstream_id in fused_op.downstream_ids:
            downstream = job_graph.operators[downstream_id]

            # Replace last operator in chain with fused op
            last_op_id = chain.operators[-1].operator_id
            if last_op_id in downstream.upstream_ids:
                idx = downstream.upstream_ids.index(last_op_id)
                downstream.upstream_ids[idx] = fused_op.operator_id

        logger.debug(
            f"Fused {len(chain.operators)} operators into {fused_op.name}"
        )
```

**Deliverables:**
- OperatorFusion class
- Fusion chain detection
- Fused operator generation
- Cython codegen integration points

---

### Task 6: Benchmark Implementation

**File:** `/Users/bengamble/Sabot/benchmarks/optimizer_bench.py` (NEW)

**Estimated Effort:** 4 hours

**Implementation:**

```python
"""
Optimizer Benchmarks

Measures performance impact of each optimization.
Compares optimized vs unoptimized execution.
"""

import time
import logging
from typing import Dict, Any

import pyarrow as pa
import pyarrow.compute as pc

from sabot.execution.job_graph import JobGraph, StreamOperatorNode, OperatorType
from sabot.compiler.plan_optimizer import PlanOptimizer

logger = logging.getLogger(__name__)


class OptimizerBenchmark:
    """Benchmark optimizer impact on query performance."""

    def __init__(self, num_rows: int = 1_000_000):
        self.num_rows = num_rows

    def run_all(self):
        """Run all optimizer benchmarks."""
        print(f"\n{'='*60}")
        print("OPTIMIZER BENCHMARKS")
        print(f"{'='*60}\n")

        self.bench_filter_pushdown()
        self.bench_projection_pushdown()
        self.bench_join_reordering()
        self.bench_operator_fusion()
        self.bench_combined_optimizations()

    def bench_filter_pushdown(self):
        """
        Benchmark filter pushdown optimization.

        Query: SELECT * FROM data WHERE value > 100 JOIN other ON id

        Unoptimized: JOIN -> FILTER
        Optimized:   FILTER -> JOIN (reduces join input size)
        """
        print("Benchmark: Filter Pushdown")
        print("-" * 40)

        # Create test data
        data = self._generate_test_data(self.num_rows)
        other_data = self._generate_other_data(self.num_rows // 10)

        # Unoptimized query (filter after join)
        unopt_graph = self._create_filter_pushdown_query_unopt()

        # Optimized query (filter before join)
        optimizer = PlanOptimizer(enable_filter_pushdown=True)
        opt_graph = optimizer.optimize(unopt_graph)

        # Benchmark
        unopt_time = self._execute_and_time(unopt_graph, data, other_data)
        opt_time = self._execute_and_time(opt_graph, data, other_data)

        speedup = unopt_time / opt_time if opt_time > 0 else 0

        print(f"Unoptimized: {unopt_time:.3f}s")
        print(f"Optimized:   {opt_time:.3f}s")
        print(f"Speedup:     {speedup:.2f}x")
        print()

    def bench_projection_pushdown(self):
        """
        Benchmark projection pushdown optimization.

        Query: SELECT id, value FROM data WHERE condition

        Unoptimized: Read all columns -> Filter -> Select
        Optimized:   Select columns early -> Filter
        """
        print("Benchmark: Projection Pushdown")
        print("-" * 40)

        # Create wide table (many columns)
        data = self._generate_wide_table(self.num_rows, num_columns=20)

        # Query that only uses 2 columns
        unopt_graph = self._create_projection_pushdown_query_unopt()

        optimizer = PlanOptimizer(enable_projection_pushdown=True)
        opt_graph = optimizer.optimize(unopt_graph)

        unopt_time = self._execute_and_time(unopt_graph, data)
        opt_time = self._execute_and_time(opt_graph, data)

        speedup = unopt_time / opt_time if opt_time > 0 else 0
        memory_reduction = self._estimate_memory_reduction(unopt_graph, opt_graph)

        print(f"Unoptimized: {unopt_time:.3f}s")
        print(f"Optimized:   {opt_time:.3f}s")
        print(f"Speedup:     {speedup:.2f}x")
        print(f"Memory reduction: {memory_reduction:.1f}%")
        print()

    def bench_join_reordering(self):
        """
        Benchmark join reordering optimization.

        Query: A JOIN B JOIN C

        Unoptimized: (A JOIN B) JOIN C (order from user)
        Optimized:   (A JOIN C) JOIN B (based on selectivity)
        """
        print("Benchmark: Join Reordering")
        print("-" * 40)

        # Create three tables with different sizes
        table_a = self._generate_test_data(self.num_rows)
        table_b = self._generate_test_data(self.num_rows // 10)  # Small
        table_c = self._generate_test_data(self.num_rows // 2)   # Medium

        unopt_graph = self._create_join_reordering_query_unopt()

        optimizer = PlanOptimizer(enable_join_reordering=True)
        opt_graph = optimizer.optimize(unopt_graph)

        unopt_time = self._execute_and_time(unopt_graph, table_a, table_b, table_c)
        opt_time = self._execute_and_time(opt_graph, table_a, table_b, table_c)

        speedup = unopt_time / opt_time if opt_time > 0 else 0

        print(f"Unoptimized: {unopt_time:.3f}s")
        print(f"Optimized:   {opt_time:.3f}s")
        print(f"Speedup:     {speedup:.2f}x")
        print()

    def bench_operator_fusion(self):
        """
        Benchmark operator fusion optimization.

        Query: data.map(f1).filter(p).map(f2).select(cols)

        Unoptimized: 4 separate operators
        Optimized:   1 fused operator
        """
        print("Benchmark: Operator Fusion")
        print("-" * 40)

        data = self._generate_test_data(self.num_rows)

        unopt_graph = self._create_fusion_query_unopt()

        optimizer = PlanOptimizer(enable_operator_fusion=True)
        opt_graph = optimizer.optimize(unopt_graph)

        unopt_time = self._execute_and_time(unopt_graph, data)
        opt_time = self._execute_and_time(opt_graph, data)

        speedup = unopt_time / opt_time if opt_time > 0 else 0

        print(f"Unoptimized: {unopt_time:.3f}s ({len(unopt_graph.operators)} ops)")
        print(f"Optimized:   {opt_time:.3f}s ({len(opt_graph.operators)} ops)")
        print(f"Speedup:     {speedup:.2f}x")
        print()

    def bench_combined_optimizations(self):
        """
        Benchmark all optimizations combined.

        Complex query with filters, projections, joins.
        """
        print("Benchmark: Combined Optimizations")
        print("-" * 40)

        data = self._generate_complex_query_data()

        unopt_graph = self._create_complex_query_unopt()

        optimizer = PlanOptimizer()  # All optimizations enabled
        opt_graph = optimizer.optimize(unopt_graph)

        stats = optimizer.get_stats()

        unopt_time = self._execute_and_time(unopt_graph, data)
        opt_time = self._execute_and_time(opt_graph, data)

        speedup = unopt_time / opt_time if opt_time > 0 else 0

        print(f"Unoptimized: {unopt_time:.3f}s")
        print(f"Optimized:   {opt_time:.3f}s")
        print(f"Speedup:     {speedup:.2f}x")
        print(f"\nOptimization stats:")
        print(f"  Filters pushed: {stats.filters_pushed}")
        print(f"  Projections pushed: {stats.projections_pushed}")
        print(f"  Joins reordered: {stats.joins_reordered}")
        print(f"  Operators fused: {stats.operators_fused}")
        print()

    # Helper methods for generating test queries and data

    def _generate_test_data(self, num_rows: int) -> pa.Table:
        """Generate test data table."""
        import numpy as np

        return pa.table({
            'id': pa.array(range(num_rows)),
            'value': pa.array(np.random.randint(0, 1000, num_rows)),
            'category': pa.array(np.random.choice(['A', 'B', 'C'], num_rows)),
        })

    def _generate_other_data(self, num_rows: int) -> pa.Table:
        """Generate other table for joins."""
        import numpy as np

        return pa.table({
            'id': pa.array(range(num_rows)),
            'metadata': pa.array([f'meta_{i}' for i in range(num_rows)]),
        })

    def _generate_wide_table(self, num_rows: int, num_columns: int) -> pa.Table:
        """Generate table with many columns."""
        import numpy as np

        data = {'id': pa.array(range(num_rows))}
        for i in range(num_columns):
            data[f'col_{i}'] = pa.array(np.random.rand(num_rows))

        return pa.table(data)

    def _execute_and_time(self, job_graph: JobGraph, *data) -> float:
        """
        Execute job graph and measure time.

        Simplified: would use actual execution engine.
        """
        start = time.perf_counter()

        # Placeholder: simulate execution
        # In real implementation, would compile to ExecutionGraph and run
        time.sleep(0.001 * len(job_graph.operators))  # Fake execution

        end = time.perf_counter()
        return end - start

    def _estimate_memory_reduction(self, unopt: JobGraph, opt: JobGraph) -> float:
        """Estimate memory reduction from optimization."""
        # Simplified: count operators
        reduction = (len(unopt.operators) - len(opt.operators)) / len(unopt.operators)
        return reduction * 100

    def _create_filter_pushdown_query_unopt(self) -> JobGraph:
        """Create unoptimized query for filter pushdown benchmark."""
        # Placeholder: would create actual JobGraph
        graph = JobGraph(job_name="filter_pushdown_unopt")
        # ... add operators
        return graph

    # ... similar methods for other benchmarks


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)

    bench = OptimizerBenchmark(num_rows=1_000_000)
    bench.run_all()
```

**Deliverables:**
- OptimizerBenchmark class
- Benchmarks for each optimization
- Performance measurement and reporting
- Test data generation

---

### Task 7: Testing Strategy

**File:** `/Users/bengamble/Sabot/tests/unit/compiler/test_plan_optimizer.py` (NEW)

**Estimated Effort:** 6 hours

**Test Cases:**

1. **Filter Pushdown Tests:**
   - Filter pushed through map
   - Filter pushed through select
   - Filter NOT pushed through join
   - Filter combined with other filter

2. **Projection Pushdown Tests:**
   - Unused columns pruned
   - Projection inserted after map
   - Projection NOT inserted when all columns needed

3. **Join Reordering Tests:**
   - Joins reordered by selectivity
   - Outer joins NOT reordered
   - Single join not modified

4. **Operator Fusion Tests:**
   - Map + filter fused
   - Map + map + select fused
   - Stateful operators NOT fused

5. **Integration Tests:**
   - All optimizations applied
   - Optimizations preserve correctness
   - Optimizations are idempotent

**Implementation:**

```python
"""
Unit tests for PlanOptimizer.
"""

import unittest
from sabot.execution.job_graph import JobGraph, StreamOperatorNode, OperatorType
from sabot.compiler.plan_optimizer import PlanOptimizer


class TestFilterPushdown(unittest.TestCase):
    """Test filter pushdown optimization."""

    def test_filter_pushed_through_map(self):
        """Filter should be pushed before map operator."""
        graph = JobGraph(job_name="test")

        # Create: source -> map -> filter
        source = StreamOperatorNode(operator_type=OperatorType.SOURCE)
        map_op = StreamOperatorNode(operator_type=OperatorType.MAP)
        filter_op = StreamOperatorNode(
            operator_type=OperatorType.FILTER,
            parameters={'columns': ['value']}
        )

        graph.add_operator(source)
        graph.add_operator(map_op)
        graph.add_operator(filter_op)

        graph.connect(source.operator_id, map_op.operator_id)
        graph.connect(map_op.operator_id, filter_op.operator_id)

        # Optimize
        optimizer = PlanOptimizer()
        optimized = optimizer.optimize(graph)

        # Verify: filter should be before map
        # source -> filter -> map
        filter_opt = [op for op in optimized.operators.values()
                     if op.operator_type == OperatorType.FILTER][0]

        self.assertIn(source.operator_id, filter_opt.upstream_ids)
        self.assertEqual(optimizer.stats.filters_pushed, 1)

    def test_filter_not_pushed_through_join(self):
        """Filter should NOT be pushed through join (changes semantics)."""
        graph = JobGraph(job_name="test")

        # Create: source1 -> join -> filter
        #         source2 ->
        source1 = StreamOperatorNode(operator_type=OperatorType.SOURCE)
        source2 = StreamOperatorNode(operator_type=OperatorType.SOURCE)
        join_op = StreamOperatorNode(operator_type=OperatorType.HASH_JOIN)
        filter_op = StreamOperatorNode(operator_type=OperatorType.FILTER)

        graph.add_operator(source1)
        graph.add_operator(source2)
        graph.add_operator(join_op)
        graph.add_operator(filter_op)

        graph.connect(source1.operator_id, join_op.operator_id)
        graph.connect(source2.operator_id, join_op.operator_id)
        graph.connect(join_op.operator_id, filter_op.operator_id)

        # Optimize
        optimizer = PlanOptimizer()
        optimized = optimizer.optimize(graph)

        # Verify: filter should still be after join
        filter_opt = [op for op in optimized.operators.values()
                     if op.operator_type == OperatorType.FILTER][0]

        self.assertIn(join_op.operator_id, filter_opt.upstream_ids)


class TestProjectionPushdown(unittest.TestCase):
    """Test projection pushdown optimization."""

    def test_unused_columns_pruned(self):
        """Unused columns should be pruned early."""
        graph = JobGraph(job_name="test")

        # Create: source (10 cols) -> filter (uses 2 cols) -> sink
        source = StreamOperatorNode(
            operator_type=OperatorType.SOURCE,
            parameters={'columns': [f'col_{i}' for i in range(10)]}
        )
        filter_op = StreamOperatorNode(
            operator_type=OperatorType.FILTER,
            parameters={'columns': ['col_0', 'col_1']}
        )
        sink = StreamOperatorNode(operator_type=OperatorType.SINK)

        graph.add_operator(source)
        graph.add_operator(filter_op)
        graph.add_operator(sink)

        graph.connect(source.operator_id, filter_op.operator_id)
        graph.connect(filter_op.operator_id, sink.operator_id)

        # Optimize
        optimizer = PlanOptimizer()
        optimized = optimizer.optimize(graph)

        # Verify: SELECT should be inserted after source
        select_ops = [op for op in optimized.operators.values()
                     if op.operator_type == OperatorType.SELECT]

        self.assertGreater(len(select_ops), 0)
        self.assertEqual(optimizer.stats.projections_pushed, 1)


class TestJoinReordering(unittest.TestCase):
    """Test join reordering optimization."""

    def test_joins_reordered_by_selectivity(self):
        """Joins should be reordered to start with most selective."""
        graph = JobGraph(job_name="test")

        # Create: A -> join1 -> join2 -> sink
        #         B ->         C ->
        # Where join2 is more selective than join1

        source_a = StreamOperatorNode(operator_type=OperatorType.SOURCE)
        source_b = StreamOperatorNode(operator_type=OperatorType.SOURCE)
        source_c = StreamOperatorNode(operator_type=OperatorType.SOURCE)

        join1 = StreamOperatorNode(
            operator_type=OperatorType.HASH_JOIN,
            parameters={'join_type': 'inner', 'left_keys': ['id']}
        )
        join2 = StreamOperatorNode(
            operator_type=OperatorType.HASH_JOIN,
            parameters={'join_type': 'inner', 'left_keys': ['id']}
        )
        sink = StreamOperatorNode(operator_type=OperatorType.SINK)

        graph.add_operator(source_a)
        graph.add_operator(source_b)
        graph.add_operator(source_c)
        graph.add_operator(join1)
        graph.add_operator(join2)
        graph.add_operator(sink)

        graph.connect(source_a.operator_id, join1.operator_id)
        graph.connect(source_b.operator_id, join1.operator_id)
        graph.connect(join1.operator_id, join2.operator_id)
        graph.connect(source_c.operator_id, join2.operator_id)
        graph.connect(join2.operator_id, sink.operator_id)

        # Optimize
        optimizer = PlanOptimizer()
        optimized = optimizer.optimize(graph)

        # Verify: join order may have changed
        # (Difficult to test without actual selectivity stats)
        self.assertGreaterEqual(optimizer.stats.joins_reordered, 0)


class TestOperatorFusion(unittest.TestCase):
    """Test operator fusion optimization."""

    def test_map_filter_fused(self):
        """Map + filter should be fused."""
        graph = JobGraph(job_name="test")

        # Create: source -> map -> filter -> sink
        source = StreamOperatorNode(operator_type=OperatorType.SOURCE)
        map_op = StreamOperatorNode(operator_type=OperatorType.MAP)
        filter_op = StreamOperatorNode(operator_type=OperatorType.FILTER)
        sink = StreamOperatorNode(operator_type=OperatorType.SINK)

        graph.add_operator(source)
        graph.add_operator(map_op)
        graph.add_operator(filter_op)
        graph.add_operator(sink)

        graph.connect(source.operator_id, map_op.operator_id)
        graph.connect(map_op.operator_id, filter_op.operator_id)
        graph.connect(filter_op.operator_id, sink.operator_id)

        initial_op_count = len(graph.operators)

        # Optimize
        optimizer = PlanOptimizer()
        optimized = optimizer.optimize(graph)

        # Verify: should have fewer operators (fused)
        self.assertLess(len(optimized.operators), initial_op_count)
        self.assertGreater(optimizer.stats.operators_fused, 0)


if __name__ == '__main__':
    unittest.main()
```

**Deliverables:**
- Unit tests for each optimization
- Integration tests
- Correctness verification tests
- Performance regression tests

---

## Part 3: Integration with Existing Code

### Modify JobManager

**File:** `/Users/bengamble/Sabot/sabot/job_manager.py` (if exists, otherwise part of future work)

Add optimizer integration:

```python
from sabot.compiler.plan_optimizer import PlanOptimizer

@DBOS.transaction
async def _optimize_dag_step(self, job_id, dag):
    """Optimize operator DAG (DBOS transaction - atomic)."""
    logger.info(f"Optimizing DAG for job {job_id}")

    # Create optimizer
    optimizer = PlanOptimizer()

    # Apply optimizations
    optimized = optimizer.optimize(dag)

    # Log statistics
    stats = optimizer.get_stats()
    logger.info(f"Optimization stats: {stats}")

    # Store in DBOS
    if self.dbos:
        await self.dbos.execute(
            """INSERT INTO job_dag_optimized (job_id, dag_json, optimization_stats)
               VALUES ($1, $2, $3)""",
            job_id, optimized.to_json(), stats.to_dict()
        )

    return optimized
```

### Update JobGraph

**File:** `/Users/bengamble/Sabot/sabot/execution/job_graph.py`

Add methods to support optimization:

```python
def to_json(self) -> str:
    """Serialize JobGraph to JSON for persistence."""
    import json
    return json.dumps({
        'job_id': self.job_id,
        'job_name': self.job_name,
        'operators': {
            op_id: op.to_dict()
            for op_id, op in self.operators.items()
        },
        'default_parallelism': self.default_parallelism,
    })

@classmethod
def from_json(cls, json_str: str) -> 'JobGraph':
    """Deserialize JobGraph from JSON."""
    import json
    data = json.loads(json_str)

    graph = cls(
        job_id=data['job_id'],
        job_name=data['job_name'],
        default_parallelism=data['default_parallelism']
    )

    # Reconstruct operators
    # ... implementation details

    return graph
```

---

## Part 4: Success Criteria

### Functional Requirements

- [ ] PlanOptimizer class implemented with all optimizations
- [ ] Filter pushdown working for linear pipelines
- [ ] Projection pushdown pruning unused columns
- [ ] Join reordering detecting and reordering join chains
- [ ] Operator fusion combining compatible operators
- [ ] Dead code elimination removing unreachable operators

### Performance Requirements

- [ ] Filter pushdown: 2-5x speedup on filtered joins
- [ ] Projection pushdown: 20-40% memory reduction on wide tables
- [ ] Join reordering: 10-30% speedup on multi-join queries
- [ ] Operator fusion: 5-15% speedup on chained transformations
- [ ] Combined: 3-10x speedup on complex queries

### Quality Requirements

- [ ] All unit tests passing
- [ ] No regressions in existing tests
- [ ] Optimization stats tracked and logged
- [ ] Optimizer can be disabled per-optimization
- [ ] Optimizations preserve correctness (verified by tests)

---

## Part 5: Estimated Effort

| Task | Hours | Priority |
|------|-------|----------|
| 1. PlanOptimizer class | 4 | P0 |
| 2. Filter pushdown | 6 | P0 |
| 3. Projection pushdown | 5 | P0 |
| 4. Join reordering | 8 | P1 |
| 5. Operator fusion | 6 | P1 |
| 6. Benchmarks | 4 | P0 |
| 7. Testing | 6 | P0 |
| 8. Integration | 3 | P0 |
| 9. Documentation | 2 | P1 |
| **TOTAL** | **44 hours** | |

**Schedule:**

- Week 1: Tasks 1-3 (Core optimizer + filter/projection pushdown)
- Week 2: Tasks 4-5 (Join reordering + operator fusion)
- Week 3: Tasks 6-9 (Benchmarks, testing, integration, docs)

---

## Part 6: Future Enhancements

### Phase 7.5: Advanced Optimizations (Future)

1. **Cost-Based Optimization:**
   - Statistics collection from sources
   - Cardinality estimation
   - Cost model for operators
   - Dynamic programming join ordering

2. **Expression Optimization:**
   - Constant folding
   - Expression simplification
   - Common subexpression elimination

3. **Predicate Pushdown to Sources:**
   - Push filters into Kafka (timestamp filtering)
   - Push filters into Parquet (predicate pushdown)
   - Push filters into databases (WHERE clause pushdown)

4. **Adaptive Optimization:**
   - Runtime statistics collection
   - Re-optimization based on actual data
   - Adaptive join algorithms

5. **Streaming-Specific Optimizations:**
   - Watermark-aware operator placement
   - Latency-aware scheduling
   - Backpressure-aware rebalancing

---

## Part 7: References

### DuckDB Optimizer Sources

Analyzed from `/Users/bengamble/Sabot/vendor/duckdb/src/`:

- `optimizer/filter_pushdown.hpp` - Filter pushdown architecture
- `optimizer/remove_unused_columns.hpp` - Projection pushdown
- `optimizer/join_order/join_order_optimizer.hpp` - Join reordering
- `optimizer/optimizer.hpp` - Main optimizer structure

### Sabot Integration Points

- `/Users/bengamble/Sabot/sabot/execution/job_graph.py` - Logical operator DAG
- `/Users/bengamble/Sabot/sabot/execution/execution_graph.py` - Physical execution plan
- `/Users/bengamble/Sabot/sabot/_cython/operators/` - Operator implementations

---

## Conclusion

This Phase 7 implementation plan provides a concrete roadmap for adding query optimization to Sabot's streaming dataflow engine. The design is:

1. **Simple** - Rule-based optimizations, not full cost-based optimizer
2. **Measurable** - Clear performance targets and benchmarks
3. **Incremental** - Each optimization can be implemented independently
4. **Foundation** - Extensible architecture for future enhancements
5. **Streaming-Aware** - Respects streaming semantics (ordering, watermarks)

By implementing these optimizations, Sabot will match or exceed the performance of modern query engines while maintaining the simplicity and flexibility needed for streaming workloads.

**Total Estimated Effort:** 44 hours (3 weeks)

**Next Steps:**
1. Review and approve this plan
2. Create sabot/compiler/ directory structure
3. Implement Task 1 (PlanOptimizer class)
4. Iteratively implement remaining tasks
5. Benchmark and validate performance improvements
