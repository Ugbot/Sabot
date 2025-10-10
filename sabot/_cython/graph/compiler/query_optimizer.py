"""
Query Optimizer Base Class

Unified optimization framework for SPARQL and Cypher queries.
Applies rule-based and cost-based optimizations to query plans.

Architecture:
1. Logical plan construction from AST
2. Apply optimization rules (filter pushdown, join reordering, etc.)
3. Cost estimation and cardinality propagation
4. Physical plan selection

Optimizations:
- Filter pushdown
- Projection pushdown
- Join reordering (cost-based)
- Predicate pushdown to storage
- Common subexpression elimination

Inspired by:
- Apache Calcite optimizer
- DuckDB logical optimizer
- PostgreSQL query planner
"""

from typing import List, Optional, Dict, Any, Callable
from dataclasses import dataclass
import logging

from .query_plan import (
    LogicalPlan, PhysicalPlan, PlanNode, NodeType,
    CostEstimate, PlanBuilder,
    ScanNode, FilterNode, JoinNode, ProjectNode, LimitNode
)
from .statistics import GraphStatistics, StatisticsCollector

logger = logging.getLogger(__name__)


# ==============================================================================
# Optimization Rules
# ==============================================================================

class OptimizationRule:
    """
    Base class for optimization rules.

    A rule transforms a plan node into an optimized equivalent.
    """

    def __init__(self, name: str):
        """
        Initialize optimization rule.

        Args:
            name: Rule name for logging
        """
        self.name = name

    def apply(self, node: PlanNode, context: 'OptimizationContext') -> Optional[PlanNode]:
        """
        Apply rule to a plan node.

        Args:
            node: Plan node to optimize
            context: Optimization context with statistics

        Returns:
            Optimized node, or None if rule doesn't apply
        """
        raise NotImplementedError

    def __repr__(self) -> str:
        return f"Rule({self.name})"


# ==============================================================================
# Optimization Context
# ==============================================================================

@dataclass
class OptimizationContext:
    """
    Context for query optimization.

    Contains:
    - Graph statistics
    - Optimization goals (minimize cost, latency, etc.)
    - Configuration options
    """
    statistics: GraphStatistics
    enable_filter_pushdown: bool = True
    enable_projection_pushdown: bool = True
    enable_join_reordering: bool = True
    enable_predicate_pushdown: bool = True
    enable_cse: bool = True  # Common subexpression elimination

    # Cost model weights
    cpu_weight: float = 1.0
    memory_weight: float = 0.5
    io_weight: float = 10.0


# ==============================================================================
# Query Optimizer
# ==============================================================================

class QueryOptimizer:
    """
    Main query optimizer.

    Orchestrates optimization rules and cost estimation.

    Usage:
        optimizer = QueryOptimizer(statistics)
        optimized_plan = optimizer.optimize(logical_plan)
    """

    def __init__(
        self,
        statistics: GraphStatistics,
        rules: Optional[List[OptimizationRule]] = None
    ):
        """
        Initialize query optimizer.

        Args:
            statistics: Graph statistics for cost estimation
            rules: Optimization rules to apply (None = use defaults)
        """
        self.statistics = statistics
        self.context = OptimizationContext(statistics=statistics)

        # Register optimization rules
        if rules is None:
            self.rules = self._get_default_rules()
        else:
            self.rules = rules

        logger.info(f"Initialized optimizer with {len(self.rules)} rules")

    def _get_default_rules(self) -> List[OptimizationRule]:
        """Get default optimization rules."""
        from .optimization_rules import (
            FilterPushdownRule,
            ProjectionPushdownRule,
            PredicatePushdownRule,
            JoinReorderingRule
        )

        return [
            FilterPushdownRule(),
            ProjectionPushdownRule(),
            PredicatePushdownRule(),
            JoinReorderingRule(),
        ]

    def optimize(self, plan: LogicalPlan) -> LogicalPlan:
        """
        Optimize logical plan.

        Args:
            plan: Input logical plan

        Returns:
            Optimized logical plan

        Optimization stages:
        1. Apply rule-based optimizations (bottom-up)
        2. Estimate costs
        3. Select best plan variant
        """
        logger.info("Starting query optimization")

        # Stage 1: Apply optimization rules
        optimized_root = self._apply_rules(plan.root)

        # Stage 2: Propagate costs bottom-up
        self._estimate_costs(optimized_root)

        # Create optimized plan
        optimized_plan = LogicalPlan(
            root=optimized_root,
            statistics=self.statistics
        )

        logger.info(f"Optimization complete: {optimized_plan.get_estimated_cost()}")
        return optimized_plan

    def _apply_rules(self, node: PlanNode) -> PlanNode:
        """
        Apply optimization rules to plan node.

        Args:
            node: Plan node to optimize

        Returns:
            Optimized plan node

        Algorithm:
            Bottom-up traversal, applying rules at each level.
            Rules are applied until fixpoint (no more changes).
        """
        # First optimize children recursively
        optimized_children = []
        for child in node.children:
            optimized_child = self._apply_rules(child)
            optimized_children.append(optimized_child)

        # Update node with optimized children
        node.children = optimized_children

        # Apply rules to current node until fixpoint
        changed = True
        iterations = 0
        max_iterations = 10  # Prevent infinite loops

        while changed and iterations < max_iterations:
            changed = False
            iterations += 1

            for rule in self.rules:
                optimized = rule.apply(node, self.context)
                if optimized is not None and optimized != node:
                    logger.debug(f"Applied {rule.name} to {node.node_type.value}")
                    node = optimized
                    changed = True
                    break  # Re-apply all rules after a change

        return node

    def _estimate_costs(self, node: PlanNode) -> CostEstimate:
        """
        Estimate costs for plan node and propagate bottom-up.

        Args:
            node: Plan node

        Returns:
            Cost estimate

        Algorithm:
            1. Recursively estimate costs for children
            2. Compute cost for current node based on:
               - Node type (scan, join, filter, etc.)
               - Input cardinalities
               - Selectivity estimates
        """
        # Base case: leaf nodes
        if not node.children:
            cost = self._estimate_leaf_cost(node)
            node.cost = cost
            return cost

        # Recursive case: estimate children first
        child_costs = []
        for child in node.children:
            child_cost = self._estimate_costs(child)
            child_costs.append(child_cost)

        # Estimate current node cost based on type
        if node.node_type == NodeType.FILTER:
            cost = self._estimate_filter_cost(node, child_costs[0])
        elif node.node_type in [NodeType.HASH_JOIN, NodeType.NESTED_LOOP_JOIN]:
            cost = self._estimate_join_cost(node, child_costs)
        elif node.node_type == NodeType.PROJECT:
            cost = self._estimate_project_cost(node, child_costs[0])
        elif node.node_type == NodeType.LIMIT:
            cost = self._estimate_limit_cost(node, child_costs[0])
        else:
            # Default: propagate child cost
            cost = child_costs[0] if child_costs else CostEstimate()

        node.cost = cost
        return cost

    def _estimate_leaf_cost(self, node: PlanNode) -> CostEstimate:
        """Estimate cost for leaf node (scan)."""
        if node.node_type == NodeType.SCAN_VERTICES:
            table_name = node.properties.get('table_name', '')
            # TODO: Use actual statistics
            rows = self.statistics.total_vertices
            cpu_cost = rows * 1.0  # 1 CPU op per row
            memory_cost = rows * 100  # ~100 bytes per vertex
            return CostEstimate(rows=rows, cpu_cost=cpu_cost, memory_cost=memory_cost)

        elif node.node_type == NodeType.SCAN_EDGES:
            rows = self.statistics.total_edges
            cpu_cost = rows * 1.0
            memory_cost = rows * 50  # ~50 bytes per edge
            return CostEstimate(rows=rows, cpu_cost=cpu_cost, memory_cost=memory_cost)

        elif node.node_type == NodeType.SCAN_TRIPLES:
            rows = self.statistics.total_triples
            cpu_cost = rows * 1.0
            memory_cost = rows * 80  # ~80 bytes per triple
            return CostEstimate(rows=rows, cpu_cost=cpu_cost, memory_cost=memory_cost)

        else:
            return CostEstimate(rows=0, cpu_cost=0)

    def _estimate_filter_cost(
        self,
        node: FilterNode,
        child_cost: CostEstimate
    ) -> CostEstimate:
        """Estimate cost for filter operation."""
        selectivity = node.properties.get('selectivity', 0.5)
        output_rows = int(child_cost.rows * selectivity)

        # CPU cost: evaluate filter on each input row
        filter_cpu_cost = child_cost.rows * 2.0  # 2 CPU ops per row (eval + copy)

        return CostEstimate(
            rows=output_rows,
            cpu_cost=child_cost.cpu_cost + filter_cpu_cost,
            memory_cost=child_cost.memory_cost,
            io_cost=child_cost.io_cost
        )

    def _estimate_join_cost(
        self,
        node: JoinNode,
        child_costs: List[CostEstimate]
    ) -> CostEstimate:
        """Estimate cost for join operation."""
        left_cost = child_costs[0]
        right_cost = child_costs[1]

        if node.node_type == NodeType.HASH_JOIN:
            # Hash join: O(|left| + |right|)
            # Build hash table on right (smaller side)
            build_cost = right_cost.rows * 5.0  # Hash + insert
            probe_cost = left_cost.rows * 3.0   # Hash + probe

            # Estimate output cardinality
            join_selectivity = self._estimate_join_selectivity_from_node(node)
            output_rows = int(left_cost.rows * right_cost.rows * join_selectivity)

            cpu_cost = (
                left_cost.cpu_cost +
                right_cost.cpu_cost +
                build_cost +
                probe_cost
            )

            # Memory: hash table for build side + output
            memory_cost = (
                right_cost.rows * 100 +  # Hash table
                output_rows * 150        # Output rows
            )

            return CostEstimate(
                rows=output_rows,
                cpu_cost=cpu_cost,
                memory_cost=memory_cost,
                io_cost=left_cost.io_cost + right_cost.io_cost
            )

        else:  # Nested loop join
            # O(|left| * |right|)
            join_cost = left_cost.rows * right_cost.rows * 1.0

            join_selectivity = self._estimate_join_selectivity_from_node(node)
            output_rows = int(left_cost.rows * right_cost.rows * join_selectivity)

            return CostEstimate(
                rows=output_rows,
                cpu_cost=left_cost.cpu_cost + right_cost.cpu_cost + join_cost,
                memory_cost=max(left_cost.memory_cost, right_cost.memory_cost),
                io_cost=left_cost.io_cost + right_cost.io_cost
            )

    def _estimate_project_cost(
        self,
        node: ProjectNode,
        child_cost: CostEstimate
    ) -> CostEstimate:
        """Estimate cost for projection operation."""
        # Projection is cheap - just column selection
        columns = node.properties.get('columns', [])
        reduction_factor = len(columns) / 10.0 if columns else 1.0  # Assume 10 cols baseline

        return CostEstimate(
            rows=child_cost.rows,
            cpu_cost=child_cost.cpu_cost + child_cost.rows * 0.1,
            memory_cost=int(child_cost.memory_cost * reduction_factor),
            io_cost=child_cost.io_cost
        )

    def _estimate_limit_cost(
        self,
        node: LimitNode,
        child_cost: CostEstimate
    ) -> CostEstimate:
        """Estimate cost for limit operation."""
        limit = node.properties.get('limit', child_cost.rows)
        offset = node.properties.get('offset', 0)

        output_rows = min(limit, max(0, child_cost.rows - offset))

        # LIMIT can short-circuit execution
        # Assume we only compute enough to satisfy LIMIT
        execution_rows = offset + limit

        return CostEstimate(
            rows=output_rows,
            cpu_cost=min(child_cost.cpu_cost, execution_rows * 1.0),
            memory_cost=output_rows * 100,
            io_cost=child_cost.io_cost
        )

    def _estimate_join_selectivity_from_node(self, node: JoinNode) -> float:
        """Estimate join selectivity from join node properties."""
        left_keys = node.properties.get('left_keys', [])
        right_keys = node.properties.get('right_keys', [])

        if left_keys and right_keys:
            # Use statistics to estimate selectivity
            return self.statistics.estimate_join_selectivity(
                'left',
                'right',
                (left_keys[0], right_keys[0])
            )
        else:
            return 0.1  # Default 10% selectivity

    def to_physical_plan(self, logical_plan: LogicalPlan) -> PhysicalPlan:
        """
        Convert logical plan to physical plan.

        Args:
            logical_plan: Optimized logical plan

        Returns:
            Physical plan ready for execution

        Physical plan selection:
        - Choose join algorithms (hash join vs nested loop)
        - Select access methods (index scan vs table scan)
        - Add exchange operators for distributed execution
        """
        # For now, logical plan = physical plan
        # Future: add operator selection logic
        return PhysicalPlan(
            root=logical_plan.root,
            statistics=logical_plan.statistics
        )
