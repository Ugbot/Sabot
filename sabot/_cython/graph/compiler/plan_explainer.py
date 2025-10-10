"""
Query Plan Explainer

Generates human-readable explanations of query execution plans,
showing optimizations applied, estimated costs, and execution strategy.

Used by EXPLAIN statement support in SPARQL and Cypher.
"""

from typing import List, Optional, Dict, Any
from dataclasses import dataclass

from .query_plan import LogicalPlan, PhysicalPlan, PlanNode, NodeType, CostEstimate
from .statistics import GraphStatistics


@dataclass
class OptimizationInfo:
    """Information about an optimization that was applied."""
    rule_name: str
    description: str
    impact: str  # e.g., "2-5x speedup expected"


@dataclass
class ExplanationResult:
    """
    Result of query plan explanation.

    Contains human-readable plan, statistics, and optimization details.
    """
    query: str
    language: str  # 'sparql' or 'cypher'
    plan_tree: str
    estimated_cost: CostEstimate
    optimizations_applied: List[OptimizationInfo]
    statistics: Optional[GraphStatistics] = None

    def to_string(self) -> str:
        """Generate formatted explanation string."""
        lines = []
        lines.append("=" * 80)
        lines.append(f"QUERY EXECUTION PLAN ({self.language.upper()})")
        lines.append("=" * 80)
        lines.append("")
        lines.append("Query:")
        for line in self.query.strip().split('\n'):
            lines.append(f"  {line}")
        lines.append("")
        lines.append("-" * 80)
        lines.append("Execution Plan:")
        lines.append("-" * 80)
        lines.append(self.plan_tree)
        lines.append("")
        lines.append("-" * 80)
        lines.append("Cost Estimates:")
        lines.append("-" * 80)
        lines.append(f"  Estimated rows: {self.estimated_cost.rows:,}")
        lines.append(f"  CPU cost: {self.estimated_cost.cpu_cost:,.2f}")
        lines.append(f"  Memory cost: {self.estimated_cost.memory_cost:,} bytes ({self.estimated_cost.memory_cost / 1024 / 1024:.2f} MB)")
        lines.append(f"  I/O cost: {self.estimated_cost.io_cost:,.2f}")
        lines.append(f"  Total cost: {self.estimated_cost.total_cost:,.2f}")
        lines.append("")

        if self.optimizations_applied:
            lines.append("-" * 80)
            lines.append("Optimizations Applied:")
            lines.append("-" * 80)
            for i, opt in enumerate(self.optimizations_applied, 1):
                lines.append(f"{i}. {opt.rule_name}")
                lines.append(f"   Description: {opt.description}")
                lines.append(f"   Impact: {opt.impact}")
                lines.append("")

        if self.statistics:
            lines.append("-" * 80)
            lines.append("Graph Statistics:")
            lines.append("-" * 80)
            lines.append(f"  Total vertices: {self.statistics.total_vertices:,}")
            lines.append(f"  Total edges: {self.statistics.total_edges:,}")
            lines.append(f"  Total triples: {self.statistics.total_triples:,}")
            lines.append(f"  Average degree: {self.statistics.avg_degree:.2f}")
            if self.statistics.vertex_label_counts:
                lines.append(f"  Vertex labels: {len(self.statistics.vertex_label_counts)}")
            if self.statistics.edge_type_counts:
                lines.append(f"  Edge types: {len(self.statistics.edge_type_counts)}")
            lines.append("")

        lines.append("=" * 80)
        return '\n'.join(lines)


class PlanExplainer:
    """
    Explains query execution plans in human-readable format.

    Generates tree-style plan representations with cost annotations.
    """

    def __init__(self, statistics: Optional[GraphStatistics] = None):
        """
        Initialize plan explainer.

        Args:
            statistics: Graph statistics for context
        """
        self.statistics = statistics

    def explain_plan(
        self,
        plan: LogicalPlan,
        query: str,
        language: str,
        optimizations_applied: Optional[List[OptimizationInfo]] = None
    ) -> ExplanationResult:
        """
        Generate explanation for a logical plan.

        Args:
            plan: Logical plan to explain
            query: Original query string
            language: Query language ('sparql' or 'cypher')
            optimizations_applied: List of optimizations that were applied

        Returns:
            ExplanationResult with formatted explanation
        """
        # Generate plan tree representation
        plan_tree = self._format_plan_tree(plan.root)

        # Get cost estimate
        cost = plan.root.cost if plan.root.cost else CostEstimate()

        return ExplanationResult(
            query=query,
            language=language,
            plan_tree=plan_tree,
            estimated_cost=cost,
            optimizations_applied=optimizations_applied or [],
            statistics=self.statistics or plan.statistics
        )

    def _format_plan_tree(self, node: PlanNode, indent: int = 0) -> str:
        """
        Format plan node as tree structure.

        Args:
            node: Plan node to format
            indent: Current indentation level

        Returns:
            Formatted tree string
        """
        lines = []
        prefix = "  " * indent

        # Node header with type
        node_type = self._format_node_type(node.node_type)
        lines.append(f"{prefix}├─ {node_type}")

        # Node properties
        if node.properties:
            important_props = self._get_important_properties(node)
            for key, value in important_props.items():
                lines.append(f"{prefix}│  {key}: {value}")

        # Cost information
        if node.cost:
            cost_str = self._format_cost(node.cost)
            lines.append(f"{prefix}│  {cost_str}")

        # Format children
        for i, child in enumerate(node.children):
            is_last = (i == len(node.children) - 1)
            child_str = self._format_plan_tree(child, indent + 1)
            lines.append(child_str)

        return '\n'.join(lines)

    def _format_node_type(self, node_type: NodeType) -> str:
        """Format node type for display."""
        type_map = {
            NodeType.SCAN_VERTICES: "SCAN Vertices",
            NodeType.SCAN_EDGES: "SCAN Edges",
            NodeType.SCAN_TRIPLES: "SCAN Triples",
            NodeType.FILTER: "FILTER",
            NodeType.HASH_JOIN: "HASH JOIN",
            NodeType.NESTED_LOOP_JOIN: "NESTED LOOP JOIN",
            NodeType.PROJECT: "PROJECT",
            NodeType.LIMIT: "LIMIT",
        }
        return type_map.get(node_type, node_type.value.upper())

    def _get_important_properties(self, node: PlanNode) -> Dict[str, Any]:
        """Extract important properties for display."""
        important = {}

        # Table name for scans
        if 'table_name' in node.properties and node.properties['table_name']:
            important['table'] = node.properties['table_name']

        # Filters
        if 'filters' in node.properties and node.properties['filters']:
            filters = node.properties['filters']
            if len(filters) == 1:
                important['filter'] = filters[0]
            else:
                important['filters'] = f"{len(filters)} filters"

        # Projection columns
        if 'columns' in node.properties and node.properties['columns']:
            cols = node.properties['columns']
            if len(cols) <= 3:
                important['columns'] = ', '.join(cols)
            else:
                important['columns'] = f"{len(cols)} columns"

        # Join keys
        if 'left_keys' in node.properties:
            important['join_keys'] = f"{node.properties['left_keys']} = {node.properties['right_keys']}"

        # Selectivity
        if 'selectivity' in node.properties:
            important['selectivity'] = f"{node.properties['selectivity']:.2%}"

        # Limit/Offset
        if 'limit' in node.properties:
            important['limit'] = node.properties['limit']
        if 'offset' in node.properties:
            important['offset'] = node.properties['offset']

        return important

    def _format_cost(self, cost: CostEstimate) -> str:
        """Format cost estimate for display."""
        parts = []
        parts.append(f"rows={cost.rows:,}")
        parts.append(f"cpu={cost.cpu_cost:,.0f}")
        parts.append(f"mem={cost.memory_cost / 1024:.0f}KB")
        parts.append(f"total={cost.total_cost:,.0f}")
        return f"cost: {', '.join(parts)}"


def create_simple_explanation(
    query: str,
    language: str,
    pattern_info: str,
    selectivity_scores: Optional[Dict[str, float]] = None,
    statistics: Optional[GraphStatistics] = None
) -> ExplanationResult:
    """
    Create a simple explanation without full plan tree.

    Used for SPARQL/Cypher when we don't have a full logical plan,
    but want to show pattern reordering information.

    Args:
        query: Original query string
        language: Query language
        pattern_info: Description of pattern execution order
        selectivity_scores: Selectivity scores for patterns
        statistics: Graph statistics

    Returns:
        ExplanationResult with simplified explanation
    """
    # Create a simple cost estimate
    cost = CostEstimate(
        rows=1000,  # Placeholder
        cpu_cost=1000.0,
        memory_cost=1024 * 1024,  # 1MB
        io_cost=100.0
    )

    # Build plan tree string
    lines = []
    lines.append("Pattern Execution Order (optimized):")
    lines.append("")
    lines.append(pattern_info)

    if selectivity_scores:
        lines.append("")
        lines.append("Selectivity Scores:")
        for pattern, score in selectivity_scores.items():
            lines.append(f"  {pattern}: {score:.2f}")

    plan_tree = '\n'.join(lines)

    # Identify optimizations
    optimizations = []
    if selectivity_scores:
        optimizations.append(OptimizationInfo(
            rule_name="Pattern Reordering",
            description="Reordered patterns by selectivity (most selective first)",
            impact="2-10x speedup on multi-pattern queries"
        ))

    return ExplanationResult(
        query=query,
        language=language,
        plan_tree=plan_tree,
        estimated_cost=cost,
        optimizations_applied=optimizations,
        statistics=statistics
    )
