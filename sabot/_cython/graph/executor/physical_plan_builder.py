"""
Physical Plan Builder

Converts LogicalPlan to PhysicalPlan by assigning concrete operators.
Annotates operators with stateful/stateless classification and partition keys.

Strategy:
- Scan nodes → PatternScanOperator (stateless)
- Filter nodes → GraphFilterOperator (stateless)
- Project nodes → GraphProjectOperator (stateless)
- Limit nodes → GraphLimitOperator (stateless)
- Join nodes → GraphJoinOperator (stateful, requires shuffle)
- Aggregate nodes → GraphAggregateOperator (stateful, requires shuffle)
"""

from typing import Optional, List, Dict, Any
from dataclasses import dataclass

from ..compiler.query_plan import (
    LogicalPlan,
    PhysicalPlan,
    PlanNode,
    NodeType,
    ScanNode,
    FilterNode,
    ProjectNode,
    LimitNode,
    JoinNode,
)

try:
    from .query_operators import (
        PatternScanOperator,
        GraphFilterOperator,
        GraphProjectOperator,
        GraphLimitOperator,
        GraphJoinOperator,
        GraphAggregateOperator,
    )
except ImportError:
    # Fallback for import issues
    PatternScanOperator = None
    GraphFilterOperator = None
    GraphProjectOperator = None
    GraphLimitOperator = None
    GraphJoinOperator = None
    GraphAggregateOperator = None


class PhysicalPlanBuilder:
    """
    Convert logical to physical query plans.

    Assigns concrete operators to plan nodes and annotates with
    execution metadata (stateful/stateless, partition keys).
    """

    def __init__(self, graph_engine, distributed=False):
        """
        Initialize physical plan builder.

        Args:
            graph_engine: GraphQueryEngine instance
            distributed: Enable distributed execution (default False)
        """
        self.graph_engine = graph_engine
        self.distributed = distributed

    def build(self, logical_plan: LogicalPlan) -> PhysicalPlan:
        """
        Convert logical plan to executable physical plan.

        Args:
            logical_plan: Logical query plan

        Returns:
            PhysicalPlan with assigned operators
        """
        # Visit plan nodes and assign operators
        physical_root = self._visit_node(logical_plan.root)

        # Create physical plan
        physical_plan = PhysicalPlan(
            root=physical_root,
            statistics=logical_plan.statistics
        )

        return physical_plan

    def _visit_node(self, node: PlanNode) -> PlanNode:
        """
        Visit plan node and assign operator.

        Recursively visits children and builds operator chain.

        Args:
            node: Logical plan node

        Returns:
            Physical plan node with operator assignment
        """
        # Visit children first (bottom-up)
        physical_children = []
        for child in node.children:
            physical_child = self._visit_node(child)
            physical_children.append(physical_child)

        # Assign operator based on node type
        if node.node_type == NodeType.SCAN_VERTICES or \
           node.node_type == NodeType.SCAN_EDGES or \
           node.node_type == NodeType.PATTERN_MATCH:
            return self._assign_scan_operator(node, physical_children)

        elif node.node_type == NodeType.FILTER:
            return self._assign_filter_operator(node, physical_children)

        elif node.node_type == NodeType.PROJECT:
            return self._assign_project_operator(node, physical_children)

        elif node.node_type == NodeType.LIMIT:
            return self._assign_limit_operator(node, physical_children)

        elif node.node_type == NodeType.HASH_JOIN or \
             node.node_type == NodeType.NESTED_LOOP_JOIN:
            return self._assign_join_operator(node, physical_children)

        elif node.node_type == NodeType.AGGREGATE:
            return self._assign_aggregate_operator(node, physical_children)

        else:
            # Unknown node type - return as-is
            node.children = physical_children
            return node

    def _assign_scan_operator(
        self,
        node: PlanNode,
        children: List[PlanNode]
    ) -> PlanNode:
        """
        Assign PatternScanOperator to scan node.

        Stateless operator - no shuffle required.

        Args:
            node: Scan node from logical plan
            children: Physical children (should be empty)

        Returns:
            Node with PatternScanOperator assigned
        """
        # Extract pattern from node properties
        pattern = node.properties.get('pattern')

        # Store operator in properties
        if PatternScanOperator is not None:
            operator = PatternScanOperator(
                pattern=pattern,
                graph=self.graph_engine
            )
            node.properties['operator'] = operator
            node.properties['operator_type'] = 'PatternScanOperator'
            node.properties['stateful'] = False

        return node

    def _assign_filter_operator(
        self,
        node: FilterNode,
        children: List[PlanNode]
    ) -> PlanNode:
        """
        Assign GraphFilterOperator to filter node.

        Stateless operator - no shuffle required.

        Args:
            node: Filter node from logical plan
            children: Physical children (should be 1)

        Returns:
            Node with GraphFilterOperator assigned
        """
        # Extract filter condition
        condition = node.properties.get('condition')

        # Create operator
        if GraphFilterOperator is not None:
            operator = GraphFilterOperator(filter_expr=condition)
            node.properties['operator'] = operator
            node.properties['operator_type'] = 'GraphFilterOperator'
            node.properties['stateful'] = False

        node.children = children
        return node

    def _assign_project_operator(
        self,
        node: ProjectNode,
        children: List[PlanNode]
    ) -> PlanNode:
        """
        Assign GraphProjectOperator to project node.

        Stateless operator - no shuffle required.

        Args:
            node: Project node from logical plan
            children: Physical children (should be 1)

        Returns:
            Node with GraphProjectOperator assigned
        """
        # Extract columns
        columns = node.properties.get('columns', [])

        # Create operator
        if GraphProjectOperator is not None:
            operator = GraphProjectOperator(columns=columns)
            node.properties['operator'] = operator
            node.properties['operator_type'] = 'GraphProjectOperator'
            node.properties['stateful'] = False

        node.children = children
        return node

    def _assign_limit_operator(
        self,
        node: LimitNode,
        children: List[PlanNode]
    ) -> PlanNode:
        """
        Assign GraphLimitOperator to limit node.

        Stateless operator - no shuffle required.

        Args:
            node: Limit node from logical plan
            children: Physical children (should be 1)

        Returns:
            Node with GraphLimitOperator assigned
        """
        # Extract limit and offset
        limit = node.properties.get('limit')
        offset = node.properties.get('offset')

        # Create operator
        if GraphLimitOperator is not None:
            operator = GraphLimitOperator(limit=limit, offset=offset)
            node.properties['operator'] = operator
            node.properties['operator_type'] = 'GraphLimitOperator'
            node.properties['stateful'] = False

        node.children = children
        return node

    def _assign_join_operator(
        self,
        node: JoinNode,
        children: List[PlanNode]
    ) -> PlanNode:
        """
        Assign GraphJoinOperator to join node.

        Stateful operator - requires shuffle by join keys.

        Args:
            node: Join node from logical plan
            children: Physical children (should be 2)

        Returns:
            Node with GraphJoinOperator assigned
        """
        # Extract join keys
        left_keys = node.properties.get('left_keys', [])
        right_keys = node.properties.get('right_keys', [])
        join_type = node.properties.get('join_type', 'inner')

        # Create operator
        if GraphJoinOperator is not None:
            operator = GraphJoinOperator(
                left_keys=left_keys,
                right_keys=right_keys,
                join_type=join_type
            )
            node.properties['operator'] = operator
            node.properties['operator_type'] = 'GraphJoinOperator'
            node.properties['stateful'] = True
            node.properties['partition_keys'] = left_keys  # Partition by left keys

        node.children = children
        return node

    def _assign_aggregate_operator(
        self,
        node: PlanNode,
        children: List[PlanNode]
    ) -> PlanNode:
        """
        Assign GraphAggregateOperator to aggregate node.

        Stateful operator - requires shuffle by group keys.

        Args:
            node: Aggregate node from logical plan
            children: Physical children (should be 1)

        Returns:
            Node with GraphAggregateOperator assigned
        """
        # Extract group keys and aggregation functions
        group_keys = node.properties.get('group_keys', [])
        agg_funcs = node.properties.get('agg_funcs', {})

        # Create operator
        if GraphAggregateOperator is not None:
            operator = GraphAggregateOperator(
                group_keys=group_keys,
                agg_funcs=agg_funcs
            )
            node.properties['operator'] = operator
            node.properties['operator_type'] = 'GraphAggregateOperator'
            node.properties['stateful'] = True
            node.properties['partition_keys'] = group_keys  # Partition by group keys

        node.children = children
        return node
