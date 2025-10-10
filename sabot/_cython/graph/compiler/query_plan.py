"""
Query Plan Representation

Logical and physical query plan nodes for SPARQL and Cypher queries.
Supports cost-based optimization and query execution.

Architecture:
- LogicalPlan: Abstract query representation (what to compute)
- PhysicalPlan: Executable plan with operators (how to compute)
- PlanNode: Base class for all plan nodes
- Cost annotations for optimization decisions

Inspired by:
- Apache Calcite query algebra
- DuckDB logical optimizer
- PostgreSQL query planner
"""

from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any, Set
from enum import Enum
import pyarrow as pa


# ==============================================================================
# Cost Model
# ==============================================================================

@dataclass
class CostEstimate:
    """
    Cost estimate for a query operation.

    Components:
    - rows: Estimated number of output rows (cardinality)
    - cpu_cost: Computational cost (operations)
    - memory_cost: Memory usage estimate (bytes)
    - io_cost: I/O cost (disk reads)
    """
    rows: int = 0
    cpu_cost: float = 0.0
    memory_cost: int = 0
    io_cost: float = 0.0

    @property
    def total_cost(self) -> float:
        """Total estimated cost (weighted sum)."""
        # Weights: CPU=1, memory=0.5 per MB, I/O=10
        return (
            self.cpu_cost +
            (self.memory_cost / 1_000_000) * 0.5 +
            self.io_cost * 10
        )

    def __add__(self, other: 'CostEstimate') -> 'CostEstimate':
        """Add two cost estimates."""
        return CostEstimate(
            rows=self.rows,  # Output rows from second operation
            cpu_cost=self.cpu_cost + other.cpu_cost,
            memory_cost=max(self.memory_cost, other.memory_cost),  # Peak memory
            io_cost=self.io_cost + other.io_cost
        )

    def __repr__(self) -> str:
        return (
            f"Cost(rows={self.rows:,}, "
            f"cpu={self.cpu_cost:.1f}, "
            f"mem={self.memory_cost // 1024}KB, "
            f"io={self.io_cost:.1f})"
        )


# ==============================================================================
# Plan Node Types
# ==============================================================================

class NodeType(Enum):
    """Type of plan node."""
    # Scan nodes
    SCAN_VERTICES = "scan_vertices"
    SCAN_EDGES = "scan_edges"
    SCAN_TRIPLES = "scan_triples"

    # Filter nodes
    FILTER = "filter"

    # Join nodes
    HASH_JOIN = "hash_join"
    NESTED_LOOP_JOIN = "nested_loop_join"

    # Transformation nodes
    PROJECT = "project"
    AGGREGATE = "aggregate"
    SORT = "sort"
    LIMIT = "limit"
    DISTINCT = "distinct"

    # Graph-specific nodes
    PATTERN_MATCH = "pattern_match"
    PATH_EXPAND = "path_expand"


# ==============================================================================
# Plan Nodes
# ==============================================================================

@dataclass
class PlanNode:
    """
    Base class for all query plan nodes.

    Attributes:
        node_type: Type of plan node
        children: Child plan nodes
        cost: Estimated cost
        properties: Node-specific properties
    """
    node_type: NodeType
    children: List['PlanNode'] = field(default_factory=list)
    cost: Optional[CostEstimate] = None
    properties: Dict[str, Any] = field(default_factory=dict)

    def get_output_schema(self) -> Optional[pa.Schema]:
        """Get output Arrow schema of this node."""
        return self.properties.get('output_schema')

    def get_output_rows(self) -> int:
        """Get estimated output row count."""
        return self.cost.rows if self.cost else 0

    def __repr__(self) -> str:
        return f"{self.node_type.value}({self.cost})"


@dataclass
class ScanNode(PlanNode):
    """
    Scan operation (read from storage).

    Properties:
        - table_name: Name of table to scan
        - filters: Pushed-down filters
        - projection: Columns to select
    """
    def __init__(
        self,
        node_type: NodeType,
        table_name: str,
        filters: Optional[List[Any]] = None,
        projection: Optional[List[str]] = None,
        **kwargs
    ):
        super().__init__(node_type, **kwargs)
        self.properties['table_name'] = table_name
        self.properties['filters'] = filters or []
        self.properties['projection'] = projection or []


@dataclass
class FilterNode(PlanNode):
    """
    Filter operation.

    Properties:
        - condition: Filter expression
        - selectivity: Estimated selectivity (0.0-1.0)
    """
    def __init__(
        self,
        condition: Any,
        selectivity: float = 1.0,
        **kwargs
    ):
        super().__init__(NodeType.FILTER, **kwargs)
        self.properties['condition'] = condition
        self.properties['selectivity'] = selectivity


@dataclass
class JoinNode(PlanNode):
    """
    Join operation.

    Properties:
        - join_type: 'inner', 'left', 'right', 'full'
        - left_keys: Join keys from left child
        - right_keys: Join keys from right child
        - join_condition: Additional join predicates
    """
    def __init__(
        self,
        node_type: NodeType,
        join_type: str,
        left_keys: List[str],
        right_keys: List[str],
        join_condition: Optional[Any] = None,
        **kwargs
    ):
        super().__init__(node_type, **kwargs)
        self.properties['join_type'] = join_type
        self.properties['left_keys'] = left_keys
        self.properties['right_keys'] = right_keys
        self.properties['join_condition'] = join_condition


@dataclass
class ProjectNode(PlanNode):
    """
    Projection operation (select columns).

    Properties:
        - columns: List of column names or expressions
    """
    def __init__(
        self,
        columns: List[str],
        **kwargs
    ):
        super().__init__(NodeType.PROJECT, **kwargs)
        self.properties['columns'] = columns


@dataclass
class LimitNode(PlanNode):
    """
    Limit operation.

    Properties:
        - limit: Max rows to return
        - offset: Rows to skip
    """
    def __init__(
        self,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        **kwargs
    ):
        super().__init__(NodeType.LIMIT, **kwargs)
        self.properties['limit'] = limit
        self.properties['offset'] = offset


# ==============================================================================
# Complete Query Plans
# ==============================================================================

@dataclass
class LogicalPlan:
    """
    Logical query plan (what to compute).

    Represents query semantics without execution details.
    Used for optimization before converting to physical plan.
    """
    root: PlanNode
    statistics: Optional[Dict[str, Any]] = None

    def get_all_nodes(self) -> List[PlanNode]:
        """Get all nodes in plan (DFS traversal)."""
        nodes = []

        def visit(node: PlanNode):
            nodes.append(node)
            for child in node.children:
                visit(child)

        visit(self.root)
        return nodes

    def get_estimated_cost(self) -> CostEstimate:
        """Get total estimated cost of plan."""
        if self.root.cost:
            return self.root.cost
        return CostEstimate()

    def __repr__(self) -> str:
        return f"LogicalPlan(cost={self.get_estimated_cost()})"


@dataclass
class PhysicalPlan:
    """
    Physical query plan (how to execute).

    Contains concrete operators and execution strategy.
    Ready for execution by query engine.
    """
    root: PlanNode
    statistics: Optional[Dict[str, Any]] = None

    def get_execution_order(self) -> List[PlanNode]:
        """Get nodes in execution order (bottom-up)."""
        nodes = []

        def visit(node: PlanNode):
            for child in node.children:
                visit(child)
            nodes.append(node)

        visit(self.root)
        return nodes

    def explain(self) -> str:
        """Generate human-readable execution plan explanation."""
        lines = ["Physical Execution Plan:", ""]

        def format_node(node: PlanNode, depth: int = 0) -> List[str]:
            indent = "  " * depth
            result = [f"{indent}→ {node.node_type.value}"]

            # Add node details
            if node.cost:
                result.append(f"{indent}  {node.cost}")

            # Add properties
            for key, value in node.properties.items():
                if key not in ['output_schema']:
                    result.append(f"{indent}  {key}: {value}")

            # Recurse to children
            for child in node.children:
                result.extend(format_node(child, depth + 1))

            return result

        lines.extend(format_node(self.root))
        lines.append("")
        lines.append(f"Total estimated cost: {self.root.cost.total_cost if self.root.cost else 0:.2f}")

        return "\n".join(lines)

    def __repr__(self) -> str:
        return f"PhysicalPlan(nodes={len(self.get_execution_order())})"


# ==============================================================================
# Plan Builder Utilities
# ==============================================================================

class PlanBuilder:
    """Helper class for building query plans."""

    @staticmethod
    def scan_vertices(
        table_name: str,
        filters: Optional[List[Any]] = None,
        projection: Optional[List[str]] = None
    ) -> ScanNode:
        """Create vertex scan node."""
        return ScanNode(
            NodeType.SCAN_VERTICES,
            table_name=table_name,
            filters=filters,
            projection=projection
        )

    @staticmethod
    def scan_edges(
        table_name: str,
        filters: Optional[List[Any]] = None,
        projection: Optional[List[str]] = None
    ) -> ScanNode:
        """Create edge scan node."""
        return ScanNode(
            NodeType.SCAN_EDGES,
            table_name=table_name,
            filters=filters,
            projection=projection
        )

    @staticmethod
    def scan_triples(
        table_name: str,
        filters: Optional[List[Any]] = None
    ) -> ScanNode:
        """Create RDF triple scan node."""
        return ScanNode(
            NodeType.SCAN_TRIPLES,
            table_name=table_name,
            filters=filters
        )

    @staticmethod
    def filter(child: PlanNode, condition: Any, selectivity: float = 0.5) -> FilterNode:
        """Create filter node."""
        node = FilterNode(
            condition=condition,
            selectivity=selectivity,
            children=[child]
        )
        return node

    @staticmethod
    def hash_join(
        left: PlanNode,
        right: PlanNode,
        left_keys: List[str],
        right_keys: List[str],
        join_type: str = 'inner'
    ) -> JoinNode:
        """Create hash join node."""
        return JoinNode(
            NodeType.HASH_JOIN,
            join_type=join_type,
            left_keys=left_keys,
            right_keys=right_keys,
            children=[left, right]
        )

    @staticmethod
    def project(child: PlanNode, columns: List[str]) -> ProjectNode:
        """Create projection node."""
        return ProjectNode(
            columns=columns,
            children=[child]
        )

    @staticmethod
    def limit(
        child: PlanNode,
        limit: Optional[int] = None,
        offset: Optional[int] = None
    ) -> LimitNode:
        """Create limit node."""
        return LimitNode(
            limit=limit,
            offset=offset,
            children=[child]
        )
