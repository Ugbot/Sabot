#!/usr/bin/env python3
"""
Unified Logical Plan

Logical query representation that all APIs (Stream, SQL, Graph) translate to.
Enables shared optimization and composition.

Design: Similar to DuckDB/DataFusion logical plans but simpler.
"""

import logging
from typing import Optional, List, Any, Dict
from dataclasses import dataclass
from enum import Enum
from abc import ABC, abstractmethod

logger = logging.getLogger(__name__)


class LogicalOperatorType(Enum):
    """Types of logical operators."""
    # Data sources
    SCAN = "scan"
    
    # Transforms
    FILTER = "filter"
    PROJECT = "project"
    MAP = "map"
    
    # Aggregations
    AGGREGATE = "aggregate"
    GROUP_BY = "group_by"
    
    # Joins
    JOIN = "join"
    
    # Windows
    WINDOW = "window"
    
    # Sorting/Limiting
    SORT = "sort"
    LIMIT = "limit"
    
    # Set operations
    UNION = "union"
    DISTINCT = "distinct"


class LogicalPlan(ABC):
    """
    Base class for logical plan nodes.
    
    All query APIs (Stream, SQL, Graph) translate to logical plans,
    enabling shared optimization and composition.
    
    Design principles:
    - Immutable (functional transformations)
    - Tree structure (parent-child relationships)
    - Type-safe (strong typing)
    - Serializable (can be sent over network)
    """
    
    def __init__(self, operator_type: LogicalOperatorType):
        self.operator_type = operator_type
        self.children: List[LogicalPlan] = []
        self.metadata: Dict[str, Any] = {}
    
    @abstractmethod
    def to_dict(self) -> Dict[str, Any]:
        """Serialize to dictionary."""
        pass
    
    @abstractmethod
    def validate(self) -> bool:
        """Validate plan correctness."""
        pass
    
    def add_child(self, child: 'LogicalPlan'):
        """Add child operator."""
        self.children.append(child)
    
    def get_children(self) -> List['LogicalPlan']:
        """Get child operators."""
        return self.children


@dataclass
class ScanPlan(LogicalPlan):
    """
    Scan data source.
    
    Represents reading from:
    - Files (Parquet, CSV, Arrow IPC)
    - Kafka topics
    - Database tables
    - In-memory tables
    """
    source_type: str  # 'kafka', 'parquet', 'table', etc.
    source_name: str  # Topic name, file path, table name
    schema: Optional[Any] = None
    options: Optional[Dict[str, Any]] = None
    
    def __post_init__(self):
        super().__init__(LogicalOperatorType.SCAN)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'type': 'scan',
            'source_type': self.source_type,
            'source_name': self.source_name,
            'options': self.options or {}
        }
    
    def validate(self) -> bool:
        return bool(self.source_name)


@dataclass
class FilterPlan(LogicalPlan):
    """
    Filter rows based on predicate.
    
    Example:
        FilterPlan(input=scan, predicate="x > 10")
    """
    input: LogicalPlan
    predicate: Any  # Can be string expression or callable
    
    def __post_init__(self):
        super().__init__(LogicalOperatorType.FILTER)
        self.add_child(self.input)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'type': 'filter',
            'predicate': str(self.predicate),
            'input': self.input.to_dict()
        }
    
    def validate(self) -> bool:
        return self.input is not None and self.input.validate()


@dataclass
class ProjectPlan(LogicalPlan):
    """
    Project (select) columns.
    
    Example:
        ProjectPlan(input=scan, columns=['a', 'b', 'c'])
    """
    input: LogicalPlan
    columns: List[str]
    
    def __post_init__(self):
        super().__init__(LogicalOperatorType.PROJECT)
        self.add_child(self.input)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'type': 'project',
            'columns': self.columns,
            'input': self.input.to_dict()
        }
    
    def validate(self) -> bool:
        return bool(self.columns) and self.input.validate()


@dataclass
class JoinPlan(LogicalPlan):
    """
    Join two inputs.
    
    Example:
        JoinPlan(left=scan1, right=scan2, left_keys=['id'], right_keys=['user_id'], join_type='inner')
    """
    left: LogicalPlan
    right: LogicalPlan
    left_keys: List[str]
    right_keys: List[str]
    join_type: str = 'inner'  # 'inner', 'left', 'right', 'outer', 'semi', 'anti'
    
    def __post_init__(self):
        super().__init__(LogicalOperatorType.JOIN)
        self.add_child(self.left)
        self.add_child(self.right)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'type': 'join',
            'join_type': self.join_type,
            'left_keys': self.left_keys,
            'right_keys': self.right_keys,
            'left': self.left.to_dict(),
            'right': self.right.to_dict()
        }
    
    def validate(self) -> bool:
        return (
            self.left.validate() and
            self.right.validate() and
            len(self.left_keys) == len(self.right_keys)
        )


@dataclass
class GroupByPlan(LogicalPlan):
    """
    Group by keys and aggregate.
    
    Example:
        GroupByPlan(input=scan, group_keys=['category'], aggregations={'amount': 'sum'})
    """
    input: LogicalPlan
    group_keys: List[str]
    aggregations: Dict[str, str]  # column -> function ('sum', 'count', 'avg', etc.)
    
    def __post_init__(self):
        super().__init__(LogicalOperatorType.GROUP_BY)
        self.add_child(self.input)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'type': 'group_by',
            'group_keys': self.group_keys,
            'aggregations': self.aggregations,
            'input': self.input.to_dict()
        }
    
    def validate(self) -> bool:
        return bool(self.group_keys) and bool(self.aggregations) and self.input.validate()


@dataclass
class WindowPlan(LogicalPlan):
    """
    Windowed aggregation.
    
    Example:
        WindowPlan(input=scan, window_config=tumbling(60.0))
    """
    input: LogicalPlan
    window_config: Any  # WindowConfig from window_unified
    
    def __post_init__(self):
        super().__init__(LogicalOperatorType.WINDOW)
        self.add_child(self.input)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'type': 'window',
            'window_type': self.window_config.window_type.value,
            'size': self.window_config.size,
            'input': self.input.to_dict()
        }
    
    def validate(self) -> bool:
        return self.window_config is not None and self.input.validate()


@dataclass
class LimitPlan(LogicalPlan):
    """
    Limit number of rows.
    
    Example:
        LimitPlan(input=scan, limit=100, offset=0)
    """
    input: LogicalPlan
    limit: int
    offset: int = 0
    
    def __post_init__(self):
        super().__init__(LogicalOperatorType.LIMIT)
        self.add_child(self.input)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'type': 'limit',
            'limit': self.limit,
            'offset': self.offset,
            'input': self.input.to_dict()
        }
    
    def validate(self) -> bool:
        return self.limit > 0 and self.input.validate()


class LogicalPlanBuilder:
    """
    Builder for constructing logical plans.
    
    Provides fluent API for building plans programmatically.
    
    Example:
        plan = (LogicalPlanBuilder()
            .scan('kafka', 'transactions')
            .filter('amount > 1000')
            .project(['id', 'amount', 'customer_id'])
            .group_by(['customer_id'], {'amount': 'sum'})
            .build())
    """
    
    def __init__(self):
        self._plan: Optional[LogicalPlan] = None
    
    def scan(self, source_type: str, source_name: str, **options) -> 'LogicalPlanBuilder':
        """Add scan operator."""
        self._plan = ScanPlan(source_type, source_name, options=options)
        return self
    
    def filter(self, predicate: Any) -> 'LogicalPlanBuilder':
        """Add filter operator."""
        if self._plan is None:
            raise ValueError("Must call scan() first")
        self._plan = FilterPlan(self._plan, predicate)
        return self
    
    def project(self, columns: List[str]) -> 'LogicalPlanBuilder':
        """Add project operator."""
        if self._plan is None:
            raise ValueError("Must call scan() first")
        self._plan = ProjectPlan(self._plan, columns)
        return self
    
    def group_by(self, keys: List[str], aggregations: Dict[str, str]) -> 'LogicalPlanBuilder':
        """Add group by operator."""
        if self._plan is None:
            raise ValueError("Must call scan() first")
        self._plan = GroupByPlan(self._plan, keys, aggregations)
        return self
    
    def join(
        self,
        right: LogicalPlan,
        left_keys: List[str],
        right_keys: List[str],
        join_type: str = 'inner'
    ) -> 'LogicalPlanBuilder':
        """Add join operator."""
        if self._plan is None:
            raise ValueError("Must call scan() first")
        self._plan = JoinPlan(self._plan, right, left_keys, right_keys, join_type)
        return self
    
    def window(self, window_config: Any) -> 'LogicalPlanBuilder':
        """Add window operator."""
        if self._plan is None:
            raise ValueError("Must call scan() first")
        self._plan = WindowPlan(self._plan, window_config)
        return self
    
    def limit(self, limit: int, offset: int = 0) -> 'LogicalPlanBuilder':
        """Add limit operator."""
        if self._plan is None:
            raise ValueError("Must call scan() first")
        self._plan = LimitPlan(self._plan, limit, offset)
        return self
    
    def build(self) -> LogicalPlan:
        """Build and return logical plan."""
        if self._plan is None:
            raise ValueError("Empty plan - must add operators")
        
        if not self._plan.validate():
            raise ValueError("Invalid plan")
        
        return self._plan

