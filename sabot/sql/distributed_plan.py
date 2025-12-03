"""
Distributed Query Plan Data Structures

These dataclasses represent the distributed query execution plan
as produced by the C++ StagePartitioner. They are designed to be
serializable and usable by the Python scheduling layer.
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any
from enum import Enum


class ShuffleType(Enum):
    """Type of shuffle between stages."""
    HASH = "HASH"
    BROADCAST = "BROADCAST"
    ROUND_ROBIN = "ROUND_ROBIN"
    RANGE = "RANGE"


class OperatorType(Enum):
    """Types of logical operators supported."""
    TABLE_SCAN = "TableScan"
    FILTER = "Filter"
    PROJECTION = "Projection"
    HASH_JOIN = "HashJoin"
    AGGREGATE = "Aggregate"
    SORT = "Sort"
    LIMIT = "Limit"
    DISTINCT = "Distinct"
    UNION = "Union"
    INTERSECT = "Intersect"
    EXCEPT = "Except"
    WINDOW = "Window"
    TOP_N = "TopN"  # Optimized LIMIT with ORDER BY
    CROSS_JOIN = "CrossJoin"
    SEMI_JOIN = "SemiJoin"  # EXISTS subquery
    ANTI_JOIN = "AntiJoin"  # NOT EXISTS subquery

    # Time Series Operators
    ASOF_JOIN = "AsofJoin"  # Join with most recent match
    INTERVAL_JOIN = "IntervalJoin"  # Join within time interval
    TIME_BUCKET = "TimeBucket"  # Group by time buckets (tumbling window)
    SESSION_WINDOW = "SessionWindow"  # Group by session gaps
    ROLLING_WINDOW = "RollingWindow"  # Moving aggregates
    RESAMPLE = "Resample"  # Upsample/downsample
    FILL = "Fill"  # Forward fill, backward fill, interpolate
    TIME_DIFF = "TimeDiff"  # Time differences
    EWMA = "EWMA"  # Exponentially weighted moving average
    LOG_RETURNS = "LogReturns"  # Financial log returns


@dataclass
class OperatorSpec:
    """
    Specification for a logical operator in a stage.

    This is the Python equivalent of LogicalOperatorNode from C++.
    Contains all information needed to instantiate a Cython/Python operator.
    """
    type: str  # OperatorType as string
    name: str
    estimated_cardinality: int

    # Schema
    column_names: List[str] = field(default_factory=list)
    column_types: List[str] = field(default_factory=list)

    # Generic parameters
    params: Dict[str, str] = field(default_factory=dict)
    int_params: Dict[str, int] = field(default_factory=dict)
    string_list_params: Dict[str, List[str]] = field(default_factory=dict)

    # TableScan specific
    table_name: str = ""
    projected_columns: List[str] = field(default_factory=list)

    # Filter specific
    filter_expression: str = ""

    # Join specific
    left_keys: List[str] = field(default_factory=list)
    right_keys: List[str] = field(default_factory=list)
    join_type: str = "inner"

    # Aggregate specific
    group_by_keys: List[str] = field(default_factory=list)
    aggregate_functions: List[str] = field(default_factory=list)
    aggregate_columns: List[str] = field(default_factory=list)
    having_expression: str = ""  # HAVING clause filter

    # Window function specific
    window_partition_by: List[str] = field(default_factory=list)
    window_order_by: List[str] = field(default_factory=list)
    window_order_directions: List[str] = field(default_factory=list)
    window_function: str = ""  # ROW_NUMBER, RANK, SUM, etc.
    window_column: str = ""  # Column to apply window function to
    window_frame_start: int = 0  # UNBOUNDED PRECEDING = -1
    window_frame_end: int = 0  # CURRENT ROW = 0, UNBOUNDED FOLLOWING = 1

    # Set operation specific (UNION, INTERSECT, EXCEPT)
    set_operation: str = ""  # UNION, UNION_ALL, INTERSECT, EXCEPT
    set_all: bool = False  # UNION ALL vs UNION

    # Time series operator specific
    time_column: str = ""  # Column containing timestamps
    time_direction: str = "backward"  # backward (ASOF) or forward
    interval_lower: int = 0  # Lower bound for interval join (milliseconds)
    interval_upper: int = 0  # Upper bound for interval join (milliseconds)
    bucket_size: str = ""  # Time bucket size (e.g., "1h", "5m", "1d")
    bucket_origin: str = ""  # Origin timestamp for bucketing
    fill_method: str = ""  # forward, backward, linear, null
    fill_limit: int = 0  # Max consecutive fills
    ewma_alpha: float = 0.0  # EWMA decay factor (0-1)
    ewma_adjust: bool = True  # EWMA adjustment for initial values
    rolling_window_size: int = 0  # Size of rolling window
    rolling_min_periods: int = 1  # Minimum observations for rolling
    session_gap: int = 0  # Session gap in milliseconds
    resample_rule: str = ""  # Resample frequency
    resample_agg: str = ""  # Aggregation method for resample

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'OperatorSpec':
        """Create OperatorSpec from dictionary (from C++ serialization)."""
        return cls(
            type=data.get('type', ''),
            name=data.get('name', ''),
            estimated_cardinality=data.get('estimated_cardinality', 0),
            column_names=data.get('column_names', []),
            column_types=data.get('column_types', []),
            params=data.get('params', {}),
            int_params=data.get('int_params', {}),
            string_list_params=data.get('string_list_params', {}),
            table_name=data.get('table_name', ''),
            projected_columns=data.get('projected_columns', []),
            filter_expression=data.get('filter_expression', ''),
            left_keys=data.get('left_keys', []),
            right_keys=data.get('right_keys', []),
            join_type=data.get('join_type', 'inner'),
            group_by_keys=data.get('group_by_keys', []),
            aggregate_functions=data.get('aggregate_functions', []),
            aggregate_columns=data.get('aggregate_columns', []),
            having_expression=data.get('having_expression', ''),
            window_partition_by=data.get('window_partition_by', []),
            window_order_by=data.get('window_order_by', []),
            window_order_directions=data.get('window_order_directions', []),
            window_function=data.get('window_function', ''),
            window_column=data.get('window_column', ''),
            window_frame_start=data.get('window_frame_start', 0),
            window_frame_end=data.get('window_frame_end', 0),
            set_operation=data.get('set_operation', ''),
            set_all=data.get('set_all', False),
            # Time series fields
            time_column=data.get('time_column', ''),
            time_direction=data.get('time_direction', 'backward'),
            interval_lower=data.get('interval_lower', 0),
            interval_upper=data.get('interval_upper', 0),
            bucket_size=data.get('bucket_size', ''),
            bucket_origin=data.get('bucket_origin', ''),
            fill_method=data.get('fill_method', ''),
            fill_limit=data.get('fill_limit', 0),
            ewma_alpha=data.get('ewma_alpha', 0.0),
            ewma_adjust=data.get('ewma_adjust', True),
            rolling_window_size=data.get('rolling_window_size', 0),
            rolling_min_periods=data.get('rolling_min_periods', 1),
            session_gap=data.get('session_gap', 0),
            resample_rule=data.get('resample_rule', ''),
            resample_agg=data.get('resample_agg', ''),
        )


@dataclass
class ShuffleSpec:
    """
    Specification for a shuffle between stages.

    Defines how data is partitioned and transferred between stages.
    """
    shuffle_id: str
    type: str  # ShuffleType as string
    partition_keys: List[str]
    num_partitions: int
    producer_stage_id: str
    consumer_stage_id: str

    # Schema
    column_names: List[str] = field(default_factory=list)
    column_types: List[str] = field(default_factory=list)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ShuffleSpec':
        """Create ShuffleSpec from dictionary."""
        return cls(
            shuffle_id=data.get('shuffle_id', ''),
            type=data.get('type', 'HASH'),
            partition_keys=data.get('partition_keys', []),
            num_partitions=data.get('num_partitions', 4),
            producer_stage_id=data.get('producer_stage_id', ''),
            consumer_stage_id=data.get('consumer_stage_id', ''),
            column_names=data.get('column_names', []),
            column_types=data.get('column_types', []),
        )


@dataclass
class ExecutionStage:
    """
    A stage in the distributed execution plan.

    A stage contains a pipeline of operators that execute without shuffle.
    Shuffle boundaries define stage boundaries.
    """
    stage_id: str
    operators: List[OperatorSpec]
    input_shuffle_ids: List[str]
    output_shuffle_id: Optional[str]
    parallelism: int
    is_source: bool
    is_sink: bool
    estimated_rows: int
    dependency_stage_ids: List[str] = field(default_factory=list)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ExecutionStage':
        """Create ExecutionStage from dictionary."""
        operators = [
            OperatorSpec.from_dict(op)
            for op in data.get('operators', [])
        ]

        output_shuffle = data.get('output_shuffle_id', '')
        if output_shuffle == '':
            output_shuffle = None

        return cls(
            stage_id=data.get('stage_id', ''),
            operators=operators,
            input_shuffle_ids=data.get('input_shuffle_ids', []),
            output_shuffle_id=output_shuffle,
            parallelism=data.get('parallelism', 4),
            is_source=data.get('is_source', False),
            is_sink=data.get('is_sink', False),
            estimated_rows=data.get('estimated_rows', 0),
            dependency_stage_ids=data.get('dependency_stage_ids', []),
        )


@dataclass
class DistributedQueryPlan:
    """
    Complete distributed query plan.

    Contains all stages and shuffle specifications needed to
    execute a query across distributed agents.
    """
    sql: str
    stages: List[ExecutionStage]
    shuffles: Dict[str, ShuffleSpec]
    execution_waves: List[List[str]]
    output_column_names: List[str]
    output_column_types: List[str]
    requires_shuffle: bool
    total_parallelism: int
    estimated_total_rows: int

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'DistributedQueryPlan':
        """Create DistributedQueryPlan from dictionary (from C++ serialization)."""
        stages = [
            ExecutionStage.from_dict(s)
            for s in data.get('stages', [])
        ]

        shuffles = {
            k: ShuffleSpec.from_dict(v)
            for k, v in data.get('shuffles', {}).items()
        }

        return cls(
            sql=data.get('sql', ''),
            stages=stages,
            shuffles=shuffles,
            execution_waves=data.get('execution_waves', []),
            output_column_names=data.get('output_column_names', []),
            output_column_types=data.get('output_column_types', []),
            requires_shuffle=data.get('requires_shuffle', False),
            total_parallelism=data.get('total_parallelism', 4),
            estimated_total_rows=data.get('estimated_total_rows', 0),
        )

    def get_source_stages(self) -> List[ExecutionStage]:
        """Get all source stages (read from tables)."""
        return [s for s in self.stages if s.is_source]

    def get_sink_stage(self) -> Optional[ExecutionStage]:
        """Get the sink stage (produces final result)."""
        for s in self.stages:
            if s.is_sink:
                return s
        return None

    def get_stage(self, stage_id: str) -> Optional[ExecutionStage]:
        """Get a stage by ID."""
        for s in self.stages:
            if s.stage_id == stage_id:
                return s
        return None

    def get_shuffle(self, shuffle_id: str) -> Optional[ShuffleSpec]:
        """Get a shuffle by ID."""
        return self.shuffles.get(shuffle_id)

    def get_producer_stage(self, shuffle_id: str) -> Optional[ExecutionStage]:
        """Get the stage that produces a shuffle."""
        shuffle = self.get_shuffle(shuffle_id)
        if shuffle:
            return self.get_stage(shuffle.producer_stage_id)
        return None

    def get_consumer_stage(self, shuffle_id: str) -> Optional[ExecutionStage]:
        """Get the stage that consumes a shuffle."""
        shuffle = self.get_shuffle(shuffle_id)
        if shuffle:
            return self.get_stage(shuffle.consumer_stage_id)
        return None

    def to_explain_string(self) -> str:
        """Generate a human-readable explain string."""
        lines = [
            f"Distributed Query Plan",
            f"=" * 50,
            f"SQL: {self.sql[:100]}..." if len(self.sql) > 100 else f"SQL: {self.sql}",
            f"Requires Shuffle: {self.requires_shuffle}",
            f"Total Parallelism: {self.total_parallelism}",
            f"Estimated Rows: {self.estimated_total_rows}",
            f"",
            f"Execution Waves ({len(self.execution_waves)}):",
        ]

        for i, wave in enumerate(self.execution_waves):
            lines.append(f"  Wave {i}: {', '.join(wave)}")

        lines.append("")
        lines.append(f"Stages ({len(self.stages)}):")

        for stage in self.stages:
            lines.append(f"  {stage.stage_id}:")
            lines.append(f"    Type: {'source' if stage.is_source else 'sink' if stage.is_sink else 'intermediate'}")
            lines.append(f"    Parallelism: {stage.parallelism}")
            lines.append(f"    Operators: {len(stage.operators)}")

            for op in stage.operators:
                lines.append(f"      - {op.type}: {op.name}")

            if stage.input_shuffle_ids:
                lines.append(f"    Input Shuffles: {', '.join(stage.input_shuffle_ids)}")
            if stage.output_shuffle_id:
                lines.append(f"    Output Shuffle: {stage.output_shuffle_id}")

        if self.shuffles:
            lines.append("")
            lines.append(f"Shuffles ({len(self.shuffles)}):")
            for shuffle_id, shuffle in self.shuffles.items():
                lines.append(f"  {shuffle_id}:")
                lines.append(f"    Type: {shuffle.type}")
                lines.append(f"    Keys: {', '.join(shuffle.partition_keys)}")
                lines.append(f"    Partitions: {shuffle.num_partitions}")
                lines.append(f"    {shuffle.producer_stage_id} -> {shuffle.consumer_stage_id}")

        return "\n".join(lines)
