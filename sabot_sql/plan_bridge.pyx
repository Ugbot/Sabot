# cython: language_level=3
# distutils: language=c++

"""
Plan Bridge - Cython wrapper for DuckDB plan parsing and stage partitioning.

This module provides Python-accessible classes for:
- Parsing SQL queries using DuckDB
- Partitioning logical plans into distributed stages
- Serializing plans for distributed execution
"""

from cython.operator cimport dereference as deref
from libcpp cimport bool as cbool
from libcpp.string cimport string
from libcpp.vector cimport vector
from libcpp.unordered_map cimport unordered_map
from libcpp.memory cimport shared_ptr, make_shared
from libcpp.optional cimport optional

cimport plan_bridge

from typing import Dict, List, Optional, Any


# Helper functions to convert C++ to Python
cdef list _vec_to_list(vector[string] vec):
    """Convert C++ string vector to Python list."""
    return [s.decode('utf-8') for s in vec]


cdef dict _map_to_dict(unordered_map[string, string] m):
    """Convert C++ unordered_map to Python dict."""
    return {k.decode('utf-8'): v.decode('utf-8') for k, v in m}


cdef dict _serialize_operator_to_dict(plan_bridge.SerializedOperator& op):
    """Convert SerializedOperator to Python dict."""
    return {
        'type': op.type.decode('utf-8'),
        'name': op.name.decode('utf-8'),
        'estimated_cardinality': op.estimated_cardinality,
        'column_names': _vec_to_list(op.column_names),
        'column_types': _vec_to_list(op.column_types),
        'params': _map_to_dict(op.params),
        'table_name': op.table_name.decode('utf-8'),
        'projected_columns': _vec_to_list(op.projected_columns),
        'filter_expression': op.filter_expression.decode('utf-8'),
        'left_keys': _vec_to_list(op.left_keys),
        'right_keys': _vec_to_list(op.right_keys),
        'join_type': op.join_type.decode('utf-8'),
        'group_by_keys': _vec_to_list(op.group_by_keys),
        'aggregate_functions': _vec_to_list(op.aggregate_functions),
        'aggregate_columns': _vec_to_list(op.aggregate_columns),
    }


cdef dict _serialize_shuffle_to_dict(plan_bridge.SerializedShuffle& shuffle):
    """Convert SerializedShuffle to Python dict."""
    return {
        'shuffle_id': shuffle.shuffle_id.decode('utf-8'),
        'type': shuffle.type.decode('utf-8'),
        'partition_keys': _vec_to_list(shuffle.partition_keys),
        'num_partitions': shuffle.num_partitions,
        'producer_stage_id': shuffle.producer_stage_id.decode('utf-8'),
        'consumer_stage_id': shuffle.consumer_stage_id.decode('utf-8'),
        'column_names': _vec_to_list(shuffle.column_names),
        'column_types': _vec_to_list(shuffle.column_types),
    }


cdef dict _serialize_stage_to_dict(plan_bridge.SerializedStage& stage):
    """Convert SerializedStage to Python dict."""
    return {
        'stage_id': stage.stage_id.decode('utf-8'),
        'operators': [_serialize_operator_to_dict(op) for op in stage.operators],
        'input_shuffle_ids': _vec_to_list(stage.input_shuffle_ids),
        'output_shuffle_id': stage.output_shuffle_id.decode('utf-8'),
        'parallelism': stage.parallelism,
        'is_source': stage.is_source,
        'is_sink': stage.is_sink,
        'estimated_rows': stage.estimated_rows,
        'dependency_stage_ids': _vec_to_list(stage.dependency_stage_ids),
    }


cdef dict _serialize_plan_to_dict(plan_bridge.SerializedPlan& plan):
    """Convert SerializedPlan to Python dict."""
    # Convert stages
    stages = [_serialize_stage_to_dict(s) for s in plan.stages]

    # Convert shuffles
    shuffles = {}
    cdef pair[string, plan_bridge.SerializedShuffle] item
    for item in plan.shuffles:
        key = item.first.decode('utf-8')
        shuffles[key] = _serialize_shuffle_to_dict(item.second)

    # Convert execution waves
    waves = []
    for wave in plan.execution_waves:
        waves.append(_vec_to_list(wave))

    return {
        'sql': plan.sql.decode('utf-8'),
        'stages': stages,
        'shuffles': shuffles,
        'execution_waves': waves,
        'output_column_names': _vec_to_list(plan.output_column_names),
        'output_column_types': _vec_to_list(plan.output_column_types),
        'requires_shuffle': plan.requires_shuffle,
        'total_parallelism': plan.total_parallelism,
        'estimated_total_rows': plan.estimated_total_rows,
    }


# Import Arrow for table handling
from libcpp.utility cimport pair

cdef extern from "arrow/python/pyarrow.h" namespace "arrow::py":
    shared_ptr[plan_bridge.Table] unwrap_table(object)
    object wrap_table(shared_ptr[plan_bridge.Table])


cdef class SQLPlanner:
    """
    SQL Planner that uses DuckDB for parsing and Sabot for distributed execution.

    This class provides the main interface for:
    1. Registering Arrow tables as SQL tables
    2. Parsing SQL queries into logical plans
    3. Partitioning plans into distributed stages
    4. Returning execution plans as Python dictionaries
    """

    cdef shared_ptr[plan_bridge.DuckDBBridge] _bridge
    cdef plan_bridge.StagePartitioner* _partitioner
    cdef int _parallelism

    def __cinit__(self, int parallelism=4, str database_path=":memory:"):
        """Initialize the SQL planner with DuckDB and stage partitioner."""
        cdef string db_path = database_path.encode('utf-8')
        cdef plan_bridge.Result[shared_ptr[plan_bridge.DuckDBBridge]] result

        result = plan_bridge.DuckDBBridge.Create(db_path)
        if not result.ok():
            raise RuntimeError(f"Failed to create DuckDB bridge: {result.status().ToString().decode('utf-8')}")

        self._bridge = result.ValueOrDie()
        self._partitioner = new plan_bridge.StagePartitioner(parallelism)
        self._parallelism = parallelism

    def __dealloc__(self):
        if self._partitioner != NULL:
            del self._partitioner

    def register_table(self, str table_name, table) -> None:
        """
        Register an Arrow table for SQL queries.

        Args:
            table_name: Name to use in SQL queries
            table: PyArrow Table to register
        """
        cdef string name = table_name.encode('utf-8')
        cdef shared_ptr[plan_bridge.Table] arrow_table = unwrap_table(table)

        cdef plan_bridge.Status status = deref(self._bridge).RegisterTable(name, arrow_table)
        if not status.ok():
            raise RuntimeError(f"Failed to register table: {status.ToString().decode('utf-8')}")

    def parse_and_partition(self, str sql) -> Dict[str, Any]:
        """
        Parse SQL and partition into distributed execution stages.

        Args:
            sql: SQL query string

        Returns:
            Dictionary containing:
            - stages: List of execution stages
            - shuffles: Dictionary of shuffle specifications
            - execution_waves: List of parallel execution waves
            - output_schema: Output column names and types
        """
        cdef string query = sql.encode('utf-8')

        # Parse and optimize with DuckDB
        cdef plan_bridge.Result[plan_bridge.LogicalPlan] plan_result
        plan_result = deref(self._bridge).ParseAndOptimize(query)

        if not plan_result.ok():
            raise RuntimeError(f"Failed to parse SQL: {plan_result.status().ToString().decode('utf-8')}")

        cdef plan_bridge.LogicalPlan logical_plan = plan_result.ValueOrDie()

        # Partition into stages
        cdef plan_bridge.Result[plan_bridge.DistributedQueryPlan] dist_result
        dist_result = deref(self._partitioner).Partition(logical_plan)

        if not dist_result.ok():
            raise RuntimeError(f"Failed to partition plan: {dist_result.status().ToString().decode('utf-8')}")

        cdef plan_bridge.DistributedQueryPlan dist_plan = dist_result.ValueOrDie()

        # Serialize to Python-accessible format
        cdef plan_bridge.Result[plan_bridge.SerializedPlan] ser_result
        ser_result = plan_bridge.PlanSerializer.Serialize(dist_plan)

        if not ser_result.ok():
            raise RuntimeError(f"Failed to serialize plan: {ser_result.status().ToString().decode('utf-8')}")

        cdef plan_bridge.SerializedPlan serialized = ser_result.ValueOrDie()

        return _serialize_plan_to_dict(serialized)

    def execute_with_duckdb(self, str sql):
        """
        Execute SQL directly with DuckDB (for testing/comparison).

        Args:
            sql: SQL query string

        Returns:
            PyArrow Table with results
        """
        cdef string query = sql.encode('utf-8')
        cdef plan_bridge.Result[shared_ptr[plan_bridge.Table]] result

        result = deref(self._bridge).ExecuteWithDuckDB(query)

        if not result.ok():
            raise RuntimeError(f"Failed to execute SQL: {result.status().ToString().decode('utf-8')}")

        return wrap_table(result.ValueOrDie())

    def set_parallelism(self, int parallelism) -> None:
        """Set the parallelism level for stage partitioning."""
        self._parallelism = parallelism
        deref(self._partitioner).SetParallelism(parallelism)

    @property
    def parallelism(self) -> int:
        """Get the current parallelism level."""
        return self._parallelism

    def table_exists(self, str table_name) -> bool:
        """Check if a table is registered."""
        cdef string name = table_name.encode('utf-8')
        return deref(self._bridge).TableExists(name)


def create_sql_planner(parallelism: int = 4, database_path: str = ":memory:") -> SQLPlanner:
    """
    Create a new SQL planner instance.

    Args:
        parallelism: Default parallelism for distributed execution
        database_path: Path to DuckDB database (default: in-memory)

    Returns:
        SQLPlanner instance
    """
    return SQLPlanner(parallelism, database_path)
