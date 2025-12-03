# cython: language_level=3
# distutils: language=c++

from libcpp cimport bool
from libcpp.string cimport string
from libcpp.vector cimport vector
from libcpp.unordered_map cimport unordered_map
from libcpp.memory cimport shared_ptr, unique_ptr
from libcpp.optional cimport optional

# Arrow declarations
cdef extern from "arrow/api.h" namespace "arrow":
    cdef cppclass Status:
        bool ok()
        string ToString()

    cdef cppclass DataType:
        string ToString()

    cdef cppclass Schema:
        int num_fields()

    cdef cppclass Table:
        pass

    cdef cppclass Result[T]:
        bool ok()
        T ValueOrDie()
        Status status()


# DuckDB declarations
cdef extern from "duckdb.hpp" namespace "duckdb":
    cdef cppclass DuckDB:
        pass

    cdef cppclass Connection:
        pass

    cdef cppclass LogicalOperator:
        pass

    cdef enum LogicalOperatorType:
        pass


# Stage Partitioner declarations
cdef extern from "sabot_sql/sql/stage_partitioner.h" namespace "sabot_sql::sql":
    cdef enum ShuffleType:
        HASH
        BROADCAST
        ROUND_ROBIN
        RANGE

    cdef cppclass ShuffleSpec:
        string shuffle_id
        ShuffleType type
        vector[string] partition_keys
        int num_partitions
        shared_ptr[Schema] schema
        string producer_stage_id
        string consumer_stage_id

    cdef cppclass LogicalOperatorNode:
        LogicalOperatorType type
        string name
        vector[string] column_names
        size_t estimated_cardinality
        string table_name
        vector[string] projected_columns
        string filter_expression
        vector[string] left_keys
        vector[string] right_keys
        string join_type
        vector[string] group_by_keys
        vector[string] aggregate_functions
        vector[string] aggregate_columns

    cdef cppclass ExecutionStage:
        string stage_id
        vector[LogicalOperatorNode] operators
        vector[string] input_shuffle_ids
        optional[string] output_shuffle_id
        int parallelism
        bool is_source
        bool is_sink
        size_t estimated_rows
        vector[string] dependency_stage_ids

    cdef cppclass DistributedQueryPlan:
        string sql
        vector[ExecutionStage] stages
        unordered_map[string, ShuffleSpec] shuffles
        vector[vector[string]] execution_waves
        shared_ptr[Schema] output_schema
        vector[string] output_column_names
        bool requires_shuffle
        int total_parallelism
        size_t estimated_total_rows

    cdef cppclass StagePartitioner:
        StagePartitioner(int) except +
        void SetParallelism(int)
        int GetParallelism()
        # Note: Partition requires LogicalPlan which we need to handle separately


# Plan Serializer declarations
cdef extern from "sabot_sql/sql/plan_serializer.h" namespace "sabot_sql::sql::PlanSerializer":
    cdef cppclass SerializedOperator:
        string type
        string name
        size_t estimated_cardinality
        vector[string] column_names
        vector[string] column_types
        unordered_map[string, string] params
        string table_name
        vector[string] projected_columns
        string filter_expression
        vector[string] left_keys
        vector[string] right_keys
        string join_type
        vector[string] group_by_keys
        vector[string] aggregate_functions
        vector[string] aggregate_columns

    cdef cppclass SerializedShuffle:
        string shuffle_id
        string type
        vector[string] partition_keys
        int num_partitions
        string producer_stage_id
        string consumer_stage_id
        vector[string] column_names
        vector[string] column_types

    cdef cppclass SerializedStage:
        string stage_id
        vector[SerializedOperator] operators
        vector[string] input_shuffle_ids
        string output_shuffle_id
        int parallelism
        bool is_source
        bool is_sink
        size_t estimated_rows
        vector[string] dependency_stage_ids

    cdef cppclass SerializedPlan:
        string sql
        vector[SerializedStage] stages
        unordered_map[string, SerializedShuffle] shuffles
        vector[vector[string]] execution_waves
        vector[string] output_column_names
        vector[string] output_column_types
        bool requires_shuffle
        int total_parallelism
        size_t estimated_total_rows


cdef extern from "sabot_sql/sql/plan_serializer.h" namespace "sabot_sql::sql":
    cdef cppclass PlanSerializer:
        @staticmethod
        Result[SerializedPlan] Serialize(const DistributedQueryPlan& plan)

        @staticmethod
        string ShuffleTypeToString(ShuffleType type)

        @staticmethod
        string ArrowTypeToString(shared_ptr[DataType] type)

        @staticmethod
        string LogicalOperatorTypeToString(LogicalOperatorType type)


# DuckDB Bridge declarations
cdef extern from "sabot_sql/sql/duckdb_bridge.h" namespace "sabot_sql::sql":
    cdef cppclass LogicalPlan:
        unique_ptr[LogicalOperator] root
        vector[string] column_names
        size_t estimated_cardinality
        bool has_ctes
        bool has_subqueries
        bool has_aggregates
        bool has_joins

    cdef cppclass DuckDBBridge:
        @staticmethod
        Result[shared_ptr[DuckDBBridge]] Create(const string& database_path)

        Status RegisterTable(const string& table_name,
                           const shared_ptr[Table]& table)

        Result[LogicalPlan] ParseAndOptimize(const string& sql)

        Result[shared_ptr[Table]] ExecuteWithDuckDB(const string& sql)

        bool TableExists(const string& table_name)
