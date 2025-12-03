#pragma once

#include <memory>
#include <string>
#include <vector>
#include <unordered_map>
#include <arrow/api.h>
#include <arrow/result.h>

#include "sabot_sql/sql/stage_partitioner.h"

namespace sabot_sql {
namespace sql {

/**
 * @brief Serializes DistributedQueryPlan to a format consumable by Python
 *
 * The plan is serialized to a key-value structure that can be
 * easily converted to Python dictionaries in the Cython wrapper.
 */
class PlanSerializer {
public:
    /**
     * @brief Serialized operator representation
     */
    struct SerializedOperator {
        std::string type;  // "TableScan", "Filter", "Projection", "HashJoin", "Aggregate"
        std::string name;
        size_t estimated_cardinality;

        // Schema
        std::vector<std::string> column_names;
        std::vector<std::string> column_types;  // Arrow type names

        // Common parameters (as strings for simplicity)
        std::unordered_map<std::string, std::string> params;

        // For TableScan
        std::string table_name;
        std::vector<std::string> projected_columns;

        // For Filter
        std::string filter_expression;

        // For Join
        std::vector<std::string> left_keys;
        std::vector<std::string> right_keys;
        std::string join_type;

        // For Aggregate
        std::vector<std::string> group_by_keys;
        std::vector<std::string> aggregate_functions;
        std::vector<std::string> aggregate_columns;
    };

    /**
     * @brief Serialized shuffle specification
     */
    struct SerializedShuffle {
        std::string shuffle_id;
        std::string type;  // "HASH", "BROADCAST", "ROUND_ROBIN", "RANGE"
        std::vector<std::string> partition_keys;
        int32_t num_partitions;
        std::string producer_stage_id;
        std::string consumer_stage_id;

        // Schema as column names and types
        std::vector<std::string> column_names;
        std::vector<std::string> column_types;
    };

    /**
     * @brief Serialized execution stage
     */
    struct SerializedStage {
        std::string stage_id;
        std::vector<SerializedOperator> operators;
        std::vector<std::string> input_shuffle_ids;
        std::string output_shuffle_id;  // Empty if none
        int32_t parallelism;
        bool is_source;
        bool is_sink;
        size_t estimated_rows;
        std::vector<std::string> dependency_stage_ids;
    };

    /**
     * @brief Serialized complete plan
     */
    struct SerializedPlan {
        std::string sql;
        std::vector<SerializedStage> stages;
        std::unordered_map<std::string, SerializedShuffle> shuffles;
        std::vector<std::vector<std::string>> execution_waves;

        // Output schema
        std::vector<std::string> output_column_names;
        std::vector<std::string> output_column_types;

        // Metadata
        bool requires_shuffle;
        int32_t total_parallelism;
        size_t estimated_total_rows;
    };

    /**
     * @brief Serialize a DistributedQueryPlan to SerializedPlan
     */
    static arrow::Result<SerializedPlan> Serialize(
        const DistributedQueryPlan& plan);

    /**
     * @brief Get string representation of shuffle type
     */
    static std::string ShuffleTypeToString(ShuffleType type);

    /**
     * @brief Get string representation of Arrow type
     */
    static std::string ArrowTypeToString(
        const std::shared_ptr<arrow::DataType>& type);

    /**
     * @brief Get string representation of DuckDB logical operator type
     */
    static std::string LogicalOperatorTypeToString(
        duckdb::LogicalOperatorType type);

private:
    /**
     * @brief Serialize a single operator node
     */
    static arrow::Result<SerializedOperator> SerializeOperator(
        const LogicalOperatorNode& node);

    /**
     * @brief Serialize a single shuffle spec
     */
    static arrow::Result<SerializedShuffle> SerializeShuffle(
        const ShuffleSpec& spec);

    /**
     * @brief Serialize a single execution stage
     */
    static arrow::Result<SerializedStage> SerializeStage(
        const ExecutionStage& stage);
};

} // namespace sql
} // namespace sabot_sql
