#pragma once

#include <memory>
#include <string>
#include <vector>
#include <optional>
#include <unordered_map>
#include <arrow/api.h>
#include <arrow/result.h>

#include "duckdb.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/common/enums/logical_operator_type.hpp"
#include "sabot_sql/sql/duckdb_bridge.h"

namespace sabot_sql {
namespace sql {

/**
 * @brief Types of shuffle operations between stages
 */
enum class ShuffleType {
    HASH,           // Hash partition by keys
    BROADCAST,      // Send all data to all partitions
    ROUND_ROBIN,    // Distribute evenly
    RANGE           // Range partition (for sorted data)
};

/**
 * @brief Specification for a shuffle between stages
 */
struct ShuffleSpec {
    std::string shuffle_id;
    ShuffleType type;
    std::vector<std::string> partition_keys;  // For HASH shuffles
    int32_t num_partitions;
    std::shared_ptr<arrow::Schema> schema;

    // Producer and consumer stage IDs
    std::string producer_stage_id;
    std::string consumer_stage_id;
};

/**
 * @brief Serialized representation of a DuckDB logical operator node
 *
 * This captures enough information to reconstruct an operator
 * in Sabot's execution layer.
 */
struct LogicalOperatorNode {
    // Operator identification
    duckdb::LogicalOperatorType type;
    std::string name;  // Human-readable name

    // Schema information
    std::vector<std::string> column_names;
    std::vector<std::shared_ptr<arrow::DataType>> column_types;
    size_t estimated_cardinality;

    // Operator-specific parameters (serialized for Cython consumption)
    std::unordered_map<std::string, std::string> string_params;
    std::unordered_map<std::string, int64_t> int_params;
    std::unordered_map<std::string, double> float_params;
    std::unordered_map<std::string, std::vector<std::string>> string_list_params;

    // For table scans
    std::string table_name;
    std::vector<std::string> projected_columns;

    // For filters - serialized expression
    std::string filter_expression;

    // For joins
    std::vector<std::string> left_keys;
    std::vector<std::string> right_keys;
    std::string join_type;  // "inner", "left", "right", "outer"

    // For aggregates
    std::vector<std::string> group_by_keys;
    std::vector<std::string> aggregate_functions;  // e.g., ["SUM", "COUNT", "AVG"]
    std::vector<std::string> aggregate_columns;    // Columns being aggregated
    std::vector<std::string> output_aliases;       // Output column names
};

/**
 * @brief A stage in the distributed execution plan
 *
 * A stage is a sequence of operators that can execute without
 * requiring a shuffle. Shuffles mark stage boundaries.
 */
struct ExecutionStage {
    std::string stage_id;

    // Operators in this stage, in execution order (source first)
    std::vector<LogicalOperatorNode> operators;

    // Shuffle inputs this stage consumes
    std::vector<std::string> input_shuffle_ids;

    // Shuffle output this stage produces (if any)
    std::optional<std::string> output_shuffle_id;

    // Parallelism configuration
    int32_t parallelism;

    // Stage classification
    bool is_source;  // Reads from tables (no shuffle input)
    bool is_sink;    // Final result (no shuffle output)

    // Estimated cost metrics
    size_t estimated_rows;
    size_t estimated_memory_bytes;

    // Dependencies - stages that must complete before this one
    std::vector<std::string> dependency_stage_ids;
};

/**
 * @brief Complete distributed query plan
 *
 * Contains all stages and shuffle specifications needed to
 * execute a query across distributed agents.
 */
struct DistributedQueryPlan {
    // Original SQL for reference
    std::string sql;

    // Execution stages in topological order
    std::vector<ExecutionStage> stages;

    // Shuffle specifications
    std::unordered_map<std::string, ShuffleSpec> shuffles;

    // Execution waves - stages that can execute in parallel
    std::vector<std::vector<std::string>> execution_waves;

    // Output schema of the final result
    std::shared_ptr<arrow::Schema> output_schema;
    std::vector<std::string> output_column_names;

    // Plan metadata
    bool requires_shuffle;
    int32_t total_parallelism;
    size_t estimated_total_rows;
};

/**
 * @brief Partitions a DuckDB logical plan into distributed execution stages
 *
 * The StagePartitioner walks a DuckDB LogicalOperator tree and partitions
 * it into stages separated by shuffle boundaries. The output is a
 * DistributedQueryPlan that can be executed by Sabot's distributed agents.
 *
 * Shuffle Boundary Rules:
 * - LOGICAL_GET (Scan): No shuffle - source stage
 * - LOGICAL_FILTER: No shuffle - stays in same stage as input
 * - LOGICAL_PROJECTION: No shuffle - stays in same stage as input
 * - LOGICAL_COMPARISON_JOIN: Shuffle - hash partition both inputs on join keys
 * - LOGICAL_AGGREGATE_AND_GROUP_BY: Shuffle - hash partition on group keys
 */
class StagePartitioner {
public:
    /**
     * @brief Create a stage partitioner
     * @param default_parallelism Default parallelism for stages
     */
    explicit StagePartitioner(int32_t default_parallelism = 4);

    /**
     * @brief Partition a logical plan into distributed stages
     * @param plan DuckDB logical plan from DuckDBBridge::ParseAndOptimize
     * @return DistributedQueryPlan ready for distributed execution
     */
    arrow::Result<DistributedQueryPlan> Partition(const LogicalPlan& plan);

    /**
     * @brief Set parallelism for all stages
     */
    void SetParallelism(int32_t parallelism);

    /**
     * @brief Get current parallelism setting
     */
    int32_t GetParallelism() const { return default_parallelism_; }

private:
    // Core partitioning logic
    arrow::Status WalkAndPartition(
        duckdb::LogicalOperator& op,
        std::string& current_stage_id,
        std::vector<ExecutionStage>& stages,
        std::unordered_map<std::string, ShuffleSpec>& shuffles);

    // Create a new execution stage
    std::string CreateStage(
        std::vector<ExecutionStage>& stages,
        const std::vector<std::string>& input_shuffles,
        bool is_source);

    // Extract operator information into serializable form
    arrow::Result<LogicalOperatorNode> ExtractOperatorNode(
        duckdb::LogicalOperator& op);

    // Operator-specific extraction
    arrow::Result<LogicalOperatorNode> ExtractTableScan(
        duckdb::LogicalOperator& op);
    arrow::Result<LogicalOperatorNode> ExtractFilter(
        duckdb::LogicalOperator& op);
    arrow::Result<LogicalOperatorNode> ExtractProjection(
        duckdb::LogicalOperator& op);
    arrow::Result<LogicalOperatorNode> ExtractJoin(
        duckdb::LogicalOperator& op);
    arrow::Result<LogicalOperatorNode> ExtractAggregate(
        duckdb::LogicalOperator& op);

    // Create shuffle specification
    ShuffleSpec CreateShuffleSpec(
        ShuffleType type,
        const std::vector<std::string>& partition_keys,
        const std::shared_ptr<arrow::Schema>& schema,
        const std::string& producer_stage_id);

    // Determine execution waves based on dependencies
    std::vector<std::vector<std::string>> ComputeExecutionWaves(
        const std::vector<ExecutionStage>& stages);

    // Check if an operator requires a shuffle
    bool RequiresShuffle(duckdb::LogicalOperatorType type) const;

    // Get shuffle partition keys for an operator
    std::vector<std::string> GetShuffleKeys(duckdb::LogicalOperator& op) const;

    // Settings
    int32_t default_parallelism_;

    // ID generation
    int32_t next_stage_id_ = 0;
    int32_t next_shuffle_id_ = 0;

    std::string GenerateStageId();
    std::string GenerateShuffleId();
};

} // namespace sql
} // namespace sabot_sql
