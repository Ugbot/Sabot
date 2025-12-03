#include "sabot_sql/sql/stage_partitioner.h"

#include <sstream>
#include <queue>
#include <unordered_set>

#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"

namespace sabot_sql {
namespace sql {

StagePartitioner::StagePartitioner(int32_t default_parallelism)
    : default_parallelism_(default_parallelism) {
}

void StagePartitioner::SetParallelism(int32_t parallelism) {
    default_parallelism_ = parallelism;
}

std::string StagePartitioner::GenerateStageId() {
    return "stage_" + std::to_string(next_stage_id_++);
}

std::string StagePartitioner::GenerateShuffleId() {
    return "shuffle_" + std::to_string(next_shuffle_id_++);
}

bool StagePartitioner::RequiresShuffle(duckdb::LogicalOperatorType type) const {
    using duckdb::LogicalOperatorType;
    switch (type) {
        case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
        case LogicalOperatorType::LOGICAL_ANY_JOIN:
        case LogicalOperatorType::LOGICAL_DELIM_JOIN:
        case LogicalOperatorType::LOGICAL_ASOF_JOIN:
            return true;
        case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY:
            return true;  // Always shuffle for aggregates (could optimize for single-partition)
        default:
            return false;
    }
}

std::vector<std::string> StagePartitioner::GetShuffleKeys(
    duckdb::LogicalOperator& op) const {

    using duckdb::LogicalOperatorType;
    std::vector<std::string> keys;

    if (op.type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
        // Extract join keys from the join operator
        auto& join_op = static_cast<duckdb::LogicalComparisonJoin&>(op);
        for (const auto& condition : join_op.conditions) {
            // Left side key
            if (condition.left->type == duckdb::ExpressionType::BOUND_COLUMN_REF) {
                auto& col_ref = static_cast<duckdb::BoundColumnRefExpression&>(*condition.left);
                keys.push_back(col_ref.GetName());
            }
        }
    } else if (op.type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
        auto& agg_op = static_cast<duckdb::LogicalAggregate&>(op);
        for (const auto& group_expr : agg_op.groups) {
            if (group_expr->type == duckdb::ExpressionType::BOUND_COLUMN_REF) {
                auto& col_ref = static_cast<duckdb::BoundColumnRefExpression&>(*group_expr);
                keys.push_back(col_ref.GetName());
            }
        }
    }

    return keys;
}

arrow::Result<DistributedQueryPlan> StagePartitioner::Partition(
    const LogicalPlan& plan) {

    if (!plan.root) {
        return arrow::Status::Invalid("Empty logical plan");
    }

    // Reset ID counters
    next_stage_id_ = 0;
    next_shuffle_id_ = 0;

    DistributedQueryPlan result;
    result.requires_shuffle = plan.has_joins || plan.has_aggregates;
    result.total_parallelism = default_parallelism_;

    // Walk the plan tree and partition into stages
    // We use a bottom-up approach: children are processed first,
    // and shuffle boundaries create new stages.

    std::string current_stage_id;
    ARROW_RETURN_NOT_OK(WalkAndPartition(
        *plan.root,
        current_stage_id,
        result.stages,
        result.shuffles));

    // Mark the final stage as sink
    if (!result.stages.empty()) {
        result.stages.back().is_sink = true;
    }

    // Compute execution waves
    result.execution_waves = ComputeExecutionWaves(result.stages);

    // Set output schema
    ARROW_ASSIGN_OR_RAISE(
        result.output_schema,
        TypeConverter::DuckDBSchemaToArrow(
            plan.column_types, plan.column_names));
    result.output_column_names = plan.column_names;
    result.estimated_total_rows = plan.estimated_cardinality;

    return result;
}

arrow::Status StagePartitioner::WalkAndPartition(
    duckdb::LogicalOperator& op,
    std::string& current_stage_id,
    std::vector<ExecutionStage>& stages,
    std::unordered_map<std::string, ShuffleSpec>& shuffles) {

    using duckdb::LogicalOperatorType;

    // Process children first (bottom-up)
    for (auto& child : op.children) {
        ARROW_RETURN_NOT_OK(WalkAndPartition(
            *child, current_stage_id, stages, shuffles));
    }

    // Check if this operator requires a shuffle
    bool needs_shuffle = RequiresShuffle(op.type);

    // Handle shuffle boundary operators (JOIN, AGGREGATE)
    if (needs_shuffle) {
        // Create shuffle specs for children
        std::vector<std::string> input_shuffle_ids;

        if (op.type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
            // Joins have two children that need to be shuffled
            auto keys = GetShuffleKeys(op);

            // Create shuffles for left and right inputs
            for (size_t i = 0; i < op.children.size() && i < 2; i++) {
                auto& child = *op.children[i];

                // Get schema for shuffle
                std::vector<std::string> col_names;
                for (size_t j = 0; j < child.types.size(); j++) {
                    col_names.push_back("col_" + std::to_string(j));
                }
                auto schema_result = TypeConverter::DuckDBSchemaToArrow(
                    child.types, col_names);
                if (!schema_result.ok()) {
                    return schema_result.status();
                }

                // Find the stage that produces this child
                std::string producer_stage = current_stage_id;
                if (!stages.empty()) {
                    // Mark current stage as producing a shuffle
                    producer_stage = stages.back().stage_id;
                    stages.back().output_shuffle_id = GenerateShuffleId();
                    input_shuffle_ids.push_back(*stages.back().output_shuffle_id);

                    ShuffleSpec spec;
                    spec.shuffle_id = *stages.back().output_shuffle_id;
                    spec.type = ShuffleType::HASH;
                    spec.partition_keys = keys;
                    spec.num_partitions = default_parallelism_;
                    spec.schema = *schema_result;
                    spec.producer_stage_id = producer_stage;
                    shuffles[spec.shuffle_id] = spec;
                }
            }
        } else if (op.type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
            auto keys = GetShuffleKeys(op);

            if (!stages.empty()) {
                // Mark current stage as producing a shuffle
                stages.back().output_shuffle_id = GenerateShuffleId();
                input_shuffle_ids.push_back(*stages.back().output_shuffle_id);

                // Get schema for shuffle
                auto& child = *op.children[0];
                std::vector<std::string> col_names;
                for (size_t j = 0; j < child.types.size(); j++) {
                    col_names.push_back("col_" + std::to_string(j));
                }
                auto schema_result = TypeConverter::DuckDBSchemaToArrow(
                    child.types, col_names);
                if (!schema_result.ok()) {
                    return schema_result.status();
                }

                ShuffleSpec spec;
                spec.shuffle_id = *stages.back().output_shuffle_id;
                spec.type = ShuffleType::HASH;
                spec.partition_keys = keys;
                spec.num_partitions = default_parallelism_;
                spec.schema = *schema_result;
                spec.producer_stage_id = stages.back().stage_id;
                shuffles[spec.shuffle_id] = spec;
            }
        }

        // Create new stage for this operator
        current_stage_id = CreateStage(stages, input_shuffle_ids, false);
    }

    // Handle source operators (TABLE SCAN)
    if (op.type == LogicalOperatorType::LOGICAL_GET) {
        // Create source stage
        current_stage_id = CreateStage(stages, {}, true);
    }

    // If no stage exists yet, create one
    if (current_stage_id.empty()) {
        current_stage_id = CreateStage(stages, {}, false);
    }

    // Extract operator node and add to current stage
    ARROW_ASSIGN_OR_RAISE(auto node, ExtractOperatorNode(op));

    // Find current stage and add operator
    for (auto& stage : stages) {
        if (stage.stage_id == current_stage_id) {
            stage.operators.push_back(std::move(node));
            stage.estimated_rows = op.estimated_cardinality;
            break;
        }
    }

    return arrow::Status::OK();
}

std::string StagePartitioner::CreateStage(
    std::vector<ExecutionStage>& stages,
    const std::vector<std::string>& input_shuffles,
    bool is_source) {

    ExecutionStage stage;
    stage.stage_id = GenerateStageId();
    stage.input_shuffle_ids = input_shuffles;
    stage.parallelism = default_parallelism_;
    stage.is_source = is_source;
    stage.is_sink = false;  // Will be set by caller if needed
    stage.estimated_rows = 0;
    stage.estimated_memory_bytes = 0;

    // Set dependencies based on shuffle inputs
    for (const auto& shuffle_id : input_shuffles) {
        // Find producer stage
        for (const auto& other_stage : stages) {
            if (other_stage.output_shuffle_id &&
                *other_stage.output_shuffle_id == shuffle_id) {
                stage.dependency_stage_ids.push_back(other_stage.stage_id);
            }
        }
    }

    stages.push_back(std::move(stage));
    return stages.back().stage_id;
}

arrow::Result<LogicalOperatorNode> StagePartitioner::ExtractOperatorNode(
    duckdb::LogicalOperator& op) {

    using duckdb::LogicalOperatorType;

    switch (op.type) {
        case LogicalOperatorType::LOGICAL_GET:
            return ExtractTableScan(op);
        case LogicalOperatorType::LOGICAL_FILTER:
            return ExtractFilter(op);
        case LogicalOperatorType::LOGICAL_PROJECTION:
            return ExtractProjection(op);
        case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
        case LogicalOperatorType::LOGICAL_ANY_JOIN:
            return ExtractJoin(op);
        case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY:
            return ExtractAggregate(op);
        default: {
            // Generic extraction for unsupported operators
            LogicalOperatorNode node;
            node.type = op.type;
            node.name = duckdb::LogicalOperatorToString(op.type);
            node.estimated_cardinality = op.estimated_cardinality;

            // Extract output types
            for (size_t i = 0; i < op.types.size(); i++) {
                ARROW_ASSIGN_OR_RAISE(auto arrow_type,
                    TypeConverter::DuckDBToArrow(op.types[i]));
                node.column_types.push_back(arrow_type);
                node.column_names.push_back("col_" + std::to_string(i));
            }

            return node;
        }
    }
}

arrow::Result<LogicalOperatorNode> StagePartitioner::ExtractTableScan(
    duckdb::LogicalOperator& op) {

    auto& get_op = static_cast<duckdb::LogicalGet&>(op);

    LogicalOperatorNode node;
    node.type = op.type;
    node.name = "TableScan";
    node.estimated_cardinality = op.estimated_cardinality;

    // Extract table name
    node.table_name = get_op.GetTable()->name;

    // Extract projected columns
    for (size_t i = 0; i < get_op.names.size(); i++) {
        node.projected_columns.push_back(get_op.names[i]);
        node.column_names.push_back(get_op.names[i]);

        if (i < op.types.size()) {
            ARROW_ASSIGN_OR_RAISE(auto arrow_type,
                TypeConverter::DuckDBToArrow(op.types[i]));
            node.column_types.push_back(arrow_type);
        }
    }

    return node;
}

arrow::Result<LogicalOperatorNode> StagePartitioner::ExtractFilter(
    duckdb::LogicalOperator& op) {

    auto& filter_op = static_cast<duckdb::LogicalFilter&>(op);

    LogicalOperatorNode node;
    node.type = op.type;
    node.name = "Filter";
    node.estimated_cardinality = op.estimated_cardinality;

    // Serialize filter expression(s)
    std::stringstream ss;
    for (size_t i = 0; i < filter_op.expressions.size(); i++) {
        if (i > 0) ss << " AND ";
        ss << filter_op.expressions[i]->ToString();
    }
    node.filter_expression = ss.str();

    // Copy output types from input (filter doesn't change schema)
    for (size_t i = 0; i < op.types.size(); i++) {
        ARROW_ASSIGN_OR_RAISE(auto arrow_type,
            TypeConverter::DuckDBToArrow(op.types[i]));
        node.column_types.push_back(arrow_type);
        node.column_names.push_back("col_" + std::to_string(i));
    }

    return node;
}

arrow::Result<LogicalOperatorNode> StagePartitioner::ExtractProjection(
    duckdb::LogicalOperator& op) {

    auto& proj_op = static_cast<duckdb::LogicalProjection&>(op);

    LogicalOperatorNode node;
    node.type = op.type;
    node.name = "Projection";
    node.estimated_cardinality = op.estimated_cardinality;

    // Extract projection expressions
    for (const auto& expr : proj_op.expressions) {
        if (expr->type == duckdb::ExpressionType::BOUND_COLUMN_REF) {
            auto& col_ref = static_cast<duckdb::BoundColumnRefExpression&>(*expr);
            node.projected_columns.push_back(col_ref.GetName());
        } else {
            // For computed columns, use the expression string
            node.projected_columns.push_back(expr->ToString());
        }
    }

    // Extract output types
    for (size_t i = 0; i < op.types.size(); i++) {
        ARROW_ASSIGN_OR_RAISE(auto arrow_type,
            TypeConverter::DuckDBToArrow(op.types[i]));
        node.column_types.push_back(arrow_type);
        node.column_names.push_back("col_" + std::to_string(i));
    }

    return node;
}

arrow::Result<LogicalOperatorNode> StagePartitioner::ExtractJoin(
    duckdb::LogicalOperator& op) {

    LogicalOperatorNode node;
    node.type = op.type;
    node.name = "HashJoin";
    node.estimated_cardinality = op.estimated_cardinality;

    if (op.type == duckdb::LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
        auto& join_op = static_cast<duckdb::LogicalComparisonJoin&>(op);

        // Map join type
        switch (join_op.join_type) {
            case duckdb::JoinType::INNER:
                node.join_type = "inner";
                break;
            case duckdb::JoinType::LEFT:
                node.join_type = "left";
                break;
            case duckdb::JoinType::RIGHT:
                node.join_type = "right";
                break;
            case duckdb::JoinType::OUTER:
                node.join_type = "outer";
                break;
            case duckdb::JoinType::SEMI:
                node.join_type = "semi";
                break;
            case duckdb::JoinType::ANTI:
                node.join_type = "anti";
                break;
            default:
                node.join_type = "inner";
        }

        // Extract join keys from conditions
        for (const auto& condition : join_op.conditions) {
            if (condition.left->type == duckdb::ExpressionType::BOUND_COLUMN_REF) {
                auto& col_ref = static_cast<duckdb::BoundColumnRefExpression&>(*condition.left);
                node.left_keys.push_back(col_ref.GetName());
            }
            if (condition.right->type == duckdb::ExpressionType::BOUND_COLUMN_REF) {
                auto& col_ref = static_cast<duckdb::BoundColumnRefExpression&>(*condition.right);
                node.right_keys.push_back(col_ref.GetName());
            }
        }
    }

    // Extract output types
    for (size_t i = 0; i < op.types.size(); i++) {
        ARROW_ASSIGN_OR_RAISE(auto arrow_type,
            TypeConverter::DuckDBToArrow(op.types[i]));
        node.column_types.push_back(arrow_type);
        node.column_names.push_back("col_" + std::to_string(i));
    }

    return node;
}

arrow::Result<LogicalOperatorNode> StagePartitioner::ExtractAggregate(
    duckdb::LogicalOperator& op) {

    auto& agg_op = static_cast<duckdb::LogicalAggregate&>(op);

    LogicalOperatorNode node;
    node.type = op.type;
    node.name = "Aggregate";
    node.estimated_cardinality = op.estimated_cardinality;

    // Extract group by keys
    for (const auto& group_expr : agg_op.groups) {
        if (group_expr->type == duckdb::ExpressionType::BOUND_COLUMN_REF) {
            auto& col_ref = static_cast<duckdb::BoundColumnRefExpression&>(*group_expr);
            node.group_by_keys.push_back(col_ref.GetName());
        } else {
            node.group_by_keys.push_back(group_expr->ToString());
        }
    }

    // Extract aggregate functions
    for (const auto& expr : agg_op.expressions) {
        if (expr->type == duckdb::ExpressionType::BOUND_AGGREGATE) {
            auto& func_expr = static_cast<duckdb::BoundFunctionExpression&>(*expr);
            node.aggregate_functions.push_back(func_expr.function.name);

            // Extract columns being aggregated
            for (const auto& child : func_expr.children) {
                if (child->type == duckdb::ExpressionType::BOUND_COLUMN_REF) {
                    auto& col_ref = static_cast<duckdb::BoundColumnRefExpression&>(*child);
                    node.aggregate_columns.push_back(col_ref.GetName());
                }
            }
        }
    }

    // Extract output types
    for (size_t i = 0; i < op.types.size(); i++) {
        ARROW_ASSIGN_OR_RAISE(auto arrow_type,
            TypeConverter::DuckDBToArrow(op.types[i]));
        node.column_types.push_back(arrow_type);
        node.column_names.push_back("col_" + std::to_string(i));
    }

    return node;
}

ShuffleSpec StagePartitioner::CreateShuffleSpec(
    ShuffleType type,
    const std::vector<std::string>& partition_keys,
    const std::shared_ptr<arrow::Schema>& schema,
    const std::string& producer_stage_id) {

    ShuffleSpec spec;
    spec.shuffle_id = GenerateShuffleId();
    spec.type = type;
    spec.partition_keys = partition_keys;
    spec.num_partitions = default_parallelism_;
    spec.schema = schema;
    spec.producer_stage_id = producer_stage_id;

    return spec;
}

std::vector<std::vector<std::string>> StagePartitioner::ComputeExecutionWaves(
    const std::vector<ExecutionStage>& stages) {

    std::vector<std::vector<std::string>> waves;

    if (stages.empty()) {
        return waves;
    }

    // Build dependency graph
    std::unordered_map<std::string, std::unordered_set<std::string>> dependencies;
    std::unordered_map<std::string, int> in_degree;

    for (const auto& stage : stages) {
        dependencies[stage.stage_id] = {};
        in_degree[stage.stage_id] = 0;
    }

    for (const auto& stage : stages) {
        for (const auto& dep : stage.dependency_stage_ids) {
            dependencies[dep].insert(stage.stage_id);
            in_degree[stage.stage_id]++;
        }
    }

    // Kahn's algorithm for topological sort with wave grouping
    std::queue<std::string> ready;

    // Find initial wave (no dependencies)
    for (const auto& stage : stages) {
        if (in_degree[stage.stage_id] == 0) {
            ready.push(stage.stage_id);
        }
    }

    while (!ready.empty()) {
        std::vector<std::string> current_wave;

        // Process all ready stages in this wave
        size_t wave_size = ready.size();
        for (size_t i = 0; i < wave_size; i++) {
            std::string stage_id = ready.front();
            ready.pop();
            current_wave.push_back(stage_id);

            // Update dependents
            for (const auto& dependent : dependencies[stage_id]) {
                in_degree[dependent]--;
                if (in_degree[dependent] == 0) {
                    ready.push(dependent);
                }
            }
        }

        waves.push_back(std::move(current_wave));
    }

    return waves;
}

} // namespace sql
} // namespace sabot_sql
