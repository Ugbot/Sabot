#include "sabot_sql/sql/plan_serializer.h"

namespace sabot_sql {
namespace sql {

std::string PlanSerializer::ShuffleTypeToString(ShuffleType type) {
    switch (type) {
        case ShuffleType::HASH:
            return "HASH";
        case ShuffleType::BROADCAST:
            return "BROADCAST";
        case ShuffleType::ROUND_ROBIN:
            return "ROUND_ROBIN";
        case ShuffleType::RANGE:
            return "RANGE";
        default:
            return "UNKNOWN";
    }
}

std::string PlanSerializer::ArrowTypeToString(
    const std::shared_ptr<arrow::DataType>& type) {
    if (!type) {
        return "null";
    }
    return type->ToString();
}

std::string PlanSerializer::LogicalOperatorTypeToString(
    duckdb::LogicalOperatorType type) {
    using duckdb::LogicalOperatorType;

    switch (type) {
        case LogicalOperatorType::LOGICAL_GET:
            return "TableScan";
        case LogicalOperatorType::LOGICAL_FILTER:
            return "Filter";
        case LogicalOperatorType::LOGICAL_PROJECTION:
            return "Projection";
        case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
        case LogicalOperatorType::LOGICAL_ANY_JOIN:
            return "HashJoin";
        case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY:
            return "Aggregate";
        case LogicalOperatorType::LOGICAL_ORDER_BY:
            return "Sort";
        case LogicalOperatorType::LOGICAL_LIMIT:
            return "Limit";
        case LogicalOperatorType::LOGICAL_DISTINCT:
            return "Distinct";
        case LogicalOperatorType::LOGICAL_UNION:
            return "Union";
        default:
            return duckdb::LogicalOperatorToString(type);
    }
}

arrow::Result<PlanSerializer::SerializedOperator>
PlanSerializer::SerializeOperator(const LogicalOperatorNode& node) {
    SerializedOperator op;

    op.type = LogicalOperatorTypeToString(node.type);
    op.name = node.name;
    op.estimated_cardinality = node.estimated_cardinality;

    // Convert column names and types
    op.column_names = node.column_names;
    for (const auto& type : node.column_types) {
        op.column_types.push_back(ArrowTypeToString(type));
    }

    // Copy operator-specific fields
    op.table_name = node.table_name;
    op.projected_columns = node.projected_columns;
    op.filter_expression = node.filter_expression;
    op.left_keys = node.left_keys;
    op.right_keys = node.right_keys;
    op.join_type = node.join_type;
    op.group_by_keys = node.group_by_keys;
    op.aggregate_functions = node.aggregate_functions;
    op.aggregate_columns = node.aggregate_columns;

    // Convert string params
    for (const auto& [key, value] : node.string_params) {
        op.params[key] = value;
    }

    return op;
}

arrow::Result<PlanSerializer::SerializedShuffle>
PlanSerializer::SerializeShuffle(const ShuffleSpec& spec) {
    SerializedShuffle shuffle;

    shuffle.shuffle_id = spec.shuffle_id;
    shuffle.type = ShuffleTypeToString(spec.type);
    shuffle.partition_keys = spec.partition_keys;
    shuffle.num_partitions = spec.num_partitions;
    shuffle.producer_stage_id = spec.producer_stage_id;
    shuffle.consumer_stage_id = spec.consumer_stage_id;

    // Extract schema info
    if (spec.schema) {
        for (const auto& field : spec.schema->fields()) {
            shuffle.column_names.push_back(field->name());
            shuffle.column_types.push_back(ArrowTypeToString(field->type()));
        }
    }

    return shuffle;
}

arrow::Result<PlanSerializer::SerializedStage>
PlanSerializer::SerializeStage(const ExecutionStage& stage) {
    SerializedStage s;

    s.stage_id = stage.stage_id;
    s.input_shuffle_ids = stage.input_shuffle_ids;
    s.output_shuffle_id = stage.output_shuffle_id.value_or("");
    s.parallelism = stage.parallelism;
    s.is_source = stage.is_source;
    s.is_sink = stage.is_sink;
    s.estimated_rows = stage.estimated_rows;
    s.dependency_stage_ids = stage.dependency_stage_ids;

    // Serialize operators
    for (const auto& op : stage.operators) {
        ARROW_ASSIGN_OR_RAISE(auto serialized_op, SerializeOperator(op));
        s.operators.push_back(std::move(serialized_op));
    }

    return s;
}

arrow::Result<PlanSerializer::SerializedPlan>
PlanSerializer::Serialize(const DistributedQueryPlan& plan) {
    SerializedPlan result;

    result.sql = plan.sql;
    result.requires_shuffle = plan.requires_shuffle;
    result.total_parallelism = plan.total_parallelism;
    result.estimated_total_rows = plan.estimated_total_rows;
    result.execution_waves = plan.execution_waves;
    result.output_column_names = plan.output_column_names;

    // Serialize output schema types
    if (plan.output_schema) {
        for (const auto& field : plan.output_schema->fields()) {
            result.output_column_types.push_back(ArrowTypeToString(field->type()));
        }
    }

    // Serialize stages
    for (const auto& stage : plan.stages) {
        ARROW_ASSIGN_OR_RAISE(auto serialized_stage, SerializeStage(stage));
        result.stages.push_back(std::move(serialized_stage));
    }

    // Serialize shuffles
    for (const auto& [id, spec] : plan.shuffles) {
        ARROW_ASSIGN_OR_RAISE(auto serialized_shuffle, SerializeShuffle(spec));
        result.shuffles[id] = std::move(serialized_shuffle);
    }

    return result;
}

} // namespace sql
} // namespace sabot_sql
