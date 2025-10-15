#include "sabot_sql/sql/sabot_operator_translator.h"
#include "sabot_sql/sql/enums.h"
#include <sstream>
#include <algorithm>

namespace sabot_sql {
namespace sql {

arrow::Result<std::shared_ptr<SabotOperatorTranslator>> 
SabotOperatorTranslator::Create() {
    try {
        return std::shared_ptr<SabotOperatorTranslator>(new SabotOperatorTranslator());
    } catch (const std::exception& e) {
        return arrow::Status::IOError(
            "Failed to create Sabot operator translator: " + std::string(e.what()));
    }
}

arrow::Result<std::shared_ptr<void>> 
SabotOperatorTranslator::TranslateToMorselOperators(const LogicalPlan& logical_plan) {
    
    // For now, create a simple morsel plan structure
    // In a real implementation, this would traverse the logical plan tree
    // and convert each operator to appropriate Sabot morsel operators
    
    auto morsel_plan = std::make_shared<MorselPlan>();
    morsel_plan->root_operator = logical_plan.root_operator; // may be null in early scaffolding
    morsel_plan->has_joins = logical_plan.has_joins;
    morsel_plan->has_asof_joins = logical_plan.has_asof_joins;
    morsel_plan->has_aggregates = logical_plan.has_aggregates;
    morsel_plan->has_windows = logical_plan.has_windows;
    if (!logical_plan.window_interval.empty()) {
        morsel_plan->window_interval = logical_plan.window_interval;
    }
    if (!logical_plan.join_key_columns.empty()) {
        morsel_plan->join_key_columns = logical_plan.join_key_columns;
    }
    if (!logical_plan.join_timestamp_column.empty()) {
        morsel_plan->join_timestamp_column = logical_plan.join_timestamp_column;
    }
    
    // Sketch Sabot operator pipeline (planning-level only)
    morsel_plan->operator_pipeline.clear();
    if (morsel_plan->has_joins) {
        // Check if dimension table join (broadcast) or stream-stream join (shuffle)
        bool is_broadcast_join = false; // TODO: detect from logical plan
        
        if (!is_broadcast_join) {
            // Stream-stream join: shuffle to ensure co-partitioning
            morsel_plan->operator_pipeline.push_back("ShuffleRepartitionByKeys");
            MorselPlan::OperatorDescriptor shuffle_desc;
            shuffle_desc.type = "ShuffleRepartitionByKeys";
            shuffle_desc.params["keys"] = morsel_plan->join_key_columns.empty() ? "" : morsel_plan->join_key_columns.front();
            shuffle_desc.is_stateful = false;
            shuffle_desc.is_broadcast = false;
            morsel_plan->operator_descriptors.push_back(shuffle_desc);
        }
        
        if (morsel_plan->has_asof_joins) {
            AppendPartitionByKeys(*morsel_plan);
            AppendTimeSortWithinPartition(*morsel_plan);
            AppendAsOfMergeProbe(*morsel_plan);
            
            MorselPlan::OperatorDescriptor part_desc, sort_desc, asof_desc;
            part_desc.type = "PartitionByKeys";
            part_desc.params["keys"] = morsel_plan->join_key_columns.empty() ? "" : morsel_plan->join_key_columns.front();
            part_desc.is_stateful = morsel_plan->is_streaming; // Stateful if streaming
            
            sort_desc.type = "TimeSortWithinPartition";
            sort_desc.params["ts"] = morsel_plan->join_timestamp_column;
            sort_desc.is_stateful = morsel_plan->is_streaming;
            
            asof_desc.type = "AsOfMergeProbe";
            asof_desc.params["ts"] = morsel_plan->join_timestamp_column;
            asof_desc.is_stateful = morsel_plan->is_streaming;
            
            morsel_plan->operator_descriptors.push_back(part_desc);
            morsel_plan->operator_descriptors.push_back(sort_desc);
            morsel_plan->operator_descriptors.push_back(asof_desc);
        } else {
            AppendPartitionByKeys(*morsel_plan);
            AppendHashJoin(*morsel_plan);
            
            MorselPlan::OperatorDescriptor part_desc, join_desc;
            part_desc.type = "PartitionByKeys";
            part_desc.params["keys"] = morsel_plan->join_key_columns.empty() ? "" : morsel_plan->join_key_columns.front();
            part_desc.is_stateful = false;
            part_desc.is_broadcast = is_broadcast_join;
            
            join_desc.type = "HashJoin";
            join_desc.params["keys"] = morsel_plan->join_key_columns.empty() ? "" : morsel_plan->join_key_columns.front();
            join_desc.is_stateful = morsel_plan->is_streaming; // Streaming joins need state
            join_desc.is_broadcast = is_broadcast_join;
            
            morsel_plan->operator_descriptors.push_back(part_desc);
            morsel_plan->operator_descriptors.push_back(join_desc);
        }
    }
    if (morsel_plan->has_windows) {
        // Shuffle by window/grouping key if needed (not for broadcast joins)
        morsel_plan->operator_pipeline.push_back("ShuffleRepartitionByWindowKey");
        
        MorselPlan::OperatorDescriptor shuffle_desc, proj_desc, groupby_desc;
        
        shuffle_desc.type = "ShuffleRepartitionByWindowKey";
        shuffle_desc.params["interval"] = morsel_plan->window_interval;
        shuffle_desc.is_stateful = false;
        
        proj_desc.type = "ProjectDateTruncForWindows";
        proj_desc.params["interval"] = morsel_plan->window_interval;
        proj_desc.is_stateful = false;
        
        groupby_desc.type = "GroupByWindowFrame";
        groupby_desc.params["interval"] = morsel_plan->window_interval;
        groupby_desc.params["state_backend"] = morsel_plan->state_backend;
        groupby_desc.params["timer_backend"] = morsel_plan->timer_backend;
        groupby_desc.is_stateful = true; // Window aggregations are stateful
        
        morsel_plan->operator_descriptors.push_back(shuffle_desc);
        AppendProjectDateTruncForWindows(*morsel_plan);
        morsel_plan->operator_descriptors.push_back(proj_desc);
        AppendGroupByWindowFrame(*morsel_plan);
        morsel_plan->operator_descriptors.push_back(groupby_desc);
    }

    // Estimate output schema
    ARROW_ASSIGN_OR_RAISE(morsel_plan->output_schema, GetOutputSchema(morsel_plan));
    
    return morsel_plan;
}

arrow::Result<std::shared_ptr<arrow::Schema>> 
SabotOperatorTranslator::GetOutputSchema(std::shared_ptr<void> morsel_plan) {
    // Cast to MorselPlan
    auto plan = std::static_pointer_cast<struct MorselPlan>(morsel_plan);
    
    if (plan->output_schema) {
        return plan->output_schema;
    }
    
    // For now, return a simple schema
    // In a real implementation, this would analyze the morsel operators
    // and determine the actual output schema
    
    std::vector<std::shared_ptr<arrow::Field>> fields = {
        arrow::field("id", arrow::int64()),
        arrow::field("name", arrow::utf8()),
        arrow::field("value", arrow::float64())
    };
    
    return arrow::schema(fields);
}

arrow::Result<std::shared_ptr<arrow::Table>> 
SabotOperatorTranslator::ExecuteMorselPlan(std::shared_ptr<void> morsel_plan) {
    // Cast to MorselPlan
    auto plan = std::static_pointer_cast<struct MorselPlan>(morsel_plan);
    
    // Execution must be routed via Sabot's morsel/shuffle engine only.
    // No vendored physical runtime is used.
    
    if (!plan->output_schema) {
        ARROW_ASSIGN_OR_RAISE(plan->output_schema, GetOutputSchema(morsel_plan));
    }
    
    // Create empty table with the output schema
    std::vector<std::shared_ptr<arrow::Array>> empty_arrays;
    for (const auto& field : plan->output_schema->fields()) {
        // Create empty array for each field
        switch (field->type()->id()) {
            case arrow::Type::INT64: {
                arrow::Int64Builder builder;
                ARROW_ASSIGN_OR_RAISE(auto array, builder.Finish());
                empty_arrays.push_back(array);
                break;
            }
            case arrow::Type::STRING: {
                arrow::StringBuilder builder;
                ARROW_ASSIGN_OR_RAISE(auto array, builder.Finish());
                empty_arrays.push_back(array);
                break;
            }
            case arrow::Type::DOUBLE: {
                arrow::DoubleBuilder builder;
                ARROW_ASSIGN_OR_RAISE(auto array, builder.Finish());
                empty_arrays.push_back(array);
                break;
            }
            default:
                return arrow::Status::NotImplemented("Unsupported field type");
        }
    }
    
    return arrow::Table::Make(plan->output_schema, empty_arrays);
}

// --- Sabot-only execution pipeline helpers ---
void SabotOperatorTranslator::AppendPartitionByKeys(struct MorselPlan& plan) {
    plan.operator_pipeline.push_back("PartitionByKeys");
}

void SabotOperatorTranslator::AppendHashJoin(struct MorselPlan& plan) {
    plan.operator_pipeline.push_back("HashJoin");
}

void SabotOperatorTranslator::AppendTimeSortWithinPartition(struct MorselPlan& plan) {
    plan.operator_pipeline.push_back("TimeSortWithinPartition");
}

void SabotOperatorTranslator::AppendAsOfMergeProbe(struct MorselPlan& plan) {
    plan.operator_pipeline.push_back("AsOfMergeProbe");
}

void SabotOperatorTranslator::AppendProjectDateTruncForWindows(struct MorselPlan& plan) {
    plan.operator_pipeline.push_back("ProjectDateTruncForWindows");
}

void SabotOperatorTranslator::AppendGroupByWindowFrame(struct MorselPlan& plan) {
    plan.operator_pipeline.push_back("GroupByWindowFrame");
}

arrow::Result<std::shared_ptr<void>> 
SabotOperatorTranslator::TranslateTableScan(const std::shared_ptr<void>& logical_op) {
    // TODO: Implement table scan translation
    // This would create a Sabot TableScanOperator
    return logical_op; // Placeholder
}

arrow::Result<std::shared_ptr<void>> 
SabotOperatorTranslator::TranslateFilter(const std::shared_ptr<void>& logical_op) {
    // TODO: Implement filter translation
    // This would create a Sabot FilterOperator
    return logical_op; // Placeholder
}

arrow::Result<std::shared_ptr<void>> 
SabotOperatorTranslator::TranslateProjection(const std::shared_ptr<void>& logical_op) {
    // TODO: Implement projection translation
    // This would create a Sabot ProjectOperator
    return logical_op; // Placeholder
}

arrow::Result<std::shared_ptr<void>> 
SabotOperatorTranslator::TranslateJoin(const std::shared_ptr<void>& logical_op) {
    // TODO: Implement join translation
    // This would create a Sabot HashJoinOperator or NestedLoopJoinOperator
    return logical_op; // Placeholder
}

arrow::Result<std::shared_ptr<void>> 
SabotOperatorTranslator::TranslateAggregate(const std::shared_ptr<void>& logical_op) {
    // TODO: Implement aggregate translation
    // This would create a Sabot GroupByOperator
    return logical_op; // Placeholder
}

arrow::Result<std::shared_ptr<void>> 
SabotOperatorTranslator::TranslateOrderBy(const std::shared_ptr<void>& logical_op) {
    // TODO: Implement order by translation
    // This would create a Sabot SortOperator
    return logical_op; // Placeholder
}

arrow::Result<std::shared_ptr<void>> 
SabotOperatorTranslator::TranslateLimit(const std::shared_ptr<void>& logical_op) {
    // TODO: Implement limit translation
    // This would create a Sabot LimitOperator
    return logical_op; // Placeholder
}

std::string SabotOperatorTranslator::GetOperatorType(const std::shared_ptr<void>& logical_op) {
    // TODO: Implement operator type detection
    return "unknown";
}

arrow::Result<std::vector<std::string>> 
SabotOperatorTranslator::GetColumnNames(const std::shared_ptr<void>& logical_op) {
    // TODO: Implement column name extraction
    return std::vector<std::string>();
}

arrow::Result<std::shared_ptr<arrow::Schema>> 
SabotOperatorTranslator::GetInputSchema(const std::shared_ptr<void>& logical_op) {
    // TODO: Implement input schema extraction
    return arrow::schema({});
}

} // namespace sql
} // namespace sabot_sql
