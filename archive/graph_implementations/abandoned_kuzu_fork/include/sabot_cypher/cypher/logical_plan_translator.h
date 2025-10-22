#pragma once

#include <arrow/api.h>
#include <memory>
#include <string>
#include <vector>
#include <map>

// Forward declare Kuzu types
namespace kuzu {
    namespace planner { 
        class LogicalPlan; 
        class LogicalOperator;
    }
}

namespace sabot_cypher {
namespace cypher {

// Arrow operator descriptor (describes physical operators to execute)
struct ArrowOperatorDesc {
    std::string type;  // "Scan", "Filter", "Project", "Join", "Aggregate", etc.
    std::map<std::string, std::string> params;
    bool is_stateful;
    bool is_broadcast;
    
    ArrowOperatorDesc() : is_stateful(false), is_broadcast(false) {}
};

// Arrow physical plan (operator DAG)
struct ArrowPlan {
    std::vector<ArrowOperatorDesc> operators;
    std::shared_ptr<arrow::Schema> output_schema;
    
    // Metadata
    bool has_joins;
    bool has_aggregates;
    bool has_filters;
    std::vector<std::string> join_keys;
    std::vector<std::string> group_by_keys;
    
    ArrowPlan() : has_joins(false), has_aggregates(false), has_filters(false) {}
};

// Translator: Kuzu LogicalPlan → Arrow Physical Plan
class LogicalPlanTranslator {
public:
    static arrow::Result<std::shared_ptr<LogicalPlanTranslator>> Create();
    
    // Main translation entry point
    arrow::Result<std::shared_ptr<ArrowPlan>> Translate(
        const kuzu::planner::LogicalPlan& logical_plan);
    
private:
    LogicalPlanTranslator() = default;
    
    // Visitor methods for different logical operators
    arrow::Status VisitOperator(
        const kuzu::planner::LogicalOperator& op,
        ArrowPlan& plan);
    
    // Specific operator translators
    arrow::Status TranslateScan(
        const kuzu::planner::LogicalOperator& op,
        ArrowPlan& plan);
    
    arrow::Status TranslateFilter(
        const kuzu::planner::LogicalOperator& op,
        ArrowPlan& plan);
    
    arrow::Status TranslateProject(
        const kuzu::planner::LogicalOperator& op,
        ArrowPlan& plan);
    
    arrow::Status TranslateHashJoin(
        const kuzu::planner::LogicalOperator& op,
        ArrowPlan& plan);
    
    arrow::Status TranslateAggregate(
        const kuzu::planner::LogicalOperator& op,
        ArrowPlan& plan);
    
    arrow::Status TranslateOrderBy(
        const kuzu::planner::LogicalOperator& op,
        ArrowPlan& plan);
    
    arrow::Status TranslateLimit(
        const kuzu::planner::LogicalOperator& op,
        ArrowPlan& plan);
    
    arrow::Status TranslateExtend(
        const kuzu::planner::LogicalOperator& op,
        ArrowPlan& plan);
};

}}  // namespace sabot_cypher::cypher

