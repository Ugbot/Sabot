#include "sabot_sql/sql/sql_operator_translator.h"
#include "sabot_sql/operators/join.h"
#include "sabot_sql/operators/aggregate.h"
#include "sabot_sql/operators/sort.h"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_order.hpp"
#include "duckdb/planner/operator/logical_limit.hpp"
#include "duckdb/planner/operator/logical_distinct.hpp"
#include <arrow/compute/api.h>

namespace sabot_sql {
namespace sql {

SQLOperatorTranslator::SQLOperatorTranslator() = default;
SQLOperatorTranslator::~SQLOperatorTranslator() = default;

void SQLOperatorTranslator::RegisterTable(
    const std::string& table_name,
    std::shared_ptr<arrow::Table> table) {
    tables_[table_name] = table;
}

arrow::Result<std::shared_ptr<Operator>> 
SQLOperatorTranslator::Translate(
    const LogicalPlan& logical_plan,
    TranslationContext& context) {
    
    if (!logical_plan.root) {
        return arrow::Status::Invalid("Empty logical plan");
    }
    
    // Copy translation options to context
    context.use_morsel_operators = use_morsel_operators_;
    context.morsel_size_kb = morsel_size_kb_;
    context.num_workers = num_workers_;
    
    // Copy registered tables to context
    context.tables = tables_;
    
    // Translate the root operator
    return TranslateOperator(*logical_plan.root, context);
}

arrow::Result<std::shared_ptr<Operator>> 
SQLOperatorTranslator::TranslateOperator(
    duckdb::LogicalOperator& op,
    TranslationContext& context) {
    
    using duckdb::LogicalOperatorType;
    
    switch (op.type) {
        case LogicalOperatorType::LOGICAL_GET:
            return TranslateTableScan(op, context);
        
        case LogicalOperatorType::LOGICAL_FILTER:
            return TranslateFilter(op, context);
        
        case LogicalOperatorType::LOGICAL_PROJECTION:
            return TranslateProjection(op, context);
        
        case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
        case LogicalOperatorType::LOGICAL_CROSS_PRODUCT:
        case LogicalOperatorType::LOGICAL_ANY_JOIN:
            return TranslateJoin(op, context);
        
        case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY:
            return TranslateAggregate(op, context);
        
        case LogicalOperatorType::LOGICAL_ORDER_BY:
            return TranslateOrder(op, context);
        
        case LogicalOperatorType::LOGICAL_LIMIT:
            return TranslateLimit(op, context);
        
        case LogicalOperatorType::LOGICAL_DISTINCT:
            return TranslateDistinct(op, context);
        
        case LogicalOperatorType::LOGICAL_CTE_REF:
            return TranslateCTE(op, context);
        
        default:
            return arrow::Status::NotImplemented(
                "Operator type not yet supported: " + op.GetName());
    }
}

arrow::Result<std::shared_ptr<Operator>> 
SQLOperatorTranslator::TranslateTableScan(
    duckdb::LogicalOperator& op,
    TranslationContext& context) {
    
    // Extract table name from DuckDB logical get operator
    auto& get_op = static_cast<duckdb::LogicalGet&>(op);
    std::string table_name = get_op.table_index.ToString();
    
    // Look up the table
    auto it = context.tables.find(table_name);
    if (it == context.tables.end()) {
        return arrow::Status::Invalid(
            "Table not found: " + table_name);
    }
    
    // For now, create a simple scan operator
    // We'll implement TableScanOperator in Phase 3
    // This is a placeholder that returns the table directly
    
    // TODO: Implement proper TableScanOperator with pushdown
    return arrow::Status::NotImplemented(
        "TableScanOperator not yet implemented - see Phase 3");
}

arrow::Result<std::shared_ptr<Operator>> 
SQLOperatorTranslator::TranslateFilter(
    duckdb::LogicalOperator& op,
    TranslationContext& context) {
    
    // Translate child first
    if (op.children.empty()) {
        return arrow::Status::Invalid("Filter has no child operator");
    }
    
    ARROW_ASSIGN_OR_RAISE(auto child, TranslateOperator(*op.children[0], context));
    ARROW_ASSIGN_OR_RAISE(auto child_schema, child->GetOutputSchema());
    
    // Extract filter expression
    if (op.expressions.empty()) {
        return arrow::Status::Invalid("Filter has no expression");
    }
    
    ARROW_ASSIGN_OR_RAISE(
        auto predicate, 
        CreatePredicate(op.expressions[0], child_schema));
    
    // Create filter operator
    return std::shared_ptr<Operator>(
        new FilterOperator(child, predicate, "DuckDB filter"));
}

arrow::Result<std::shared_ptr<Operator>> 
SQLOperatorTranslator::TranslateProjection(
    duckdb::LogicalOperator& op,
    TranslationContext& context) {
    
    // Translate child first
    if (op.children.empty()) {
        return arrow::Status::Invalid("Projection has no child operator");
    }
    
    ARROW_ASSIGN_OR_RAISE(auto child, TranslateOperator(*op.children[0], context));
    
    // Extract column names/indices
    auto column_names = ExtractColumnNames(op);
    
    // Create project operator
    return std::shared_ptr<Operator>(
        new ProjectOperator(child, column_names));
}

arrow::Result<std::shared_ptr<Operator>> 
SQLOperatorTranslator::TranslateJoin(
    duckdb::LogicalOperator& op,
    TranslationContext& context) {
    
    // Translate children
    if (op.children.size() < 2) {
        return arrow::Status::Invalid("Join needs two children");
    }
    
    ARROW_ASSIGN_OR_RAISE(auto left, TranslateOperator(*op.children[0], context));
    ARROW_ASSIGN_OR_RAISE(auto right, TranslateOperator(*op.children[1], context));
    
    // Extract join keys from expressions
    // For now, assume simple equality join on first columns
    std::vector<std::string> left_keys = {"col_0"};  // TODO: Extract from expression
    std::vector<std::string> right_keys = {"col_0"}; // TODO: Extract from expression
    
    // Create hash join operator
    return std::shared_ptr<Operator>(
        new HashJoinOperator(left, right, left_keys, right_keys));
}

arrow::Result<std::shared_ptr<Operator>> 
SQLOperatorTranslator::TranslateAggregate(
    duckdb::LogicalOperator& op,
    TranslationContext& context) {
    
    // Translate child first
    if (op.children.empty()) {
        return arrow::Status::Invalid("Aggregate has no child operator");
    }
    
    ARROW_ASSIGN_OR_RAISE(auto child, TranslateOperator(*op.children[0], context));
    
    // Extract group-by columns and aggregates
    std::vector<std::string> group_by_cols = ExtractColumnNames(op);
    
    // For now, create simple COUNT aggregate
    // TODO: Extract actual aggregate functions from expressions
    std::vector<std::string> agg_cols = {"*"};
    
    // Create GroupBy operator
    auto group_by = std::make_shared<GroupByOperator>(child, group_by_cols);
    
    // Add aggregate on top
    return std::shared_ptr<Operator>(
        new AggregateOperator(group_by, agg_cols, 
                             std::vector<AggregateFunction>{AggregateFunction::COUNT}));
}

arrow::Result<std::shared_ptr<Operator>> 
SQLOperatorTranslator::TranslateOrder(
    duckdb::LogicalOperator& op,
    TranslationContext& context) {
    
    // Translate child first
    if (op.children.empty()) {
        return arrow::Status::Invalid("Order has no child operator");
    }
    
    ARROW_ASSIGN_OR_RAISE(auto child, TranslateOperator(*op.children[0], context));
    
    // Extract sort columns
    auto column_names = ExtractColumnNames(op);
    
    // Create sort operator (ascending by default)
    std::vector<bool> descending(column_names.size(), false);
    
    return std::shared_ptr<Operator>(
        new SortOperator(child, column_names, descending));
}

arrow::Result<std::shared_ptr<Operator>> 
SQLOperatorTranslator::TranslateLimit(
    duckdb::LogicalOperator& op,
    TranslationContext& context) {
    
    // Translate child first
    if (op.children.empty()) {
        return arrow::Status::Invalid("Limit has no child operator");
    }
    
    ARROW_ASSIGN_OR_RAISE(auto child, TranslateOperator(*op.children[0], context));
    
    // Extract limit value (default to 100 if not specified)
    size_t limit = 100; // TODO: Extract from DuckDB limit operator
    
    // Create limit operator
    return std::shared_ptr<Operator>(
        new LimitOperator(child, limit));
}

arrow::Result<std::shared_ptr<Operator>> 
SQLOperatorTranslator::TranslateDistinct(
    duckdb::LogicalOperator& op,
    TranslationContext& context) {
    
    // Translate child first
    if (op.children.empty()) {
        return arrow::Status::Invalid("Distinct has no child operator");
    }
    
    ARROW_ASSIGN_OR_RAISE(auto child, TranslateOperator(*op.children[0], context));
    
    // Create distinct operator
    return std::shared_ptr<Operator>(
        new DistinctOperator(child));
}

arrow::Result<std::shared_ptr<Operator>> 
SQLOperatorTranslator::TranslateCTE(
    duckdb::LogicalOperator& op,
    TranslationContext& context) {
    
    // Extract CTE name
    std::string cte_name = "cte_0"; // TODO: Extract from DuckDB CTE ref operator
    
    // Look up CTE operator in context
    auto it = context.cte_operators.find(cte_name);
    if (it == context.cte_operators.end()) {
        return arrow::Status::Invalid(
            "CTE not found: " + cte_name);
    }
    
    // Return the cached CTE operator
    return it->second;
}

arrow::Result<std::shared_ptr<Operator>> 
SQLOperatorTranslator::TranslateSubquery(
    duckdb::LogicalOperator& op,
    TranslationContext& context) {
    
    // Subqueries are translated as nested operator chains
    // The parent operator will consume the subquery results
    
    return arrow::Status::NotImplemented(
        "Subquery operator not yet implemented - see Phase 3");
}

// Helper functions
arrow::Result<FilterOperator::PredicateFn> 
SQLOperatorTranslator::CreatePredicate(
    duckdb::unique_ptr<duckdb::Expression>& expr,
    const std::shared_ptr<arrow::Schema>& schema) {
    
    // For now, create a simple predicate that accepts all rows
    // TODO: Implement full expression translation
    
    return FilterOperator::PredicateFn([](const std::shared_ptr<arrow::RecordBatch>& batch) 
        -> arrow::Result<std::shared_ptr<arrow::BooleanArray>> {
        
        // Create all-true boolean array
        auto builder = arrow::BooleanBuilder();
        ARROW_RETURN_NOT_OK(builder.Reserve(batch->num_rows()));
        for (int64_t i = 0; i < batch->num_rows(); i++) {
            ARROW_RETURN_NOT_OK(builder.Append(true));
        }
        
        std::shared_ptr<arrow::BooleanArray> result;
        ARROW_RETURN_NOT_OK(builder.Finish(&result));
        return result;
    });
}

std::vector<std::string> 
SQLOperatorTranslator::ExtractColumnNames(duckdb::LogicalOperator& op) {
    // Extract column names from operator
    // For now, return generic names
    // TODO: Implement proper column name extraction
    
    std::vector<std::string> names;
    for (size_t i = 0; i < op.types.size(); i++) {
        names.push_back("col_" + std::to_string(i));
    }
    return names;
}

std::vector<int> 
SQLOperatorTranslator::ExtractColumnIndices(duckdb::LogicalOperator& op) {
    // Extract column indices from operator
    std::vector<int> indices;
    for (size_t i = 0; i < op.types.size(); i++) {
        indices.push_back(static_cast<int>(i));
    }
    return indices;
}

// OperatorChainBuilder implementation
OperatorChainBuilder& OperatorChainBuilder::From(std::shared_ptr<Operator> source) {
    current_ = source;
    return *this;
}

OperatorChainBuilder& OperatorChainBuilder::Filter(
    FilterOperator::PredicateFn predicate,
    const std::string& description) {
    
    current_ = std::make_shared<FilterOperator>(current_, predicate, description);
    return *this;
}

OperatorChainBuilder& OperatorChainBuilder::Project(
    const std::vector<std::string>& columns) {
    
    current_ = std::make_shared<ProjectOperator>(current_, columns);
    return *this;
}

OperatorChainBuilder& OperatorChainBuilder::Join(
    std::shared_ptr<Operator> right,
    const std::string& left_key,
    const std::string& right_key) {
    
    current_ = std::make_shared<HashJoinOperator>(
        current_, right,
        std::vector<std::string>{left_key},
        std::vector<std::string>{right_key});
    return *this;
}

OperatorChainBuilder& OperatorChainBuilder::Aggregate(
    const std::vector<std::string>& group_by,
    const std::vector<std::string>& agg_columns) {
    
    auto group_by_op = std::make_shared<GroupByOperator>(current_, group_by);
    current_ = std::make_shared<AggregateOperator>(
        group_by_op, agg_columns,
        std::vector<AggregateFunction>{AggregateFunction::COUNT});
    return *this;
}

OperatorChainBuilder& OperatorChainBuilder::Limit(size_t limit) {
    current_ = std::make_shared<LimitOperator>(current_, limit);
    return *this;
}

std::shared_ptr<Operator> OperatorChainBuilder::Build() {
    return current_;
}

} // namespace sql
} // namespace sabot_sql

