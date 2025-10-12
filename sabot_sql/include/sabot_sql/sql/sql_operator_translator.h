#pragma once

#include <memory>
#include <string>
#include <vector>
#include <unordered_map>
#include <arrow/api.h>
#include <arrow/result.h>
#include "sabot_sql/operators/operator.h"
#include "sabot_sql/sql/duckdb_bridge.h"
#include "duckdb/planner/logical_operator.hpp"

namespace sabot_sql {
namespace sql {

// Forward declarations
class TableScanOperator;
class CTEOperator;
class SubqueryOperator;

/**
 * @brief Context for SQL operator translation
 * Maintains state during translation of logical plan to physical operators
 */
struct TranslationContext {
    // Registered tables (name -> Arrow table)
    std::unordered_map<std::string, std::shared_ptr<arrow::Table>> tables;
    
    // CTE results (name -> operator producing results)
    std::unordered_map<std::string, std::shared_ptr<Operator>> cte_operators;
    
    // Column name mappings
    std::unordered_map<std::string, std::string> column_aliases;
    
    // Translation options
    bool use_morsel_operators = true;
    size_t morsel_size_kb = 64;
    int num_workers = 4;
};

/**
 * @brief Translates DuckDB logical operators to Sabot physical operators
 * 
 * This is the core of the integration:
 * - Takes DuckDB's optimized logical plan
 * - Translates to Sabot operator tree
 * - Maps operators to use existing Sabot implementations
 * - Creates SQL-specific operators for CTEs, subqueries
 */
class SQLOperatorTranslator {
public:
    SQLOperatorTranslator();
    ~SQLOperatorTranslator();
    
    /**
     * @brief Translate a logical plan to Sabot operators
     * @param logical_plan DuckDB logical plan
     * @param context Translation context with tables, CTEs, etc.
     * @return Root operator of translated plan
     */
    arrow::Result<std::shared_ptr<Operator>> 
        Translate(const LogicalPlan& logical_plan, 
                 TranslationContext& context);
    
    /**
     * @brief Register a table for scanning
     * @param table_name Name of the table
     * @param table Arrow table data
     */
    void RegisterTable(const std::string& table_name,
                      std::shared_ptr<arrow::Table> table);
    
    /**
     * @brief Set translation options
     */
    void SetUseMorselOperators(bool use) { use_morsel_operators_ = use; }
    void SetMorselSizeKB(size_t size_kb) { morsel_size_kb_ = size_kb; }
    void SetNumWorkers(int num_workers) { num_workers_ = num_workers; }
    
private:
    // Main translation dispatcher
    arrow::Result<std::shared_ptr<Operator>> 
        TranslateOperator(duckdb::LogicalOperator& op,
                         TranslationContext& context);
    
    // Per-operator translators
    arrow::Result<std::shared_ptr<Operator>> 
        TranslateTableScan(duckdb::LogicalOperator& op,
                          TranslationContext& context);
    
    arrow::Result<std::shared_ptr<Operator>> 
        TranslateFilter(duckdb::LogicalOperator& op,
                       TranslationContext& context);
    
    arrow::Result<std::shared_ptr<Operator>> 
        TranslateProjection(duckdb::LogicalOperator& op,
                           TranslationContext& context);
    
    arrow::Result<std::shared_ptr<Operator>> 
        TranslateJoin(duckdb::LogicalOperator& op,
                     TranslationContext& context);
    
    arrow::Result<std::shared_ptr<Operator>> 
        TranslateAggregate(duckdb::LogicalOperator& op,
                          TranslationContext& context);
    
    arrow::Result<std::shared_ptr<Operator>> 
        TranslateOrder(duckdb::LogicalOperator& op,
                      TranslationContext& context);
    
    arrow::Result<std::shared_ptr<Operator>> 
        TranslateLimit(duckdb::LogicalOperator& op,
                      TranslationContext& context);
    
    arrow::Result<std::shared_ptr<Operator>> 
        TranslateDistinct(duckdb::LogicalOperator& op,
                         TranslationContext& context);
    
    arrow::Result<std::shared_ptr<Operator>> 
        TranslateCTE(duckdb::LogicalOperator& op,
                    TranslationContext& context);
    
    arrow::Result<std::shared_ptr<Operator>> 
        TranslateSubquery(duckdb::LogicalOperator& op,
                         TranslationContext& context);
    
    // Helper functions
    arrow::Result<FilterOperator::PredicateFn> 
        CreatePredicate(duckdb::unique_ptr<duckdb::Expression>& expr,
                       const std::shared_ptr<arrow::Schema>& schema);
    
    std::vector<std::string> ExtractColumnNames(duckdb::LogicalOperator& op);
    std::vector<int> ExtractColumnIndices(duckdb::LogicalOperator& op);
    
    // Translation state
    std::unordered_map<std::string, std::shared_ptr<arrow::Table>> tables_;
    bool use_morsel_operators_ = true;
    size_t morsel_size_kb_ = 64;
    int num_workers_ = 4;
};

/**
 * @brief Helper class to create operator chains
 */
class OperatorChainBuilder {
public:
    OperatorChainBuilder() = default;
    
    /**
     * @brief Start with a source operator
     */
    OperatorChainBuilder& From(std::shared_ptr<Operator> source);
    
    /**
     * @brief Add a filter
     */
    OperatorChainBuilder& Filter(FilterOperator::PredicateFn predicate,
                                  const std::string& description);
    
    /**
     * @brief Add a projection
     */
    OperatorChainBuilder& Project(const std::vector<std::string>& columns);
    
    /**
     * @brief Add a join
     */
    OperatorChainBuilder& Join(std::shared_ptr<Operator> right,
                               const std::string& left_key,
                               const std::string& right_key);
    
    /**
     * @brief Add aggregation
     */
    OperatorChainBuilder& Aggregate(const std::vector<std::string>& group_by,
                                     const std::vector<std::string>& agg_columns);
    
    /**
     * @brief Add limit
     */
    OperatorChainBuilder& Limit(size_t limit);
    
    /**
     * @brief Build the final operator chain
     */
    std::shared_ptr<Operator> Build();
    
private:
    std::shared_ptr<Operator> current_;
};

} // namespace sql
} // namespace sabot_sql

