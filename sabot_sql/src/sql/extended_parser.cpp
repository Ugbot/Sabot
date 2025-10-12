#include "sabot_sql/sql/extended_parser.h"
#include "sabot_sql/sql/duckdb_bridge.h" // For LogicalPlan definition
#include <regex>
#include <sstream>
#include <algorithm>

namespace sabot_sql {
namespace sql {

ExtendedSQLParser::ExtendedSQLParser(const ExtendedParserConfig& config)
    : config_(config) {
    
    // Initialize DuckDB
    database_ = std::make_unique<duckdb::DuckDB>(config_.database_path);
    connection_ = std::make_unique<duckdb::Connection>(*database_);
    
    // Initialize extension patterns
    InitializePatterns();
}

ExtendedSQLParser::~ExtendedSQLParser() = default;

arrow::Result<std::shared_ptr<ExtendedSQLParser>> 
ExtendedSQLParser::Create(const ExtendedParserConfig& config) {
    try {
        return std::shared_ptr<ExtendedSQLParser>(
            new ExtendedSQLParser(config));
    } catch (const std::exception& e) {
        return arrow::Status::IOError(
            "Failed to create extended SQL parser: " + std::string(e.what()));
    }
}

void ExtendedSQLParser::InitializePatterns() {
    if (config_.enable_flink_syntax) {
        // Flink streaming patterns
        flink_patterns_["TUMBLE\\s*\\(([^,]+),\\s*INTERVAL\\s*'([^']+)'\\s*\\)"] = 
            "DATE_TRUNC('$2', $1)";
        flink_patterns_["HOP\\s*\\(([^,]+),\\s*INTERVAL\\s*'([^']+)',\\s*INTERVAL\\s*'([^']+)'\\s*\\)"] = 
            "DATE_TRUNC('$2', $1)"; // Simplified for now
        flink_patterns_["SESSION\\s*\\(([^,]+),\\s*INTERVAL\\s*'([^']+)'\\s*\\)"] = 
            "DATE_TRUNC('$2', $1)"; // Simplified for now
        flink_patterns_["CURRENT_TIMESTAMP"] = "NOW()";
        flink_patterns_["CURRENT_DATE"] = "CURRENT_DATE";
        flink_patterns_["CURRENT_TIME"] = "CURRENT_TIME";
    }
    
    if (config_.enable_questdb_syntax) {
        // QuestDB time-series patterns
        questdb_patterns_["SAMPLE\\s+BY\\s+([^\\s]+)"] = "GROUP BY DATE_TRUNC('$1', timestamp_col)";
        questdb_patterns_["LATEST\\s+BY\\s+([^\\s]+)"] = "ORDER BY $1 DESC LIMIT 1";
        questdb_patterns_["ASOF\\s+JOIN"] = "LEFT JOIN"; // Simplified for now
    }
    
    if (config_.enable_streaming_sql) {
        // Streaming SQL patterns
        streaming_patterns_["WATERMARK\\s+FOR\\s+([^\\s]+)\\s+AS\\s+([^\\s]+)"] = 
            "-- WATERMARK: $1 AS $2"; // Comment out for now
        streaming_patterns_["OVER\\s+\\(([^)]+)\\)"] = "OVER ($1)"; // Keep as-is
    }
}

arrow::Result<std::string> 
ExtendedSQLParser::PreprocessSQL(const std::string& sql) {
    std::string processed_sql = sql;
    
    // Apply Flink patterns
    if (config_.enable_flink_syntax) {
        for (const auto& [pattern, replacement] : flink_patterns_) {
            std::regex regex_pattern(pattern, std::regex_constants::icase);
            processed_sql = std::regex_replace(processed_sql, regex_pattern, replacement);
        }
    }
    
    // Apply QuestDB patterns
    if (config_.enable_questdb_syntax) {
        for (const auto& [pattern, replacement] : questdb_patterns_) {
            std::regex regex_pattern(pattern, std::regex_constants::icase);
            processed_sql = std::regex_replace(processed_sql, regex_pattern, replacement);
        }
    }
    
    // Apply streaming patterns
    if (config_.enable_streaming_sql) {
        for (const auto& [pattern, replacement] : streaming_patterns_) {
            std::regex regex_pattern(pattern, std::regex_constants::icase);
            processed_sql = std::regex_replace(processed_sql, regex_pattern, replacement);
        }
    }
    
    return processed_sql;
}

arrow::Result<LogicalPlan> 
ExtendedSQLParser::ParseExtendedSQL(const std::string& sql) {
    try {
        // Preprocess SQL to convert extensions to DuckDB-compatible SQL
        ARROW_ASSIGN_OR_RAISE(auto processed_sql, PreprocessSQL(sql));
        
        // Parse with DuckDB
        duckdb::Parser parser;
        parser.ParseQuery(processed_sql);
        
        if (parser.statements.empty()) {
            return arrow::Status::Invalid("Empty query");
        }
        
        // Bind and plan
        auto binder = duckdb::Binder::CreateBinder(*connection_->context);
        binder->BindStatement(*parser.statements[0]);
        
        duckdb::Planner planner(*connection_->context);
        planner.CreatePlan(std::move(parser.statements[0]));
        
        // Optimize
        duckdb::Optimizer optimizer(*planner.binder, *connection_->context);
        auto optimized_plan = optimizer.Optimize(std::move(planner.plan));
        
        // Extract logical plan
        LogicalPlan plan = ExtractLogicalPlan(std::move(optimized_plan));
        
        // Analyze plan features
        AnalyzePlanFeatures(plan);
        
        return plan;
        
    } catch (const std::exception& e) {
        return arrow::Status::Invalid(
            "Failed to parse extended SQL: " + std::string(e.what()));
    }
}

LogicalPlan ExtendedSQLParser::ExtractLogicalPlan(
    std::unique_ptr<duckdb::LogicalOperator> op) {
    
    LogicalPlan plan;
    plan.root_operator = std::move(op);
    
    // Extract plan information
    plan.has_joins = false;
    plan.has_aggregates = false;
    plan.has_subqueries = false;
    plan.has_ctes = false;
    plan.has_windows = false;
    
    // Analyze the plan structure
    if (plan.root_operator) {
        AnalyzePlanFeatures(plan);
    }
    
    return plan;
}

void ExtendedSQLParser::AnalyzePlanFeatures(LogicalPlan& plan) {
    if (!plan.root_operator) return;
    
    // Simple feature detection based on operator types
    // This is a simplified version - in practice, you'd want to traverse the tree
    auto op_type = plan.root_operator->type;
    
    switch (op_type) {
        case duckdb::LogicalOperatorType::LOGICAL_JOIN:
        case duckdb::LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
        case duckdb::LogicalOperatorType::LOGICAL_CROSS_PRODUCT:
            plan.has_joins = true;
            break;
        case duckdb::LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY:
        case duckdb::LogicalOperatorType::LOGICAL_WINDOW:
            plan.has_aggregates = true;
            break;
        case duckdb::LogicalOperatorType::LOGICAL_SUBQUERY:
            plan.has_subqueries = true;
            break;
        case duckdb::LogicalOperatorType::LOGICAL_CTE_REF:
        case duckdb::LogicalOperatorType::LOGICAL_RECURSIVE_CTE:
            plan.has_ctes = true;
            break;
        case duckdb::LogicalOperatorType::LOGICAL_WINDOW:
            plan.has_windows = true;
            break;
        default:
            break;
    }
}

arrow::Status ExtendedSQLParser::RegisterTable(
    const std::string& table_name,
    const std::shared_ptr<arrow::Table>& table) {
    
    // Store locally
    registered_tables_[table_name] = table;
    
    // Register in DuckDB
    ARROW_RETURN_NOT_OK(RegisterArrowTableInternal(table_name, table));
    
    return arrow::Status::OK();
}

arrow::Status ExtendedSQLParser::RegisterArrowTableInternal(
    const std::string& table_name,
    const std::shared_ptr<arrow::Table>& table) {
    
    try {
        // Register Arrow table directly in DuckDB
        connection_->TableFunction("arrow_scan", {duckdb::Value::POINTER((uintptr_t)&table)})
            ->CreateView(table_name, true, true);
        
        return arrow::Status::OK();
    } catch (const std::exception& e) {
        return arrow::Status::IOError(
            "Failed to register table: " + std::string(e.what()));
    }
}

bool ExtendedSQLParser::TableExists(const std::string& table_name) {
    return registered_tables_.find(table_name) != registered_tables_.end();
}

} // namespace sql
} // namespace sabot_sql
