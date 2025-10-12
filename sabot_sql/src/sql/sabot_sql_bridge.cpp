#include "sabot_sql/sql/sabot_sql_bridge.h"
#include "sabot_sql/sql/duckdb_bridge.h" // For LogicalPlan definition
#include <regex>
#include <sstream>
#include <algorithm>

namespace sabot_sql {
namespace sql {

SabotSQLBridge::SabotSQLBridge(const std::string& database_path)
    : database_(std::make_unique<sabot_sql::Database>(database_path))
    , connection_(std::make_unique<sabot_sql::Connection>(*database_)) {
    
    // Create morsel executor
    auto executor_result = execution::MorselExecutor::Create();
    if (executor_result.ok()) {
        morsel_executor_ = executor_result.ValueOrDie();
    }
}

SabotSQLBridge::~SabotSQLBridge() = default;

arrow::Result<std::shared_ptr<SabotSQLBridge>> 
SabotSQLBridge::Create(const std::string& database_path) {
    try {
        return std::shared_ptr<SabotSQLBridge>(new SabotSQLBridge(database_path));
    } catch (const std::exception& e) {
        return arrow::Status::IOError(
            "Failed to create SabotSQL bridge: " + std::string(e.what()));
    }
}

arrow::Status SabotSQLBridge::RegisterTable(
    const std::string& table_name,
    const std::shared_ptr<arrow::Table>& table) {
    
    // Store locally
    registered_tables_[table_name] = table;
    
    // Register in SabotSQL core
    ARROW_RETURN_NOT_OK(RegisterArrowTableInternal(table_name, table));
    
    return arrow::Status::OK();
}

arrow::Status SabotSQLBridge::RegisterArrowTableInternal(
    const std::string& table_name,
    const std::shared_ptr<arrow::Table>& table) {
    
    try {
        // Register Arrow table directly in SabotSQL core
        connection_->TableFunction("arrow_scan", {sabot_sql::Value::POINTER((uintptr_t)&table)})
            ->CreateView(table_name, true, true);
        
        return arrow::Status::OK();
    } catch (const std::exception& e) {
        return arrow::Status::IOError(
            "Failed to register table: " + std::string(e.what()));
    }
}

arrow::Result<LogicalPlan> 
SabotSQLBridge::ParseAndOptimize(const std::string& sql) {
    try {
        // Parse SQL
        sabot_sql::Parser parser;
        parser.ParseQuery(sql);
        
        if (parser.statements.empty()) {
            return arrow::Status::Invalid("Empty query");
        }
        
        // Bind and plan
        auto binder = sabot_sql::Binder::CreateBinder(*connection_->context);
        binder->BindStatement(*parser.statements[0]);
        
        sabot_sql::Planner planner(*connection_->context);
        planner.CreatePlan(std::move(parser.statements[0]));
        
        // Optimize
        sabot_sql::Optimizer optimizer(*planner.binder, *connection_->context);
        auto optimized_plan = optimizer.Optimize(std::move(planner.plan));
        
        // Extract logical plan
        LogicalPlan plan = ExtractLogicalPlan(std::move(optimized_plan));
        
        // Analyze plan features
        AnalyzePlanFeatures(plan);
        
        return plan;
        
    } catch (const std::exception& e) {
        return arrow::Status::Invalid(
            "Failed to parse/optimize SQL: " + std::string(e.what()));
    }
}

arrow::Result<std::shared_ptr<arrow::Table>> 
SabotSQLBridge::ExecuteSQL(const std::string& sql) {
    try {
        // Parse and optimize
        ARROW_ASSIGN_OR_RAISE(auto plan, ParseAndOptimize(sql));
        
        // Execute using morsel executor
        if (morsel_executor_) {
            return morsel_executor_->ExecutePlan(plan.root_operator);
        } else {
            return arrow::Status::NotImplemented("Morsel executor not available");
        }
        
    } catch (const std::exception& e) {
        return arrow::Status::Invalid(
            "Failed to execute SQL: " + std::string(e.what()));
    }
}

std::function<arrow::Result<std::shared_ptr<arrow::RecordBatch>>()>
SabotSQLBridge::ExecuteSQLStreaming(const std::string& sql) {
    try {
        // Parse and optimize
        auto plan_result = ParseAndOptimize(sql);
        if (!plan_result.ok()) {
            return [error = plan_result.status()]() -> arrow::Result<std::shared_ptr<arrow::RecordBatch>> {
                return error;
            };
        }
        
        auto plan = plan_result.ValueOrDie();
        
        // Execute using morsel executor
        if (morsel_executor_) {
            return morsel_executor_->ExecutePlanStreaming(plan.root_operator);
        } else {
            return []() -> arrow::Result<std::shared_ptr<arrow::RecordBatch>> {
                return arrow::Status::NotImplemented("Morsel executor not available");
            };
        }
        
    } catch (const std::exception& e) {
        return [error = arrow::Status::Invalid("Failed to execute SQL: " + std::string(e.what()))]() -> arrow::Result<std::shared_ptr<arrow::RecordBatch>> {
            return error;
        };
    }
}

LogicalPlan SabotSQLBridge::ExtractLogicalPlan(
    std::unique_ptr<sabot_sql::LogicalOperator> op) {
    
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

void SabotSQLBridge::AnalyzePlanFeatures(LogicalPlan& plan) {
    if (!plan.root_operator) return;
    
    // Simple feature detection based on operator types
    // This is a simplified version - in practice, you'd want to traverse the tree
    auto op_type = plan.root_operator->type;
    
    switch (op_type) {
        case sabot_sql::LogicalOperatorType::LOGICAL_JOIN:
        case sabot_sql::LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
        case sabot_sql::LogicalOperatorType::LOGICAL_CROSS_PRODUCT:
            plan.has_joins = true;
            break;
        case sabot_sql::LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY:
        case sabot_sql::LogicalOperatorType::LOGICAL_WINDOW:
            plan.has_aggregates = true;
            break;
        case sabot_sql::LogicalOperatorType::LOGICAL_SUBQUERY:
            plan.has_subqueries = true;
            break;
        case sabot_sql::LogicalOperatorType::LOGICAL_CTE_REF:
        case sabot_sql::LogicalOperatorType::LOGICAL_RECURSIVE_CTE:
            plan.has_ctes = true;
            break;
        case sabot_sql::LogicalOperatorType::LOGICAL_WINDOW:
            plan.has_windows = true;
            break;
        default:
            break;
    }
}

bool SabotSQLBridge::TableExists(const std::string& table_name) {
    return registered_tables_.find(table_name) != registered_tables_.end();
}

} // namespace sql
} // namespace sabot_sql
