// Minimal sabot_sql bridge - parser-only implementation
// This version uses ONLY the parser/planner/optimizer from sabot_sql_core
// WITHOUT requiring Connection/Database classes

#include "sabot_sql/sql/sabot_sql_bridge.h"

// Only include parser header - it includes statement types
#include "sabot_sql/parser/parser.hpp"

#include <regex>
#include <sstream>

namespace sabot_sql {
namespace sql {

// Minimal LogicalPlan definition
struct LogicalPlan {
    std::string query_type;
    std::vector<std::string> table_references;
    bool has_aggregates = false;
    bool has_joins = false;
    bool has_subqueries = false;
    bool has_windows = false;
    bool has_ctes = false;
};

SabotSQLBridge::SabotSQLBridge(const std::string& database_path)
    : database_path_(database_path) {

    // Create morsel executor
    auto executor_result = execution::MorselExecutor::Create();
    if (executor_result.ok()) {
        morsel_executor_ = executor_result.ValueOrDie();
    }
}

SabotSQLBridge::~SabotSQLBridge() = default;

arrow::Result<std::shared_ptr<SabotSQLBridge>>
SabotSQLBridge::Create(const std::string& database_path) {
    // Use new since constructor is private
    return std::shared_ptr<SabotSQLBridge>(new SabotSQLBridge(database_path));
}

arrow::Status SabotSQLBridge::RegisterTable(
    const std::string& table_name,
    const std::shared_ptr<arrow::Table>& table) {

    registered_tables_[table_name] = table;
    return arrow::Status::OK();
}

arrow::Result<LogicalPlan> SabotSQLBridge::ParseAndOptimize(const std::string& sql) {
    LogicalPlan plan;

    try {
        // Use sabot_sql_core parser
        sabot_sql::Parser parser;
        parser.ParseQuery(sql);

        // Use reference to avoid copying unique_ptr vector
        auto& statements = parser.statements;
        if (statements.empty()) {
            return arrow::Status::Invalid("No statements parsed from SQL");
        }

        // Extract basic info from first statement
        auto& stmt = statements[0];
        plan.query_type = sabot_sql::StatementTypeToString(stmt->type);

        // For now, return basic plan
        // In full implementation, would use Binder/Planner/Optimizer
        return plan;

    } catch (const std::exception& e) {
        return arrow::Status::Invalid("SQL parsing failed: ", e.what());
    }
}

arrow::Result<std::shared_ptr<arrow::Table>>
SabotSQLBridge::ExecuteSQL(const std::string& sql) {

    // Parse the SQL
    ARROW_ASSIGN_OR_RAISE(auto plan, ParseAndOptimize(sql));

    // For minimal version, return empty table
    // Full version would translate plan to Sabot operators and execute
    auto schema = arrow::schema({});
    std::vector<std::shared_ptr<arrow::Array>> arrays;
    auto table = arrow::Table::Make(schema, arrays);

    return table;
}

std::function<arrow::Result<std::shared_ptr<arrow::RecordBatch>>()>
SabotSQLBridge::ExecuteSQLStreaming(const std::string& sql) {

    return [this, sql]() -> arrow::Result<std::shared_ptr<arrow::RecordBatch>> {
        // Return nullptr to signal end of stream
        return nullptr;
    };
}

bool SabotSQLBridge::TableExists(const std::string& table_name) {
    return registered_tables_.find(table_name) != registered_tables_.end();
}

LogicalPlan SabotSQLBridge::ExtractLogicalPlan(
    std::unique_ptr<sabot_sql::LogicalOperator> op) {

    LogicalPlan plan;
    // Minimal implementation
    return plan;
}

void SabotSQLBridge::AnalyzePlanFeatures(LogicalPlan& plan) {
    // Minimal implementation
}

arrow::Status SabotSQLBridge::RegisterArrowTableInternal(
    const std::string& table_name,
    const std::shared_ptr<arrow::Table>& table) {

    return RegisterTable(table_name, table);
}

} // namespace sql
} // namespace sabot_sql
