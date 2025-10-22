#pragma once

#include <memory>
#include <string>
#include <vector>
#include <unordered_map>
#include <arrow/api.h>
#include <arrow/result.h>

// Forward declarations from sabot_sql_core
namespace sabot_sql {
    class SabotSQL;  // Main database class (NOT Database!)
    class Connection;
    class LogicalOperator;
}

// SabotSQL execution
#include "sabot_sql/execution/morsel_executor.h"

namespace sabot_sql {
namespace sql {

// Forward declarations
class SQLOperatorTranslator;
struct LogicalPlan;

/**
 * @brief Bridge between SabotSQL core and Sabot's morsel execution
 * 
 * This class provides:
 * - SQL parsing via SabotSQL core
 * - Logical plan optimization via SabotSQL core
 * - Morsel-driven execution via Sabot's execution engine
 * - Type conversion between SabotSQL and Arrow
 */
class SabotSQLBridge {
public:
    /**
     * @brief Create a SabotSQL bridge
     * @param database_path Path to database (":memory:" for in-memory)
     */
    static arrow::Result<std::shared_ptr<SabotSQLBridge>> 
        Create(const std::string& database_path = ":memory:");
    
    ~SabotSQLBridge();
    
    /**
     * @brief Register an Arrow table as a SabotSQL table
     * @param table_name Name to register the table as
     * @param table Arrow table to register
     */
    arrow::Status RegisterTable(const std::string& table_name,
                                const std::shared_ptr<arrow::Table>& table);
    
    /**
     * @brief Parse SQL and get optimized logical plan
     * @param sql SQL query string
     * @return Optimized logical plan from SabotSQL core
     */
    arrow::Result<LogicalPlan> ParseAndOptimize(const std::string& sql);
    
    /**
     * @brief Execute SQL using Sabot's morsel execution
     * @param sql SQL query string
     * @return Arrow table result
     */
    arrow::Result<std::shared_ptr<arrow::Table>> 
        ExecuteSQL(const std::string& sql);
    
    /**
     * @brief Execute SQL and return streaming batches
     * @param sql SQL query string
     * @return Generator of record batches
     */
    std::function<arrow::Result<std::shared_ptr<arrow::RecordBatch>>()>
        ExecuteSQLStreaming(const std::string& sql);
    
    /**
     * @brief Get SabotSQL connection (for advanced use)
     * @note Not available in minimal parser-only implementation
     */
    // sabot_sql::Connection& GetConnection() { return *connection_; }
    
    /**
     * @brief Check if a table exists
     */
    bool TableExists(const std::string& table_name);

private:
    explicit SabotSQLBridge(const std::string& database_path);

    // Minimal implementation doesn't use database/connection
    // std::unique_ptr<sabot_sql::SabotSQL> database_;
    // std::unique_ptr<sabot_sql::Connection> connection_;
    std::string database_path_;
    std::unordered_map<std::string, std::shared_ptr<arrow::Table>> registered_tables_;

    // Morsel executor for Sabot's execution engine
    std::shared_ptr<execution::MorselExecutor> morsel_executor_;

    // Helper to extract logical plan from SabotSQL core
    LogicalPlan ExtractLogicalPlan(std::unique_ptr<sabot_sql::LogicalOperator> op);
    void AnalyzePlanFeatures(LogicalPlan& plan);

    // Internal table registration
    arrow::Status RegisterArrowTableInternal(
        const std::string& table_name,
        const std::shared_ptr<arrow::Table>& table);
};

} // namespace sql
} // namespace sabot_sql
