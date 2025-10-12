#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <arrow/api.h>
#include <arrow/result.h>
#include "sabot_sql/sql/duckdb_bridge.h"
#include "sabot_sql/sql/sql_operator_translator.h"
#include "sabot_sql/operators/operator.h"
#include "sabot_sql/operators/cte.h"
#include "sabot_sql/execution/executor.h"

namespace sabot_sql {
namespace sql {

/**
 * @brief Execution mode for SQL queries
 */
enum class ExecutionMode {
    LOCAL,          // Single-threaded local execution
    LOCAL_PARALLEL, // Multi-threaded local execution with morsels
    DISTRIBUTED     // Distributed execution across agents
};

/**
 * @brief SQL Query Engine Configuration
 */
struct SQLQueryEngineConfig {
    ExecutionMode execution_mode = ExecutionMode::LOCAL;
    int num_workers = 4;                    // For parallel/distributed execution
    size_t morsel_size_kb = 64;            // Morsel size for parallel execution
    std::string database_path = ":memory:"; // DuckDB database path
    bool explain_mode = false;              // Return EXPLAIN instead of results
    bool analyze_mode = false;              // Return EXPLAIN ANALYZE with stats
};

/**
 * @brief SQL Query Engine
 * 
 * Main interface for SQL query execution using DuckDB parser/optimizer
 * and Sabot morsel operators.
 * 
 * Features:
 * - Parse SQL with DuckDB
 * - Optimize with DuckDB
 * - Translate to Sabot operators
 * - Execute with morsel parallelism
 * - Support CTEs and subqueries
 * - Local and distributed execution
 * 
 * Example:
 *   auto engine = SQLQueryEngine::Create();
 *   engine->RegisterTable("orders", orders_table);
 *   auto result = engine->Execute("SELECT * FROM orders WHERE amount > 1000");
 */
class SQLQueryEngine {
public:
    /**
     * @brief Create a SQL query engine
     */
    static arrow::Result<std::shared_ptr<SQLQueryEngine>> 
        Create(const SQLQueryEngineConfig& config = SQLQueryEngineConfig());
    
    ~SQLQueryEngine();
    
    /**
     * @brief Register an Arrow table for querying
     * @param table_name Name to register the table as
     * @param table Arrow table data
     */
    arrow::Status RegisterTable(const std::string& table_name,
                                const std::shared_ptr<arrow::Table>& table);
    
    /**
     * @brief Register a table from file (CSV, Parquet, Arrow IPC)
     * @param table_name Name to register the table as
     * @param file_path Path to data file
     */
    arrow::Status RegisterTableFromFile(const std::string& table_name,
                                        const std::string& file_path);
    
    /**
     * @brief Execute SQL query and return results
     * @param sql SQL query string
     * @return Arrow table with results
     */
    arrow::Result<std::shared_ptr<arrow::Table>> 
        Execute(const std::string& sql);
    
    /**
     * @brief Execute SQL query and stream results batch-by-batch
     * @param sql SQL query string
     * @return Iterator over result batches
     */
    arrow::Result<std::shared_ptr<Operator>> 
        ExecuteStreaming(const std::string& sql);
    
    /**
     * @brief Get query execution plan (EXPLAIN)
     * @param sql SQL query string
     * @return String representation of execution plan
     */
    arrow::Result<std::string> 
        Explain(const std::string& sql);
    
    /**
     * @brief Get query execution plan with statistics (EXPLAIN ANALYZE)
     * @param sql SQL query string
     * @return Execution plan with runtime statistics
     */
    arrow::Result<std::string> 
        ExplainAnalyze(const std::string& sql);
    
    /**
     * @brief Check if a table is registered
     */
    bool HasTable(const std::string& table_name) const;
    
    /**
     * @brief Get table schema
     */
    arrow::Result<std::shared_ptr<arrow::Schema>> 
        GetTableSchema(const std::string& table_name) const;
    
    /**
     * @brief Get list of registered tables
     */
    std::vector<std::string> GetTableNames() const;
    
    /**
     * @brief Get execution mode
     */
    ExecutionMode GetExecutionMode() const { return config_.execution_mode; }
    
    /**
     * @brief Set execution mode
     */
    void SetExecutionMode(ExecutionMode mode) { config_.execution_mode = mode; }
    
    /**
     * @brief Get query statistics from last execution
     */
    std::unordered_map<std::string, std::string> GetLastQueryStats() const;
    
private:
    SQLQueryEngine(const SQLQueryEngineConfig& config,
                   std::shared_ptr<DuckDBBridge> bridge,
                   std::unique_ptr<SQLOperatorTranslator> translator,
                   std::unique_ptr<QueryExecutor> executor);
    
    // Query execution pipeline
    arrow::Result<std::shared_ptr<Operator>> 
        CreateQueryPlan(const std::string& sql);
    
    arrow::Result<std::shared_ptr<Operator>> 
        OptimizeQueryPlan(std::shared_ptr<Operator> plan);
    
    std::string GenerateExplainOutput(const Operator& root_op);
    
    // Configuration
    SQLQueryEngineConfig config_;
    
    // Components
    std::shared_ptr<DuckDBBridge> duck_bridge_;
    std::unique_ptr<SQLOperatorTranslator> translator_;
    std::unique_ptr<QueryExecutor> executor_;
    
    // CTE registry for current query
    CTERegistry cte_registry_;
    
    // Registered tables
    std::unordered_map<std::string, std::shared_ptr<arrow::Table>> tables_;
    
    // Statistics from last query
    std::unordered_map<std::string, std::string> last_query_stats_;
};

} // namespace sql
} // namespace sabot_sql

