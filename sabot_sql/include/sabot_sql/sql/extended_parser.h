#pragma once

#include <memory>
#include <string>
#include <vector>
#include <unordered_map>
#include <arrow/api.h>
#include <arrow/result.h>

// Vendored DuckDB includes
#include "duckdb/duckdb.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/planner.hpp"
#include "duckdb/optimizer/optimizer.hpp"

namespace sabot_sql {
namespace sql {

// Forward declarations
class SQLOperatorTranslator;
struct LogicalPlan;

/**
 * @brief Configuration for extended SQL parser
 */
struct ExtendedParserConfig {
    bool enable_flink_syntax = true;
    bool enable_questdb_syntax = true;
    bool enable_streaming_sql = true;
    bool enable_time_series_functions = true;
    std::string database_path = ":memory:";
};

/**
 * @brief Extended SQL Parser that builds on vendored DuckDB
 *
 * This parser extends DuckDB's parser with:
 * - Flink streaming SQL constructs (TUMBLE, HOP, SESSION windows)
 * - QuestDB time-series functions
 * - Custom Sabot-specific extensions
 * - Direct integration with Sabot operators
 */
class ExtendedSQLParser {
public:
    /**
     * @brief Create an ExtendedSQLParser
     * @param config Configuration for the parser
     */
    static arrow::Result<std::shared_ptr<ExtendedSQLParser>>
        Create(const ExtendedParserConfig& config = {});

    ~ExtendedSQLParser();

    /**
     * @brief Parse extended SQL and get optimized logical plan
     * @param sql SQL query string with extensions
     * @return Optimized logical plan
     */
    arrow::Result<LogicalPlan> ParseExtendedSQL(const std::string& sql);

    /**
     * @brief Register an Arrow table
     * @param table_name Name to register the table as
     * @param table Arrow table to register
     */
    arrow::Status RegisterTable(const std::string& table_name,
                                const std::shared_ptr<arrow::Table>& table);

    /**
     * @brief Get DuckDB connection (for advanced use)
     */
    duckdb::Connection& GetConnection() { return *connection_; }

    /**
     * @brief Check if a table exists
     */
    bool TableExists(const std::string& table_name);

private:
    explicit ExtendedSQLParser(const ExtendedParserConfig& config);

    ExtendedParserConfig config_;
    std::unique_ptr<duckdb::DuckDB> database_;
    std::unique_ptr<duckdb::Connection> connection_;
    std::unordered_map<std::string, std::shared_ptr<arrow::Table>> registered_tables_;

    // Extension patterns
    std::unordered_map<std::string, std::string> flink_patterns_;
    std::unordered_map<std::string, std::string> questdb_patterns_;
    std::unordered_map<std::string, std::string> streaming_patterns_;

    void InitializePatterns();

    // Preprocess SQL to convert extensions to DuckDB-compatible SQL
    arrow::Result<std::string> PreprocessSQL(const std::string& sql);

    // Helper to extract logical plan from DuckDB
    LogicalPlan ExtractLogicalPlan(std::unique_ptr<duckdb::LogicalOperator> op);
    void AnalyzePlanFeatures(LogicalPlan& plan);

    // Internal table registration
    arrow::Status RegisterArrowTableInternal(
        const std::string& table_name,
        const std::shared_ptr<arrow::Table>& table);
};

} // namespace sql
} // namespace sabot_sql
