#pragma once

#include <memory>
#include <string>
#include <vector>
#include <unordered_map>
#include <arrow/api.h>
#include <arrow/result.h>

// DuckDB includes
#include "duckdb/duckdb.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/common/enums/logical_operator_type.hpp"
#include "duckdb/common/enums/physical_operator_type.hpp"

namespace sabot_sql {
namespace sql {

// Forward declarations
class SQLOperatorTranslator;

/**
 * @brief Converts DuckDB types to Arrow types
 */
class TypeConverter {
public:
    static arrow::Result<std::shared_ptr<arrow::DataType>> 
        DuckDBToArrow(const duckdb::LogicalType& duck_type);
    
    static arrow::Result<duckdb::LogicalType> 
        ArrowToDuckDB(const std::shared_ptr<arrow::DataType>& arrow_type);
    
    static arrow::Result<std::shared_ptr<arrow::Schema>> 
        DuckDBSchemaToArrow(const std::vector<duckdb::LogicalType>& duck_types,
                           const std::vector<std::string>& column_names);
};

/**
 * @brief Logical plan extracted from DuckDB after optimization
 */
struct LogicalPlan {
    std::unique_ptr<duckdb::LogicalOperator> root;
    std::vector<std::string> column_names;
    std::vector<duckdb::LogicalType> column_types;
    duckdb::idx_t estimated_cardinality;
    
    // Plan metadata
    bool has_ctes = false;
    bool has_subqueries = false;
    bool has_aggregates = false;
    bool has_joins = false;
};

/**
 * @brief Physical plan information from DuckDB
 * We extract this but replace operators with Sabot equivalents
 */
struct PhysicalPlanInfo {
    duckdb::PhysicalOperatorType operator_type;
    std::vector<duckdb::LogicalType> output_types;
    std::vector<std::string> output_names;
    duckdb::idx_t estimated_cardinality;
    
    // Operator-specific metadata
    std::unordered_map<std::string, std::string> metadata;
};

/**
 * @brief Bridge between DuckDB and Sabot
 * 
 * This class provides:
 * - SQL parsing via DuckDB
 * - Logical plan optimization via DuckDB
 * - Physical plan extraction (structure only)
 * - Type conversion between DuckDB and Arrow
 */
class DuckDBBridge {
public:
    /**
     * @brief Create a DuckDB bridge
     * @param database_path Path to DuckDB database (":memory:" for in-memory)
     */
    static arrow::Result<std::shared_ptr<DuckDBBridge>> 
        Create(const std::string& database_path = ":memory:");
    
    ~DuckDBBridge();
    
    /**
     * @brief Register an Arrow table as a DuckDB table
     * @param table_name Name to register the table as
     * @param table Arrow table to register
     */
    arrow::Status RegisterTable(const std::string& table_name,
                                const std::shared_ptr<arrow::Table>& table);
    
    /**
     * @brief Parse SQL and get optimized logical plan
     * @param sql SQL query string
     * @return Optimized logical plan from DuckDB
     */
    arrow::Result<LogicalPlan> ParseAndOptimize(const std::string& sql);
    
    /**
     * @brief Extract physical plan structure (for reference only)
     * We don't use DuckDB's physical operators, but we extract their
     * structure to understand the query plan.
     * 
     * @param logical_plan Logical plan to generate physical plan from
     * @return Physical plan information
     */
    arrow::Result<std::vector<PhysicalPlanInfo>> 
        ExtractPhysicalPlanStructure(const LogicalPlan& logical_plan);
    
    /**
     * @brief Execute SQL directly with DuckDB (for comparison/testing)
     * @param sql SQL query string
     * @return Arrow table result
     */
    arrow::Result<std::shared_ptr<arrow::Table>> 
        ExecuteWithDuckDB(const std::string& sql);
    
    /**
     * @brief Get DuckDB connection (for advanced use)
     */
    duckdb::Connection& GetConnection() { return *connection_; }
    
    /**
     * @brief Check if a table exists
     */
    bool TableExists(const std::string& table_name);
    
    /**
     * @brief Get table schema
     */
    arrow::Result<std::shared_ptr<arrow::Schema>> 
        GetTableSchema(const std::string& table_name);
    
private:
    DuckDBBridge(std::unique_ptr<duckdb::DuckDB> database,
                 std::unique_ptr<duckdb::Connection> connection);
    
    // Helper methods
    arrow::Status RegisterArrowTableInternal(
        const std::string& table_name,
        const std::shared_ptr<arrow::Table>& table);
    
    LogicalPlan ExtractLogicalPlan(duckdb::unique_ptr<duckdb::LogicalOperator> root);
    
    void AnalyzePlanFeatures(LogicalPlan& plan);
    
    // DuckDB instances
    std::unique_ptr<duckdb::DuckDB> database_;
    std::unique_ptr<duckdb::Connection> connection_;
    
    // Registered tables
    std::unordered_map<std::string, std::shared_ptr<arrow::Table>> registered_tables_;
};

/**
 * @brief Helper to walk logical operator tree
 */
class LogicalOperatorWalker {
public:
    using VisitorFunction = std::function<void(duckdb::LogicalOperator&)>;
    
    static void Walk(duckdb::LogicalOperator& root, VisitorFunction visitor);
    
    static std::vector<duckdb::LogicalOperator*> 
        FindOperatorsByType(duckdb::LogicalOperator& root, 
                           duckdb::LogicalOperatorType type);
};

} // namespace sql
} // namespace sabot_sql

