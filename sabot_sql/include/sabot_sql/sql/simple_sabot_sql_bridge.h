#pragma once

#include <memory>
#include <string>
#include <vector>
#include <unordered_map>
#include <regex>
#include <arrow/api.h>
#include <arrow/result.h>
#include "sabot_sql/sql/common_types.h"

namespace sabot_sql {
namespace sql {

/**
 * @brief SabotSQL Bridge with Integrated Extensions
 * 
 * This is the core SabotSQL implementation with Flink and QuestDB SQL extensions
 * built directly into the core. It provides a unified SQL interface that supports
 * standard SQL, Flink SQL, and QuestDB SQL dialects.
 */
class SabotSQLBridge {
public:
    /**
     * @brief Create a SabotSQL bridge with integrated extensions
     */
    static arrow::Result<std::shared_ptr<SabotSQLBridge>> Create();
    
    ~SabotSQLBridge() = default;
    
    /**
     * @brief Register an Arrow table
     * @param table_name Name to register the table as
     * @param table Arrow table to register
     */
    arrow::Status RegisterTable(const std::string& table_name,
                                const std::shared_ptr<arrow::Table>& table);
    
    /**
     * @brief Parse SQL (simplified)
     * @param sql SQL query string
     * @return Logical plan (simplified)
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
     * @brief Check if a table exists
     */
    bool TableExists(const std::string& table_name);

    /**
     * @brief Detect if SQL contains Flink constructs
     */
    bool ContainsFlinkConstructs(const std::string& sql) const;

    /**
     * @brief Detect if SQL contains QuestDB constructs
     */
    bool ContainsQuestDBConstructs(const std::string& sql) const;

    /**
     * @brief Extract window specifications from Flink SQL
     */
    std::vector<std::string> ExtractWindowSpecifications(const std::string& sql) const;

    /**
     * @brief Extract SAMPLE BY clauses from QuestDB SQL
     */
    std::vector<std::string> ExtractSampleByClauses(const std::string& sql) const;

    /**
     * @brief Extract LATEST BY clauses from QuestDB SQL
     */
    std::vector<std::string> ExtractLatestByClauses(const std::string& sql) const;

private:
    SabotSQLBridge() = default;
    
    std::unordered_map<std::string, std::shared_ptr<arrow::Table>> registered_tables_;
    
    // Integrated SQL parsing with extensions
    arrow::Result<LogicalPlan> ParseSQLWithExtensions(const std::string& sql);
    
    // Preprocess Flink SQL constructs
    std::string PreprocessFlinkSQL(const std::string& sql) const;
    
    // Preprocess QuestDB SQL constructs
    std::string PreprocessQuestDBSQL(const std::string& sql) const;
    
    // Simple execution (for testing)
    arrow::Result<std::shared_ptr<arrow::Table>> 
        ExecuteSimpleSQL(const std::string& sql);
    
    // Regex patterns for extension detection
    mutable std::vector<std::regex> flink_patterns_;
    mutable std::vector<std::regex> questdb_patterns_;
    
    // Initialize regex patterns
    void InitializePatterns() const;
    
    // Simple SQL parsing (for testing)
    arrow::Result<LogicalPlan> ParseSimpleSQL(const std::string& sql);
};

} // namespace sql
} // namespace sabot_sql
