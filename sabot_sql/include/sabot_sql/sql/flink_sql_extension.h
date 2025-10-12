#pragma once

#include <memory>
#include <string>
#include <vector>
#include <unordered_map>
#include <arrow/api.h>
#include <arrow/result.h>

namespace sabot_sql {
namespace sql {

/**
 * @brief Flink SQL extension for SabotSQL
 * 
 * Adds support for Flink streaming SQL constructs:
 * - TUMBLE, HOP, SESSION windows
 * - WATERMARK definitions
 * - OVER clauses for window functions
 * - Streaming-specific functions
 */
class FlinkSQLExtension {
public:
    /**
     * @brief Create a Flink SQL extension
     */
    static arrow::Result<std::shared_ptr<FlinkSQLExtension>> Create();
    
    ~FlinkSQLExtension() = default;
    
    /**
     * @brief Preprocess Flink SQL to SabotSQL-compatible SQL
     * @param flink_sql Flink SQL query
     * @return Preprocessed SQL
     */
    arrow::Result<std::string> PreprocessFlinkSQL(const std::string& flink_sql);
    
    /**
     * @brief Check if SQL contains Flink constructs
     * @param sql SQL query to check
     * @return True if contains Flink constructs
     */
    bool ContainsFlinkConstructs(const std::string& sql);
    
    /**
     * @brief Extract window specifications from SQL
     * @param sql SQL query
     * @return Window specifications
     */
    arrow::Result<std::vector<std::string>> 
        ExtractWindowSpecifications(const std::string& sql);
    
    /**
     * @brief Extract watermark definitions from SQL
     * @param sql SQL query
     * @return Watermark definitions
     */
    arrow::Result<std::vector<std::string>> 
        ExtractWatermarkDefinitions(const std::string& sql);

private:
    FlinkSQLExtension() = default;
    
    // Flink SQL patterns
    std::unordered_map<std::string, std::string> flink_patterns_;
    std::vector<std::string> window_functions_;
    std::vector<std::string> streaming_functions_;
    
    void InitializePatterns();
    
    // Pattern matching helpers
    bool MatchesPattern(const std::string& sql, const std::string& pattern);
    std::string ReplacePattern(const std::string& sql, const std::string& pattern, const std::string& replacement);
};

/**
 * @brief QuestDB SQL extension for SabotSQL
 * 
 * Adds support for QuestDB time-series SQL constructs:
 * - SAMPLE BY for time-series grouping
 * - LATEST BY for latest records per group
 * - ASOF JOIN for time-series joins
 * - Time-series specific functions
 */
class QuestDBSQLExtension {
public:
    /**
     * @brief Create a QuestDB SQL extension
     */
    static arrow::Result<std::shared_ptr<QuestDBSQLExtension>> Create();
    
    ~QuestDBSQLExtension() = default;
    
    /**
     * @brief Preprocess QuestDB SQL to SabotSQL-compatible SQL
     * @param questdb_sql QuestDB SQL query
     * @return Preprocessed SQL
     */
    arrow::Result<std::string> PreprocessQuestDBSQL(const std::string& questdb_sql);
    
    /**
     * @brief Check if SQL contains QuestDB constructs
     * @param sql SQL query to check
     * @return True if contains QuestDB constructs
     */
    bool ContainsQuestDBConstructs(const std::string& sql);
    
    /**
     * @brief Extract SAMPLE BY clauses from SQL
     * @param sql SQL query
     * @return SAMPLE BY specifications
     */
    arrow::Result<std::vector<std::string>> 
        ExtractSampleByClauses(const std::string& sql);
    
    /**
     * @brief Extract LATEST BY clauses from SQL
     * @param sql SQL query
     * @return LATEST BY specifications
     */
    arrow::Result<std::vector<std::string>> 
        ExtractLatestByClauses(const std::string& sql);

private:
    QuestDBSQLExtension() = default;
    
    // QuestDB SQL patterns
    std::unordered_map<std::string, std::string> questdb_patterns_;
    std::vector<std::string> timeseries_functions_;
    
    void InitializePatterns();
    
    // Pattern matching helpers
    bool MatchesPattern(const std::string& sql, const std::string& pattern);
    std::string ReplacePattern(const std::string& sql, const std::string& pattern, const std::string& replacement);
};

} // namespace sql
} // namespace sabot_sql
