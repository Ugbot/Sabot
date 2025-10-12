#pragma once

#include <memory>
#include <string>
#include <vector>
#include <unordered_map>
#include <arrow/api.h>
#include <arrow/result.h>

// DuckDB includes
#include "duckdb.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/parser/parser.hpp"
// DuckDB expression includes - removed due to API changes

namespace sabot_sql {
namespace sql {

/**
 * @brief Enhanced SQL Parser extending DuckDB with streaming and time-series constructs
 * 
 * Borrows concepts from:
 * - Flink SQL: Streaming constructs, window functions, time-based operations
 * - QuestDB: Time-series functions, temporal operations, performance optimizations
 * 
 * Key Extensions:
 * 1. Streaming SQL constructs (TUMBLE, HOP, SESSION windows)
 * 2. Time-based functions (CURRENT_TIMESTAMP, INTERVAL arithmetic)
 * 3. Window functions (ROW_NUMBER, RANK, DENSE_RANK, etc.)
 * 4. Time-series functions (SAMPLE BY, LATEST BY, etc.)
 * 5. Event-time vs processing-time semantics
 * 6. Streaming aggregations and joins
 */

/**
 * @brief Streaming SQL constructs borrowed from Flink
 */
enum class StreamingConstruct {
    TUMBLE_WINDOW,      // TUMBLE(time_col, INTERVAL '1' MINUTE)
    HOP_WINDOW,         // HOP(time_col, INTERVAL '30' SECOND, INTERVAL '1' MINUTE)
    SESSION_WINDOW,     // SESSION(time_col, INTERVAL '5' MINUTE)
    CURRENT_TIMESTAMP,  // CURRENT_TIMESTAMP
    CURRENT_DATE,       // CURRENT_DATE
    CURRENT_TIME,       // CURRENT_TIME
    INTERVAL_ARITHMETIC, // INTERVAL '1' HOUR + INTERVAL '30' MINUTE
    EVENT_TIME,         // Event-time semantics
    PROCESSING_TIME     // Processing-time semantics
};

/**
 * @brief Time-series functions borrowed from QuestDB
 */
enum class TimeSeriesFunction {
    SAMPLE_BY,          // SAMPLE BY 1h
    LATEST_BY,          // LATEST BY symbol
    FIRST_BY,           // FIRST BY symbol
    LAST_BY,            // LAST BY symbol
    ASOF_JOIN,          // ASOF JOIN for time-series joins
    FILL_NULL,          // FILL NULL for missing values
    TIMESTAMP_FUNCTION, // timestamp() function
    DATE_TRUNC,         // date_trunc() function
    EXTRACT_TIME        // extract() for time components
};

/**
 * @brief Window function types
 */
enum class WindowFunctionType {
    ROW_NUMBER,         // ROW_NUMBER() OVER (PARTITION BY ... ORDER BY ...)
    RANK,              // RANK() OVER (PARTITION BY ... ORDER BY ...)
    DENSE_RANK,        // DENSE_RANK() OVER (PARTITION BY ... ORDER BY ...)
    PERCENT_RANK,      // PERCENT_RANK() OVER (PARTITION BY ... ORDER BY ...)
    CUME_DIST,         // CUME_DIST() OVER (PARTITION BY ... ORDER BY ...)
    NTILE,             // NTILE(4) OVER (PARTITION BY ... ORDER BY ...)
    LAG,               // LAG(column, 1) OVER (PARTITION BY ... ORDER BY ...)
    LEAD,              // LEAD(column, 1) OVER (PARTITION BY ... ORDER BY ...)
    FIRST_VALUE,       // FIRST_VALUE(column) OVER (PARTITION BY ... ORDER BY ...)
    LAST_VALUE,        // LAST_VALUE(column) OVER (PARTITION BY ... ORDER BY ...)
    NTH_VALUE          // NTH_VALUE(column, 2) OVER (PARTITION BY ... ORDER BY ...)
};

/**
 * @brief Enhanced SQL Parser Configuration
 */
struct EnhancedParserConfig {
    bool enable_streaming_constructs = true;
    bool enable_time_series_functions = true;
    bool enable_window_functions = true;
    bool enable_flink_syntax = true;
    bool enable_questdb_syntax = true;
    bool strict_mode = false;  // Strict SQL compliance
    std::string time_zone = "UTC";
};

/**
 * @brief Enhanced SQL Parser extending DuckDB
 * 
 * This parser extends DuckDB's parser with:
 * 1. Flink SQL streaming constructs
 * 2. QuestDB time-series functions
 * 3. Window functions
 * 4. Time-based operations
 * 5. Event-time semantics
 */
class EnhancedSQLParser {
public:
    /**
     * @brief Create enhanced SQL parser
     */
    static arrow::Result<std::shared_ptr<EnhancedSQLParser>> 
        Create(const EnhancedParserConfig& config = EnhancedParserConfig());
    
    ~EnhancedSQLParser();
    
    /**
     * @brief Parse SQL with enhanced constructs
     * @param sql SQL query string
     * @return Enhanced logical plan
     */
    arrow::Result<LogicalPlan> ParseEnhancedSQL(const std::string& sql);
    
    /**
     * @brief Check if SQL contains streaming constructs
     */
    bool HasStreamingConstructs(const std::string& sql) const;
    
    /**
     * @brief Check if SQL contains time-series functions
     */
    bool HasTimeSeriesFunctions(const std::string& sql) const;
    
    /**
     * @brief Check if SQL contains window functions
     */
    bool HasWindowFunctions(const std::string& sql) const;
    
    /**
     * @brief Get supported streaming constructs
     */
    std::vector<StreamingConstruct> GetSupportedStreamingConstructs() const;
    
    /**
     * @brief Get supported time-series functions
     */
    std::vector<TimeSeriesFunction> GetSupportedTimeSeriesFunctions() const;
    
    /**
     * @brief Get supported window functions
     */
    std::vector<WindowFunctionType> GetSupportedWindowFunctions() const;
    
    /**
     * @brief Validate SQL syntax
     */
    arrow::Status ValidateSQL(const std::string& sql) const;
    
    /**
     * @brief Get SQL syntax help
     */
    std::string GetSyntaxHelp() const;
    
private:
    EnhancedSQLParser(const EnhancedParserConfig& config,
                      std::shared_ptr<duckdb::DuckDB> database,
                      std::unique_ptr<duckdb::Connection> connection);
    
    // Preprocessing methods
    arrow::Result<std::string> PreprocessSQL(const std::string& sql) const;
    
    // Streaming construct handlers
    arrow::Result<std::string> HandleTumbleWindow(const std::string& sql) const;
    arrow::Result<std::string> HandleHopWindow(const std::string& sql) const;
    arrow::Result<std::string> HandleSessionWindow(const std::string& sql) const;
    arrow::Result<std::string> HandleCurrentTimestamp(const std::string& sql) const;
    arrow::Result<std::string> HandleIntervalArithmetic(const std::string& sql) const;
    
    // Time-series function handlers
    arrow::Result<std::string> HandleSampleBy(const std::string& sql) const;
    arrow::Result<std::string> HandleLatestBy(const std::string& sql) const;
    arrow::Result<std::string> HandleAsofJoin(const std::string& sql) const;
    arrow::Result<std::string> HandleFillNull(const std::string& sql) const;
    
    // Window function handlers
    arrow::Result<std::string> HandleRowNumber(const std::string& sql) const;
    arrow::Result<std::string> HandleRank(const std::string& sql) const;
    arrow::Result<std::string> HandleLagLead(const std::string& sql) const;
    
    // Utility methods
    bool ContainsPattern(const std::string& sql, const std::string& pattern) const;
    std::string ReplacePattern(const std::string& sql, const std::string& pattern, 
                              const std::string& replacement) const;
    
    // Configuration
    EnhancedParserConfig config_;
    
    // DuckDB components
    std::shared_ptr<duckdb::DuckDB> database_;
    std::unique_ptr<duckdb::Connection> connection_;
    
    // Pattern matching
    std::unordered_map<std::string, std::string> streaming_patterns_;
    std::unordered_map<std::string, std::string> timeseries_patterns_;
    std::unordered_map<std::string, std::string> window_patterns_;
};

/**
 * @brief SQL Syntax Extensions
 */
namespace sql_extensions {

/**
 * @brief Flink SQL streaming constructs
 */
namespace flink {
    constexpr const char* TUMBLE_WINDOW = "TUMBLE";
    constexpr const char* HOP_WINDOW = "HOP";
    constexpr const char* SESSION_WINDOW = "SESSION";
    constexpr const char* CURRENT_TIMESTAMP = "CURRENT_TIMESTAMP";
    constexpr const char* CURRENT_DATE = "CURRENT_DATE";
    constexpr const char* CURRENT_TIME = "CURRENT_TIME";
    constexpr const char* INTERVAL = "INTERVAL";
    constexpr const char* EVENT_TIME = "EVENT_TIME";
    constexpr const char* PROCESSING_TIME = "PROCESSING_TIME";
}

/**
 * @brief QuestDB time-series functions
 */
namespace questdb {
    constexpr const char* SAMPLE_BY = "SAMPLE BY";
    constexpr const char* LATEST_BY = "LATEST BY";
    constexpr const char* FIRST_BY = "FIRST BY";
    constexpr const char* LAST_BY = "LAST BY";
    constexpr const char* ASOF_JOIN = "ASOF JOIN";
    constexpr const char* FILL_NULL = "FILL NULL";
    constexpr const char* TIMESTAMP_FUNCTION = "timestamp";
    constexpr const char* DATE_TRUNC = "date_trunc";
    constexpr const char* EXTRACT_TIME = "extract";
}

/**
 * @brief Standard window functions
 */
namespace window_functions {
    constexpr const char* ROW_NUMBER = "ROW_NUMBER";
    constexpr const char* RANK = "RANK";
    constexpr const char* DENSE_RANK = "DENSE_RANK";
    constexpr const char* PERCENT_RANK = "PERCENT_RANK";
    constexpr const char* CUME_DIST = "CUME_DIST";
    constexpr const char* NTILE = "NTILE";
    constexpr const char* LAG = "LAG";
    constexpr const char* LEAD = "LEAD";
    constexpr const char* FIRST_VALUE = "FIRST_VALUE";
    constexpr const char* LAST_VALUE = "LAST_VALUE";
    constexpr const char* NTH_VALUE = "NTH_VALUE";
}

} // namespace sql_extensions

/**
 * @brief Enhanced Logical Plan with streaming metadata
 */
struct EnhancedLogicalPlan {
    LogicalPlan base_plan;
    
    // Streaming metadata
    bool has_streaming_constructs = false;
    bool has_time_series_functions = false;
    bool has_window_functions = false;
    
    // Window specifications
    std::vector<std::string> window_specs;
    std::vector<std::string> time_columns;
    
    // Time semantics
    bool uses_event_time = false;
    bool uses_processing_time = false;
    
    // QuestDB-specific metadata
    std::vector<std::string> sample_by_clauses;
    std::vector<std::string> latest_by_clauses;
    
    // Performance hints
    std::unordered_map<std::string, std::string> performance_hints;
};

} // namespace sql
} // namespace sabot_sql
