#include "sabot_sql/sql/flink_sql_extension.h"
#include <regex>
#include <algorithm>

namespace sabot_sql {
namespace sql {

// FlinkSQL Extension Implementation

arrow::Result<std::shared_ptr<FlinkSQLExtension>> 
FlinkSQLExtension::Create() {
    try {
        auto extension = std::shared_ptr<FlinkSQLExtension>(new FlinkSQLExtension());
        extension->InitializePatterns();
        return extension;
    } catch (const std::exception& e) {
        return arrow::Status::IOError(
            "Failed to create Flink SQL extension: " + std::string(e.what()));
    }
}

void FlinkSQLExtension::InitializePatterns() {
    // Flink window functions
    window_functions_ = {
        "TUMBLE", "HOP", "SESSION", "OVER"
    };
    
    // Flink streaming functions
    streaming_functions_ = {
        "CURRENT_TIMESTAMP", "CURRENT_DATE", "CURRENT_TIME",
        "WATERMARK", "LAG", "LEAD", "FIRST_VALUE", "LAST_VALUE"
    };
    
    // Flink SQL patterns and their SabotSQL equivalents
    flink_patterns_ = {
        // Window functions
        {R"(TUMBLE\s*\(([^,]+),\s*INTERVAL\s*'([^']+)'\s*\))", "DATE_TRUNC('$2', $1)"},
        {R"(HOP\s*\(([^,]+),\s*INTERVAL\s*'([^']+)',\s*INTERVAL\s*'([^']+)'\s*\))", "DATE_TRUNC('$2', $1)"},
        {R"(SESSION\s*\(([^,]+),\s*INTERVAL\s*'([^']+)'\s*\))", "DATE_TRUNC('$2', $1)"},
        
        // Time functions
        {"CURRENT_TIMESTAMP", "NOW()"},
        {"CURRENT_DATE", "CURRENT_DATE"},
        {"CURRENT_TIME", "CURRENT_TIME"},
        
        // Watermark (comment out for now)
        {R"(WATERMARK\s+FOR\s+([^\\s]+)\s+AS\s+([^\\s]+))", "-- WATERMARK: $1 AS $2"},
        
        // Window functions with OVER
        {R"(OVER\s*\(([^)]+)\))", "OVER ($1)"}
    };
}

arrow::Result<std::string> 
FlinkSQLExtension::PreprocessFlinkSQL(const std::string& flink_sql) {
    std::string processed_sql = flink_sql;
    
    // Apply Flink patterns
    for (const auto& [pattern, replacement] : flink_patterns_) {
        std::regex regex_pattern(pattern, std::regex_constants::icase);
        processed_sql = std::regex_replace(processed_sql, regex_pattern, replacement);
    }
    
    return processed_sql;
}

bool FlinkSQLExtension::ContainsFlinkConstructs(const std::string& sql) {
    std::string upper_sql = sql;
    std::transform(upper_sql.begin(), upper_sql.end(), upper_sql.begin(), ::toupper);
    
    // Check for window functions
    for (const auto& func : window_functions_) {
        if (upper_sql.find(func) != std::string::npos) {
            return true;
        }
    }
    
    // Check for streaming functions
    for (const auto& func : streaming_functions_) {
        if (upper_sql.find(func) != std::string::npos) {
            return true;
        }
    }
    
    return false;
}

arrow::Result<std::vector<std::string>> 
FlinkSQLExtension::ExtractWindowSpecifications(const std::string& sql) {
    std::vector<std::string> windows;
    
    // Extract TUMBLE windows
    std::regex tumble_regex(R"(TUMBLE\s*\([^)]+\))", std::regex_constants::icase);
    std::sregex_iterator tumble_begin(sql.begin(), sql.end(), tumble_regex);
    std::sregex_iterator tumble_end;
    
    for (auto it = tumble_begin; it != tumble_end; ++it) {
        windows.push_back(it->str());
    }
    
    // Extract HOP windows
    std::regex hop_regex(R"(HOP\s*\([^)]+\))", std::regex_constants::icase);
    std::sregex_iterator hop_begin(sql.begin(), sql.end(), hop_regex);
    std::sregex_iterator hop_end;
    
    for (auto it = hop_begin; it != hop_end; ++it) {
        windows.push_back(it->str());
    }
    
    // Extract SESSION windows
    std::regex session_regex(R"(SESSION\s*\([^)]+\))", std::regex_constants::icase);
    std::sregex_iterator session_begin(sql.begin(), sql.end(), session_regex);
    std::sregex_iterator session_end;
    
    for (auto it = session_begin; it != session_end; ++it) {
        windows.push_back(it->str());
    }
    
    return windows;
}

arrow::Result<std::vector<std::string>> 
FlinkSQLExtension::ExtractWatermarkDefinitions(const std::string& sql) {
    std::vector<std::string> watermarks;
    
    // Extract WATERMARK definitions
    std::regex watermark_regex(R"(WATERMARK\s+FOR\s+[^\\s]+\s+AS\s+[^\\s]+)", std::regex_constants::icase);
    std::sregex_iterator watermark_begin(sql.begin(), sql.end(), watermark_regex);
    std::sregex_iterator watermark_end;
    
    for (auto it = watermark_begin; it != watermark_end; ++it) {
        watermarks.push_back(it->str());
    }
    
    return watermarks;
}

bool FlinkSQLExtension::MatchesPattern(const std::string& sql, const std::string& pattern) {
    std::regex regex_pattern(pattern, std::regex_constants::icase);
    return std::regex_search(sql, regex_pattern);
}

std::string FlinkSQLExtension::ReplacePattern(const std::string& sql, const std::string& pattern, const std::string& replacement) {
    std::regex regex_pattern(pattern, std::regex_constants::icase);
    return std::regex_replace(sql, regex_pattern, replacement);
}

// QuestDBSQL Extension Implementation

arrow::Result<std::shared_ptr<QuestDBSQLExtension>> 
QuestDBSQLExtension::Create() {
    try {
        auto extension = std::shared_ptr<QuestDBSQLExtension>(new QuestDBSQLExtension());
        extension->InitializePatterns();
        return extension;
    } catch (const std::exception& e) {
        return arrow::Status::IOError(
            "Failed to create QuestDB SQL extension: " + std::string(e.what()));
    }
}

void QuestDBSQLExtension::InitializePatterns() {
    // QuestDB time-series functions
    timeseries_functions_ = {
        "SAMPLE BY", "LATEST BY", "ASOF JOIN", "TIMESTAMP"
    };
    
    // QuestDB SQL patterns and their SabotSQL equivalents
    questdb_patterns_ = {
        // SAMPLE BY -> GROUP BY with time truncation
        {R"(SAMPLE\s+BY\s+([^\\s]+))", "GROUP BY DATE_TRUNC('$1', timestamp_col)"},
        
        // LATEST BY -> ORDER BY DESC LIMIT 1
        {R"(LATEST\s+BY\s+([^\\s]+))", "ORDER BY $1 DESC LIMIT 1"},
        
        // ASOF JOIN -> LEFT JOIN (simplified)
        {"ASOF JOIN", "LEFT JOIN"},
        
        // QuestDB timestamp functions
        {"TIMESTAMP", "TIMESTAMP"}
    };
}

arrow::Result<std::string> 
QuestDBSQLExtension::PreprocessQuestDBSQL(const std::string& questdb_sql) {
    std::string processed_sql = questdb_sql;
    
    // Apply QuestDB patterns
    for (const auto& [pattern, replacement] : questdb_patterns_) {
        std::regex regex_pattern(pattern, std::regex_constants::icase);
        processed_sql = std::regex_replace(processed_sql, regex_pattern, replacement);
    }
    
    return processed_sql;
}

bool QuestDBSQLExtension::ContainsQuestDBConstructs(const std::string& sql) {
    std::string upper_sql = sql;
    std::transform(upper_sql.begin(), upper_sql.end(), upper_sql.begin(), ::toupper);
    
    // Check for QuestDB functions
    for (const auto& func : timeseries_functions_) {
        if (upper_sql.find(func) != std::string::npos) {
            return true;
        }
    }
    
    return false;
}

arrow::Result<std::vector<std::string>> 
QuestDBSQLExtension::ExtractSampleByClauses(const std::string& sql) {
    std::vector<std::string> sample_clauses;
    
    // Extract SAMPLE BY clauses
    std::regex sample_regex(R"(SAMPLE\s+BY\s+[^\\s]+)", std::regex_constants::icase);
    std::sregex_iterator sample_begin(sql.begin(), sql.end(), sample_regex);
    std::sregex_iterator sample_end;
    
    for (auto it = sample_begin; it != sample_end; ++it) {
        sample_clauses.push_back(it->str());
    }
    
    return sample_clauses;
}

arrow::Result<std::vector<std::string>> 
QuestDBSQLExtension::ExtractLatestByClauses(const std::string& sql) {
    std::vector<std::string> latest_clauses;
    
    // Extract LATEST BY clauses
    std::regex latest_regex(R"(LATEST\s+BY\s+[^\\s]+)", std::regex_constants::icase);
    std::sregex_iterator latest_begin(sql.begin(), sql.end(), latest_regex);
    std::sregex_iterator latest_end;
    
    for (auto it = latest_begin; it != latest_end; ++it) {
        latest_clauses.push_back(it->str());
    }
    
    return latest_clauses;
}

bool QuestDBSQLExtension::MatchesPattern(const std::string& sql, const std::string& pattern) {
    std::regex regex_pattern(pattern, std::regex_constants::icase);
    return std::regex_search(sql, regex_pattern);
}

std::string QuestDBSQLExtension::ReplacePattern(const std::string& sql, const std::string& pattern, const std::string& replacement) {
    std::regex regex_pattern(pattern, std::regex_constants::icase);
    return std::regex_replace(sql, regex_pattern, replacement);
}

} // namespace sql
} // namespace sabot_sql
