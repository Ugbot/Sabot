#include "sabot_sql/sql/enhanced_parser.h"
#include <regex>
#include <sstream>
#include <algorithm>

namespace sabot_sql {
namespace sql {

EnhancedSQLParser::EnhancedSQLParser(
    const EnhancedParserConfig& config,
    std::shared_ptr<duckdb::DuckDB> database,
    std::unique_ptr<duckdb::Connection> connection)
    : config_(config)
    , database_(std::move(database))
    , connection_(std::move(connection)) {
    
    // Initialize pattern mappings
    InitializePatterns();
}

EnhancedSQLParser::~EnhancedSQLParser() = default;

arrow::Result<std::shared_ptr<EnhancedSQLParser>> 
EnhancedSQLParser::Create(const EnhancedParserConfig& config) {
    try {
        auto database = std::make_unique<duckdb::DuckDB>(":memory:");
        auto connection = std::make_unique<duckdb::Connection>(*database);
        
        return std::shared_ptr<EnhancedSQLParser>(
            new EnhancedSQLParser(config, std::move(database), std::move(connection)));
    } catch (const std::exception& e) {
        return arrow::Status::IOError(
            "Failed to create enhanced SQL parser: " + std::string(e.what()));
    }
}

void EnhancedSQLParser::InitializePatterns() {
    if (config_.enable_flink_syntax) {
        // Flink streaming patterns
        streaming_patterns_["TUMBLE\\s*\\(([^,]+),\\s*INTERVAL\\s*'([^']+)'\\s*\\)"] = 
            "TUMBLE_WINDOW($1, $2)";
        streaming_patterns_["HOP\\s*\\(([^,]+),\\s*INTERVAL\\s*'([^']+)',\\s*INTERVAL\\s*'([^']+)'\\s*\\)"] = 
            "HOP_WINDOW($1, $2, $3)";
        streaming_patterns_["SESSION\\s*\\(([^,]+),\\s*INTERVAL\\s*'([^']+)'\\s*\\)"] = 
            "SESSION_WINDOW($1, $2)";
        streaming_patterns_["CURRENT_TIMESTAMP"] = "NOW()";
        streaming_patterns_["CURRENT_DATE"] = "CURRENT_DATE";
        streaming_patterns_["CURRENT_TIME"] = "CURRENT_TIME";
    }
    
    if (config_.enable_questdb_syntax) {
        // QuestDB time-series patterns
        timeseries_patterns_["SAMPLE\\s+BY\\s+([^\\s]+)"] = "SAMPLE_BY($1)";
        timeseries_patterns_["LATEST\\s+BY\\s+([^\\s]+)"] = "LATEST_BY($1)";
        timeseries_patterns_["FIRST\\s+BY\\s+([^\\s]+)"] = "FIRST_BY($1)";
        timeseries_patterns_["LAST\\s+BY\\s+([^\\s]+)"] = "LAST_BY($1)";
        timeseries_patterns_["ASOF\\s+JOIN"] = "ASOF_JOIN";
        timeseries_patterns_["FILL\\s+NULL"] = "FILL_NULL";
    }
    
    if (config_.enable_window_functions) {
        // Window function patterns
        window_patterns_["ROW_NUMBER\\s*\\(\\)"] = "ROW_NUMBER()";
        window_patterns_["RANK\\s*\\(\\)"] = "RANK()";
        window_patterns_["DENSE_RANK\\s*\\(\\)"] = "DENSE_RANK()";
        window_patterns_["LAG\\s*\\(([^,]+),\\s*([^)]+)\\)"] = "LAG($1, $2)";
        window_patterns_["LEAD\\s*\\(([^,]+),\\s*([^)]+)\\)"] = "LEAD($1, $2)";
    }
}

arrow::Result<LogicalPlan> 
EnhancedSQLParser::ParseEnhancedSQL(const std::string& sql) {
    try {
        // Preprocess SQL to handle enhanced constructs
        ARROW_ASSIGN_OR_RAISE(auto preprocessed_sql, PreprocessSQL(sql));
        
        // Parse with DuckDB
        duckdb::Parser parser;
        parser.ParseQuery(preprocessed_sql);
        
        if (parser.statements.empty()) {
            return arrow::Status::Invalid("Empty query after preprocessing");
        }
        
        // Bind and plan
        duckdb::Binder binder(duckdb::Binder::CreateBinder(*connection_->context));
        binder.BindStatement(*parser.statements[0]);
        
        duckdb::Planner planner(*connection_->context);
        planner.CreatePlan(std::move(parser.statements[0]));
        
        // Extract logical plan
        LogicalPlan plan = ExtractLogicalPlan(std::move(planner.plan));
        
        // Analyze plan features
        AnalyzePlanFeatures(plan);
        
        return plan;
        
    } catch (const std::exception& e) {
        return arrow::Status::Invalid(
            "Failed to parse enhanced SQL: " + std::string(e.what()));
    }
}

arrow::Result<std::string> 
EnhancedSQLParser::PreprocessSQL(const std::string& sql) const {
    std::string processed_sql = sql;
    
    // Handle streaming constructs
    if (config_.enable_streaming_constructs) {
        ARROW_ASSIGN_OR_RAISE(processed_sql, HandleTumbleWindow(processed_sql));
        ARROW_ASSIGN_OR_RAISE(processed_sql, HandleHopWindow(processed_sql));
        ARROW_ASSIGN_OR_RAISE(processed_sql, HandleSessionWindow(processed_sql));
        ARROW_ASSIGN_OR_RAISE(processed_sql, HandleCurrentTimestamp(processed_sql));
        ARROW_ASSIGN_OR_RAISE(processed_sql, HandleIntervalArithmetic(processed_sql));
    }
    
    // Handle time-series functions
    if (config_.enable_time_series_functions) {
        ARROW_ASSIGN_OR_RAISE(processed_sql, HandleSampleBy(processed_sql));
        ARROW_ASSIGN_OR_RAISE(processed_sql, HandleLatestBy(processed_sql));
        ARROW_ASSIGN_OR_RAISE(processed_sql, HandleAsofJoin(processed_sql));
        ARROW_ASSIGN_OR_RAISE(processed_sql, HandleFillNull(processed_sql));
    }
    
    // Handle window functions
    if (config_.enable_window_functions) {
        ARROW_ASSIGN_OR_RAISE(processed_sql, HandleRowNumber(processed_sql));
        ARROW_ASSIGN_OR_RAISE(processed_sql, HandleRank(processed_sql));
        ARROW_ASSIGN_OR_RAISE(processed_sql, HandleLagLead(processed_sql));
    }
    
    return processed_sql;
}

arrow::Result<std::string> 
EnhancedSQLParser::HandleTumbleWindow(const std::string& sql) const {
    std::regex tumble_regex(R"(TUMBLE\s*\(([^,]+),\s*INTERVAL\s*'([^']+)'\s*\))", 
                           std::regex_constants::icase);
    
    std::string result = std::regex_replace(sql, tumble_regex, 
        [](const std::smatch& match) -> std::string {
            std::string time_col = match[1].str();
            std::string interval = match[2].str();
            
            // Convert to DuckDB-compatible syntax
            return "DATE_TRUNC('" + interval + "', " + time_col + ")";
        });
    
    return result;
}

arrow::Result<std::string> 
EnhancedSQLParser::HandleHopWindow(const std::string& sql) const {
    std::regex hop_regex(R"(HOP\s*\(([^,]+),\s*INTERVAL\s*'([^']+)',\s*INTERVAL\s*'([^']+)'\s*\))", 
                        std::regex_constants::icase);
    
    std::string result = std::regex_replace(sql, hop_regex, 
        [](const std::smatch& match) -> std::string {
            std::string time_col = match[1].str();
            std::string slide = match[2].str();
            std::string size = match[3].str();
            
            // Convert to DuckDB-compatible syntax
            return "DATE_TRUNC('" + slide + "', " + time_col + ")";
        });
    
    return result;
}

arrow::Result<std::string> 
EnhancedSQLParser::HandleSessionWindow(const std::string& sql) const {
    std::regex session_regex(R"(SESSION\s*\(([^,]+),\s*INTERVAL\s*'([^']+)'\s*\))", 
                             std::regex_constants::icase);
    
    std::string result = std::regex_replace(sql, session_regex, 
        [](const std::smatch& match) -> std::string {
            std::string time_col = match[1].str();
            std::string timeout = match[2].str();
            
            // Convert to DuckDB-compatible syntax
            return "DATE_TRUNC('" + timeout + "', " + time_col + ")";
        });
    
    return result;
}

arrow::Result<std::string> 
EnhancedSQLParser::HandleCurrentTimestamp(const std::string& sql) const {
    std::string result = sql;
    
    // Replace CURRENT_TIMESTAMP with NOW()
    std::regex current_timestamp_regex(R"(\bCURRENT_TIMESTAMP\b)", 
                                     std::regex_constants::icase);
    result = std::regex_replace(result, current_timestamp_regex, "NOW()");
    
    // Replace CURRENT_DATE with CURRENT_DATE (DuckDB supports this)
    std::regex current_date_regex(R"(\bCURRENT_DATE\b)", 
                                 std::regex_constants::icase);
    result = std::regex_replace(result, current_date_regex, "CURRENT_DATE");
    
    // Replace CURRENT_TIME with CURRENT_TIME (DuckDB supports this)
    std::regex current_time_regex(R"(\bCURRENT_TIME\b)", 
                                 std::regex_constants::icase);
    result = std::regex_replace(result, current_time_regex, "CURRENT_TIME");
    
    return result;
}

arrow::Result<std::string> 
EnhancedSQLParser::HandleIntervalArithmetic(const std::string& sql) const {
    std::string result = sql;
    
    // Handle INTERVAL arithmetic
    std::regex interval_regex(R"(INTERVAL\s*'([^']+)'\s*([+\-])\s*INTERVAL\s*'([^']+)')", 
                             std::regex_constants::icase);
    
    result = std::regex_replace(result, interval_regex, 
        [](const std::smatch& match) -> std::string {
            std::string interval1 = match[1].str();
            std::string op = match[2].str();
            std::string interval2 = match[3].str();
            
            // Convert to DuckDB-compatible syntax
            return "INTERVAL '" + interval1 + "' " + op + " INTERVAL '" + interval2 + "'";
        });
    
    return result;
}

arrow::Result<std::string> 
EnhancedSQLParser::HandleSampleBy(const std::string& sql) const {
    std::regex sample_by_regex(R"(SAMPLE\s+BY\s+([^\s]+))", 
                              std::regex_constants::icase);
    
    std::string result = std::regex_replace(sql, sample_by_regex, 
        [](const std::smatch& match) -> std::string {
            std::string interval = match[1].str();
            
            // Convert to DuckDB-compatible syntax
            return "GROUP BY DATE_TRUNC('" + interval + "', timestamp)";
        });
    
    return result;
}

arrow::Result<std::string> 
EnhancedSQLParser::HandleLatestBy(const std::string& sql) const {
    std::regex latest_by_regex(R"(LATEST\s+BY\s+([^\s]+))", 
                              std::regex_constants::icase);
    
    std::string result = std::regex_replace(sql, latest_by_regex, 
        [](const std::smatch& match) -> std::string {
            std::string column = match[1].str();
            
            // Convert to DuckDB-compatible syntax
            return "ORDER BY " + column + " DESC LIMIT 1";
        });
    
    return result;
}

arrow::Result<std::string> 
EnhancedSQLParser::HandleAsofJoin(const std::string& sql) const {
    std::regex asof_join_regex(R"(\bASOF\s+JOIN\b)", 
                              std::regex_constants::icase);
    
    std::string result = std::regex_replace(sql, asof_join_regex, "JOIN");
    
    return result;
}

arrow::Result<std::string> 
EnhancedSQLParser::HandleFillNull(const std::string& sql) const {
    std::regex fill_null_regex(R"(FILL\s+NULL)", 
                              std::regex_constants::icase);
    
    std::string result = std::regex_replace(sql, fill_null_regex, "COALESCE");
    
    return result;
}

arrow::Result<std::string> 
EnhancedSQLParser::HandleRowNumber(const std::string& sql) const {
    std::regex row_number_regex(R"(ROW_NUMBER\s*\(\s*\)\s+OVER\s*\(([^)]+)\))", 
                               std::regex_constants::icase);
    
    std::string result = std::regex_replace(sql, row_number_regex, 
        [](const std::smatch& match) -> std::string {
            std::string over_clause = match[1].str();
            
            // Convert to DuckDB-compatible syntax
            return "ROW_NUMBER() OVER (" + over_clause + ")";
        });
    
    return result;
}

arrow::Result<std::string> 
EnhancedSQLParser::HandleRank(const std::string& sql) const {
    std::string result = sql;
    
    // Handle RANK()
    std::regex rank_regex(R"(RANK\s*\(\s*\)\s+OVER\s*\(([^)]+)\))", 
                         std::regex_constants::icase);
    result = std::regex_replace(result, rank_regex, 
        [](const std::smatch& match) -> std::string {
            return "RANK() OVER (" + match[1].str() + ")";
        });
    
    // Handle DENSE_RANK()
    std::regex dense_rank_regex(R"(DENSE_RANK\s*\(\s*\)\s+OVER\s*\(([^)]+)\))", 
                               std::regex_constants::icase);
    result = std::regex_replace(result, dense_rank_regex, 
        [](const std::smatch& match) -> std::string {
            return "DENSE_RANK() OVER (" + match[1].str() + ")";
        });
    
    return result;
}

arrow::Result<std::string> 
EnhancedSQLParser::HandleLagLead(const std::string& sql) const {
    std::string result = sql;
    
    // Handle LAG()
    std::regex lag_regex(R"(LAG\s*\(\s*([^,]+),\s*([^)]+)\s*\)\s+OVER\s*\(([^)]+)\))", 
                        std::regex_constants::icase);
    result = std::regex_replace(result, lag_regex, 
        [](const std::smatch& match) -> std::string {
            return "LAG(" + match[1].str() + ", " + match[2].str() + ") OVER (" + match[3].str() + ")";
        });
    
    // Handle LEAD()
    std::regex lead_regex(R"(LEAD\s*\(\s*([^,]+),\s*([^)]+)\s*\)\s+OVER\s*\(([^)]+)\))", 
                         std::regex_constants::icase);
    result = std::regex_replace(result, lead_regex, 
        [](const std::smatch& match) -> std::string {
            return "LEAD(" + match[1].str() + ", " + match[2].str() + ") OVER (" + match[3].str() + ")";
        });
    
    return result;
}

bool EnhancedSQLParser::HasStreamingConstructs(const std::string& sql) const {
    std::vector<std::string> patterns = {
        "TUMBLE\\s*\\(",
        "HOP\\s*\\(",
        "SESSION\\s*\\(",
        "CURRENT_TIMESTAMP",
        "EVENT_TIME",
        "PROCESSING_TIME"
    };
    
    for (const auto& pattern : patterns) {
        if (ContainsPattern(sql, pattern)) {
            return true;
        }
    }
    
    return false;
}

bool EnhancedSQLParser::HasTimeSeriesFunctions(const std::string& sql) const {
    std::vector<std::string> patterns = {
        "SAMPLE\\s+BY",
        "LATEST\\s+BY",
        "FIRST\\s+BY",
        "LAST\\s+BY",
        "ASOF\\s+JOIN",
        "FILL\\s+NULL"
    };
    
    for (const auto& pattern : patterns) {
        if (ContainsPattern(sql, pattern)) {
            return true;
        }
    }
    
    return false;
}

bool EnhancedSQLParser::HasWindowFunctions(const std::string& sql) const {
    std::vector<std::string> patterns = {
        "ROW_NUMBER\\s*\\(\\)",
        "RANK\\s*\\(\\)",
        "DENSE_RANK\\s*\\(\\)",
        "LAG\\s*\\(",
        "LEAD\\s*\\(",
        "OVER\\s*\\("
    };
    
    for (const auto& pattern : patterns) {
        if (ContainsPattern(sql, pattern)) {
            return true;
        }
    }
    
    return false;
}

std::vector<StreamingConstruct> 
EnhancedSQLParser::GetSupportedStreamingConstructs() const {
    std::vector<StreamingConstruct> constructs;
    
    if (config_.enable_streaming_constructs) {
        constructs.push_back(StreamingConstruct::TUMBLE_WINDOW);
        constructs.push_back(StreamingConstruct::HOP_WINDOW);
        constructs.push_back(StreamingConstruct::SESSION_WINDOW);
        constructs.push_back(StreamingConstruct::CURRENT_TIMESTAMP);
        constructs.push_back(StreamingConstruct::CURRENT_DATE);
        constructs.push_back(StreamingConstruct::CURRENT_TIME);
        constructs.push_back(StreamingConstruct::INTERVAL_ARITHMETIC);
        constructs.push_back(StreamingConstruct::EVENT_TIME);
        constructs.push_back(StreamingConstruct::PROCESSING_TIME);
    }
    
    return constructs;
}

std::vector<TimeSeriesFunction> 
EnhancedSQLParser::GetSupportedTimeSeriesFunctions() const {
    std::vector<TimeSeriesFunction> functions;
    
    if (config_.enable_time_series_functions) {
        functions.push_back(TimeSeriesFunction::SAMPLE_BY);
        functions.push_back(TimeSeriesFunction::LATEST_BY);
        functions.push_back(TimeSeriesFunction::FIRST_BY);
        functions.push_back(TimeSeriesFunction::LAST_BY);
        functions.push_back(TimeSeriesFunction::ASOF_JOIN);
        functions.push_back(TimeSeriesFunction::FILL_NULL);
        functions.push_back(TimeSeriesFunction::TIMESTAMP_FUNCTION);
        functions.push_back(TimeSeriesFunction::DATE_TRUNC);
        functions.push_back(TimeSeriesFunction::EXTRACT_TIME);
    }
    
    return functions;
}

std::vector<WindowFunctionType> 
EnhancedSQLParser::GetSupportedWindowFunctions() const {
    std::vector<WindowFunctionType> functions;
    
    if (config_.enable_window_functions) {
        functions.push_back(WindowFunctionType::ROW_NUMBER);
        functions.push_back(WindowFunctionType::RANK);
        functions.push_back(WindowFunctionType::DENSE_RANK);
        functions.push_back(WindowFunctionType::PERCENT_RANK);
        functions.push_back(WindowFunctionType::CUME_DIST);
        functions.push_back(WindowFunctionType::NTILE);
        functions.push_back(WindowFunctionType::LAG);
        functions.push_back(WindowFunctionType::LEAD);
        functions.push_back(WindowFunctionType::FIRST_VALUE);
        functions.push_back(WindowFunctionType::LAST_VALUE);
        functions.push_back(WindowFunctionType::NTH_VALUE);
    }
    
    return functions;
}

arrow::Status EnhancedSQLParser::ValidateSQL(const std::string& sql) const {
    try {
        // Try to parse the SQL
        ARROW_ASSIGN_OR_RAISE(auto plan, ParseEnhancedSQL(sql));
        
        // Additional validation
        if (config_.strict_mode) {
            // Check for unsupported constructs
            if (HasStreamingConstructs(sql) && !config_.enable_streaming_constructs) {
                return arrow::Status::Invalid("Streaming constructs not enabled");
            }
            
            if (HasTimeSeriesFunctions(sql) && !config_.enable_time_series_functions) {
                return arrow::Status::Invalid("Time-series functions not enabled");
            }
            
            if (HasWindowFunctions(sql) && !config_.enable_window_functions) {
                return arrow::Status::Invalid("Window functions not enabled");
            }
        }
        
        return arrow::Status::OK();
        
    } catch (const std::exception& e) {
        return arrow::Status::Invalid(
            "SQL validation failed: " + std::string(e.what()));
    }
}

std::string EnhancedSQLParser::GetSyntaxHelp() const {
    std::ostringstream oss;
    
    oss << "SabotSQL Enhanced Parser Syntax Help\n";
    oss << "====================================\n\n";
    
    if (config_.enable_streaming_constructs) {
        oss << "Streaming Constructs (Flink SQL):\n";
        oss << "  TUMBLE(time_col, INTERVAL '1' MINUTE)\n";
        oss << "  HOP(time_col, INTERVAL '30' SECOND, INTERVAL '1' MINUTE)\n";
        oss << "  SESSION(time_col, INTERVAL '5' MINUTE)\n";
        oss << "  CURRENT_TIMESTAMP, CURRENT_DATE, CURRENT_TIME\n";
        oss << "  INTERVAL '1' HOUR + INTERVAL '30' MINUTE\n\n";
    }
    
    if (config_.enable_time_series_functions) {
        oss << "Time-Series Functions (QuestDB):\n";
        oss << "  SAMPLE BY 1h\n";
        oss << "  LATEST BY symbol\n";
        oss << "  FIRST BY symbol\n";
        oss << "  LAST BY symbol\n";
        oss << "  ASOF JOIN\n";
        oss << "  FILL NULL\n\n";
    }
    
    if (config_.enable_window_functions) {
        oss << "Window Functions:\n";
        oss << "  ROW_NUMBER() OVER (PARTITION BY ... ORDER BY ...)\n";
        oss << "  RANK() OVER (PARTITION BY ... ORDER BY ...)\n";
        oss << "  DENSE_RANK() OVER (PARTITION BY ... ORDER BY ...)\n";
        oss << "  LAG(column, 1) OVER (PARTITION BY ... ORDER BY ...)\n";
        oss << "  LEAD(column, 1) OVER (PARTITION BY ... ORDER BY ...)\n\n";
    }
    
    return oss.str();
}

bool EnhancedSQLParser::ContainsPattern(const std::string& sql, const std::string& pattern) const {
    std::regex regex_pattern(pattern, std::regex_constants::icase);
    return std::regex_search(sql, regex_pattern);
}

std::string EnhancedSQLParser::ReplacePattern(const std::string& sql, const std::string& pattern, 
                                              const std::string& replacement) const {
    std::regex regex_pattern(pattern, std::regex_constants::icase);
    return std::regex_replace(sql, regex_pattern, replacement);
}

} // namespace sql
} // namespace sabot_sql
