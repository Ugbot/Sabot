#include "sabot_sql/sql/simple_sabot_sql_bridge.h"
#include "sabot_sql/sql/binder_rewrites.h"
#include "sabot_sql/operators/operator.h"
#include "sabot_sql/operators/filter.h"
#include "sabot_sql/operators/projection.h"
#include "sabot_sql/operators/aggregate.h"
#include <regex>
#include <sstream>
#include <algorithm>

namespace sabot_sql {
namespace sql {

arrow::Result<std::shared_ptr<SabotSQLBridge>> 
SabotSQLBridge::Create() {
    try {
        auto bridge = std::shared_ptr<SabotSQLBridge>(new SabotSQLBridge());
        bridge->InitializePatterns();
        return bridge;
    } catch (const std::exception& e) {
        return arrow::Status::IOError(
            "Failed to create SabotSQL bridge: " + std::string(e.what()));
    }
}

arrow::Status SabotSQLBridge::RegisterTable(
    const std::string& table_name,
    const std::shared_ptr<arrow::Table>& table) {
    
    registered_tables_[table_name] = table;
    return arrow::Status::OK();
}

arrow::Result<LogicalPlan> 
SabotSQLBridge::ParseAndOptimize(const std::string& sql) {
    return ParseSQLWithExtensions(sql);
}

arrow::Result<std::shared_ptr<arrow::Table>> 
SabotSQLBridge::ExecuteSQL(const std::string& sql) {
    return ExecuteSimpleSQL(sql);
}

bool SabotSQLBridge::TableExists(const std::string& table_name) {
    return registered_tables_.find(table_name) != registered_tables_.end();
}

void SabotSQLBridge::InitializePatterns() const {
    // Flink SQL patterns
    flink_patterns_.emplace_back(R"(TUMBLE\s*\([^)]+\))", std::regex_constants::icase);
    flink_patterns_.emplace_back(R"(HOP\s*\([^)]+\))", std::regex_constants::icase);
    flink_patterns_.emplace_back(R"(SESSION\s*\([^)]+\))", std::regex_constants::icase);
    flink_patterns_.emplace_back(R"(CURRENT_TIMESTAMP)", std::regex_constants::icase);
    flink_patterns_.emplace_back(R"(WATERMARK\s+FOR)", std::regex_constants::icase);
    flink_patterns_.emplace_back(R"(OVER\s*\()", std::regex_constants::icase);
    
    // QuestDB SQL patterns
    questdb_patterns_.emplace_back(R"(SAMPLE\s+BY\s+[^\s]+)", std::regex_constants::icase);
    questdb_patterns_.emplace_back(R"(LATEST\s+BY\s+[^\s]+)", std::regex_constants::icase);
    questdb_patterns_.emplace_back(R"(ASOF\s+JOIN)", std::regex_constants::icase);
}

bool SabotSQLBridge::ContainsFlinkConstructs(const std::string& sql) const {
    for (const auto& pattern : flink_patterns_) {
        if (std::regex_search(sql, pattern)) {
            return true;
        }
    }
    return false;
}

bool SabotSQLBridge::ContainsQuestDBConstructs(const std::string& sql) const {
    for (const auto& pattern : questdb_patterns_) {
        if (std::regex_search(sql, pattern)) {
            return true;
        }
    }
    return false;
}

std::vector<std::string> SabotSQLBridge::ExtractWindowSpecifications(const std::string& sql) const {
    std::vector<std::string> windows;
    
    // Extract TUMBLE windows
    std::regex tumble_regex(R"(TUMBLE\s*\([^)]+\))", std::regex_constants::icase);
    std::sregex_iterator tumble_iter(sql.begin(), sql.end(), tumble_regex);
    std::sregex_iterator tumble_end;
    while (tumble_iter != tumble_end) {
        windows.push_back(tumble_iter->str());
        ++tumble_iter;
    }
    
    // Extract HOP windows
    std::regex hop_regex(R"(HOP\s*\([^)]+\))", std::regex_constants::icase);
    std::sregex_iterator hop_iter(sql.begin(), sql.end(), hop_regex);
    std::sregex_iterator hop_end;
    while (hop_iter != hop_end) {
        windows.push_back(hop_iter->str());
        ++hop_iter;
    }
    
    // Extract SESSION windows
    std::regex session_regex(R"(SESSION\s*\([^)]+\))", std::regex_constants::icase);
    std::sregex_iterator session_iter(sql.begin(), sql.end(), session_regex);
    std::sregex_iterator session_end;
    while (session_iter != session_end) {
        windows.push_back(session_iter->str());
        ++session_iter;
    }
    
    return windows;
}

std::vector<std::string> SabotSQLBridge::ExtractSampleByClauses(const std::string& sql) const {
    std::vector<std::string> samples;
    std::regex sample_regex(R"(SAMPLE\s+BY\s+[^\s]+)", std::regex_constants::icase);
    std::sregex_iterator iter(sql.begin(), sql.end(), sample_regex);
    std::sregex_iterator end;
    while (iter != end) {
        samples.push_back(iter->str());
        ++iter;
    }
    return samples;
}

std::vector<std::string> SabotSQLBridge::ExtractLatestByClauses(const std::string& sql) const {
    std::vector<std::string> latest;
    std::regex latest_regex(R"(LATEST\s+BY\s+[^\s]+)", std::regex_constants::icase);
    std::sregex_iterator iter(sql.begin(), sql.end(), latest_regex);
    std::sregex_iterator end;
    while (iter != end) {
        latest.push_back(iter->str());
        ++iter;
    }
    return latest;
}

std::string SabotSQLBridge::PreprocessFlinkSQL(const std::string& sql) const {
    std::string processed = sql;
    
    // Replace CURRENT_TIMESTAMP with NOW()
    std::regex current_timestamp_regex(R"(CURRENT_TIMESTAMP)", std::regex_constants::icase);
    processed = std::regex_replace(processed, current_timestamp_regex, "NOW()");
    
    // Replace CURRENT_DATE with CURRENT_DATE (already standard)
    std::regex current_date_regex(R"(CURRENT_DATE)", std::regex_constants::icase);
    processed = std::regex_replace(processed, current_date_regex, "CURRENT_DATE");
    
    // Replace CURRENT_TIME with CURRENT_TIME (already standard)
    std::regex current_time_regex(R"(CURRENT_TIME)", std::regex_constants::icase);
    processed = std::regex_replace(processed, current_time_regex, "CURRENT_TIME");
    
    return processed;
}

std::string SabotSQLBridge::PreprocessQuestDBSQL(const std::string& sql) const {
    std::string processed = sql;
    
    // Replace SAMPLE BY with GROUP BY DATE_TRUNC
    std::regex sample_by_regex(R"(SAMPLE\s+BY\s+([^\s]+))", std::regex_constants::icase);
    processed = std::regex_replace(processed, sample_by_regex, "GROUP BY DATE_TRUNC('$1', timestamp_col)");
    
    // Replace LATEST BY with ORDER BY DESC LIMIT 1
    std::regex latest_by_regex(R"(LATEST\s+BY\s+([^\s]+))", std::regex_constants::icase);
    processed = std::regex_replace(processed, latest_by_regex, "ORDER BY $1 DESC LIMIT 1");
    
    // Replace ASOF JOIN with LEFT JOIN
    std::regex asof_join_regex(R"(ASOF\s+JOIN)", std::regex_constants::icase);
    processed = std::regex_replace(processed, asof_join_regex, "LEFT JOIN");
    
    return processed;
}

arrow::Result<LogicalPlan> 
SabotSQLBridge::ParseSQLWithExtensions(const std::string& sql) {
    std::string processed_sql = sql;

    // Preprocess extensions (Flink/QuestDB) into base SQL via binder rewrites
    RewriteInfo info;
    processed_sql = ApplyBinderRewrites(sql, info);

    // Lightweight parsing and feature detection (planning-only)
    LogicalPlan plan;
    plan.root_operator = nullptr; // Placeholder root

    auto to_upper = [](std::string s) {
        std::transform(s.begin(), s.end(), s.begin(), [](unsigned char c){ return std::toupper(c); });
        return s;
    };
    const std::string upper = to_upper(processed_sql);

    plan.has_joins = upper.find(" JOIN ") != std::string::npos || upper.find("JOIN ") != std::string::npos;
    plan.has_asof_joins = upper.find("ASOF JOIN") != std::string::npos;
    plan.has_aggregates = upper.find(" GROUP BY ") != std::string::npos ||
                          upper.find(" COUNT(") != std::string::npos ||
                          upper.find(" SUM(") != std::string::npos ||
                          upper.find(" AVG(") != std::string::npos ||
                          upper.find(" MIN(") != std::string::npos ||
                          upper.find(" MAX(") != std::string::npos;
    plan.has_subqueries = upper.find("SELECT ") != std::string::npos && upper.find(" FROM (") != std::string::npos;
    plan.has_ctes = upper.find("WITH ") != std::string::npos;
    plan.has_windows = info.has_windows || info.has_flink_constructs ||
                       upper.find(" OVER (") != std::string::npos ||
                       upper.find(" TUMBLE(") != std::string::npos ||
                       upper.find(" HOP(") != std::string::npos ||
                       upper.find(" SESSION(") != std::string::npos ||
                       upper.find(" SAMPLE BY ") != std::string::npos ||
                       upper.find(" LATEST BY ") != std::string::npos;
    plan.has_asof_joins = plan.has_asof_joins || info.has_asof_join;
    if (!info.window_interval.empty()) {
        plan.window_interval = info.window_interval;
    }
    if (!info.join_key_columns.empty()) {
        plan.join_key_columns = info.join_key_columns;
    }
    if (!info.join_timestamp_column.empty()) {
        plan.join_timestamp_column = info.join_timestamp_column;
    }

    plan.processed_sql = processed_sql;
    return plan;
}

arrow::Result<LogicalPlan> 
SabotSQLBridge::ParseSimpleSQL(const std::string& sql) {
    // Simple SQL parsing for testing
    LogicalPlan plan;
    
    // Basic pattern matching
    std::regex select_regex(R"(SELECT\s+(.+?)\s+FROM\s+(\w+))", std::regex_constants::icase);
    std::regex where_regex(R"(WHERE\s+(.+))", std::regex_constants::icase);
    
    std::smatch select_match;
    std::smatch where_match;
    
    if (std::regex_search(sql, select_match, select_regex)) {
        std::string columns = select_match[1].str();
        std::string table_name = select_match[2].str();
        
        // Check if table exists
        if (!TableExists(table_name)) {
            return arrow::Status::Invalid("Table not found: " + table_name);
        }
        
        // Look for WHERE clause
        if (std::regex_search(sql, where_match, where_regex)) {
            std::string where_clause = where_match[1].str();
            // Simple WHERE clause parsing could be added here
        }
        
        // Set plan features
        plan.has_joins = false;
        plan.has_aggregates = false;
        plan.has_subqueries = false;
        plan.has_ctes = false;
        plan.has_windows = false;
        
        return plan;
    }
    
    return arrow::Status::Invalid("Unsupported SQL query: " + sql);
}

// Simple table scan operator (inline since TableScanOperator has issues)
class SimpleTableScan : public operators::Operator {
public:
    SimpleTableScan(std::shared_ptr<arrow::Table> table) 
        : table_(table), exhausted_(false) {}
    
    arrow::Result<std::shared_ptr<arrow::Schema>> GetOutputSchema() const override {
        return table_->schema();
    }
    
    arrow::Result<std::shared_ptr<arrow::RecordBatch>> GetNextBatch() override {
        if (exhausted_) return nullptr;
        exhausted_ = true;
        ARROW_ASSIGN_OR_RAISE(auto batch, table_->CombineChunksToBatch());
        return batch;
    }
    
    bool HasNextBatch() const override { return !exhausted_; }
    std::string ToString() const override { return "TableScan"; }
    size_t EstimateCardinality() const override { return table_->num_rows(); }
    
    arrow::Result<std::shared_ptr<arrow::Table>> GetAllResults() override {
        return table_;
    }
    
private:
    std::shared_ptr<arrow::Table> table_;
    bool exhausted_;
};

arrow::Result<std::shared_ptr<arrow::Table>> 
SabotSQLBridge::ExecuteSimpleSQL(const std::string& sql) {
    // Parse SQL and build operator tree using our operators
    
    // Extract table name
    std::regex from_regex(R"(FROM\s+(\w+))", std::regex_constants::icase);
    std::smatch from_match;
    
    if (!std::regex_search(sql, from_match, from_regex)) {
        return arrow::Status::Invalid("No FROM clause found");
    }
    
    std::string table_name = from_match[1].str();
    auto it = registered_tables_.find(table_name);
    if (it == registered_tables_.end()) {
        return arrow::Status::Invalid("Table not found: " + table_name);
    }
    
    // Start with table scan
    std::shared_ptr<operators::Operator> root = 
        std::make_shared<SimpleTableScan>(it->second);
    
    // Check for WHERE clause
    std::regex where_regex(R"(WHERE\s+(\w+)\s*(<>|!=|=|>|<|>=|<=)\s*(.+?)(?:\s+GROUP|\s+ORDER|;|$))", 
                          std::regex_constants::icase);
    std::smatch where_match;
    
    if (std::regex_search(sql, where_match, where_regex)) {
        std::string column = where_match[1].str();
        std::string op = where_match[2].str();
        std::string value = where_match[3].str();
        
        // Trim whitespace and quotes
        value.erase(0, value.find_first_not_of(" \t'\""));
        value.erase(value.find_last_not_of(" \t'\"") + 1);
        
        root = std::make_shared<operators::FilterOperator>(root, column, op, value);
    }
    
    // Check for aggregations
    std::vector<operators::AggregationSpec> aggs;
    
    // COUNT(*)
    if (std::regex_search(sql, std::regex(R"(COUNT\s*\(\s*\*\s*\))", std::regex_constants::icase))) {
        aggs.push_back({operators::AggregationType::COUNT, "", "count_star()"});
    }
    
    // COUNT(DISTINCT col)
    std::regex count_distinct_regex(R"(COUNT\s*\(\s*DISTINCT\s+(\w+)\s*\))", std::regex_constants::icase);
    std::smatch cd_match;
    if (std::regex_search(sql, cd_match, count_distinct_regex)) {
        aggs.push_back({operators::AggregationType::COUNT_DISTINCT, cd_match[1].str(), "count()"});
    }
    
    // SUM(col)
    std::regex sum_regex(R"(SUM\s*\(\s*(\w+)\s*\))", std::regex_constants::icase);
    std::smatch sum_match;
    if (std::regex_search(sql, sum_match, sum_regex)) {
        aggs.push_back({operators::AggregationType::SUM, sum_match[1].str(), "sum(" + sum_match[1].str() + ")"});
    }
    
    // AVG(col)
    std::regex avg_regex(R"(AVG\s*\(\s*(\w+)\s*\))", std::regex_constants::icase);
    std::smatch avg_match;
    if (std::regex_search(sql, avg_match, avg_regex)) {
        aggs.push_back({operators::AggregationType::AVG, avg_match[1].str(), "avg(" + avg_match[1].str() + ")"});
    }
    
    // MIN(col)
    std::regex min_regex(R"(MIN\s*\(\s*(\w+)\s*\))", std::regex_constants::icase);
    std::smatch min_match;
    if (std::regex_search(sql, min_match, min_regex)) {
        aggs.push_back({operators::AggregationType::MIN, min_match[1].str(), "min(" + min_match[1].str() + ")"});
    }
    
    // MAX(col)
    std::regex max_regex(R"(MAX\s*\(\s*(\w+)\s*\))", std::regex_constants::icase);
    std::smatch max_match;
    if (std::regex_search(sql, max_match, max_regex)) {
        aggs.push_back({operators::AggregationType::MAX, max_match[1].str(), "max(" + max_match[1].str() + ")"});
    }
    
    // If we have aggregations, create aggregate operator
    if (!aggs.empty()) {
        root = std::make_shared<operators::AggregateOperator>(root, aggs);
    }
    
    // Execute the operator tree
    return root->GetAllResults();
}

} // namespace sql
} // namespace sabot_sql
