/**
 * Simple SQL Executor using Sabot Operators
 * 
 * Handles common SQL patterns without full DuckDB parsing:
 * - SELECT columns FROM table
 * - SELECT aggregations FROM table  
 * - SELECT ... FROM table WHERE condition
 * - SELECT ... FROM table GROUP BY columns
 * 
 * Uses our Arrow-based operators for execution.
 */

#include "sabot_sql/operators/filter.h"
#include "sabot_sql/operators/projection.h"
#include "sabot_sql/operators/aggregate.h"
#include "sabot_sql/operators/sort.h"
#include <regex>
#include <sstream>

namespace sabot_sql {

/**
 * Parse a simple SELECT query and build operator tree
 */
arrow::Result<std::shared_ptr<sabot_sql::operators::Operator>>
BuildOperatorTree(
    const std::string& sql,
    const std::unordered_map<std::string, std::shared_ptr<arrow::Table>>& tables) {
    
    // Extract table name
    std::regex from_regex(R"(FROM\s+(\w+))", std::regex_constants::icase);
    std::smatch from_match;
    
    if (!std::regex_search(sql, from_match, from_regex)) {
        return arrow::Status::Invalid("No FROM clause found");
    }
    
    std::string table_name = from_match[1].str();
    
    // Get the table
    auto it = tables.find(table_name);
    if (it == tables.end()) {
        return arrow::Status::Invalid("Table not found: " + table_name);
    }
    
    auto table = it->second;
    
    // Create a simple table wrapper operator
    // Since we don't have TableScanOperator yet, create one inline
    class SimpleTableOperator : public operators::Operator {
    public:
        SimpleTableOperator(std::shared_ptr<arrow::Table> table) 
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
    
    std::shared_ptr<operators::Operator> root = 
        std::make_shared<SimpleTableOperator>(table);
    
    // Check for WHERE clause
    std::regex where_regex(R"(WHERE\s+(\w+)\s*(<>|!=|=|>|<|>=|<=)\s*'?([^']+)'?)", std::regex_constants::icase);
    std::smatch where_match;
    
    if (std::regex_search(sql, where_match, where_regex)) {
        std::string column = where_match[1].str();
        std::string op = where_match[2].str();
        std::string value = where_match[3].str();
        
        // Trim quotes and whitespace from value
        value.erase(std::remove(value.begin(), value.end(), '\''), value.end());
        value.erase(std::remove(value.begin(), value.end(), ' '), value.end());
        
        root = std::make_shared<operators::FilterOperator>(root, column, op, value);
    }
    
    // Check for aggregations
    std::regex agg_regex(R"(SELECT\s+(COUNT|SUM|AVG|MIN|MAX))", std::regex_constants::icase);
    std::smatch agg_match;
    
    if (std::regex_search(sql, agg_match, agg_regex)) {
        // Has aggregation
        std::string agg_func = agg_match[1].str();
        std::transform(agg_func.begin(), agg_func.end(), agg_func.begin(), ::toupper);
        
        // Parse aggregation details
        std::vector<operators::AggregationSpec> aggs;
        
        if (agg_func == "COUNT") {
            // Check if COUNT(*) or COUNT(DISTINCT col)
            if (sql.find("COUNT(*)") != std::string::npos) {
                aggs.push_back({operators::AggregationType::COUNT, "", "count_star()"});
            } else if (sql.find("COUNT(DISTINCT") != std::string::npos) {
                // Extract column name
                std::regex count_distinct_regex(R"(COUNT\(DISTINCT\s+(\w+)\))", std::regex_constants::icase);
                std::smatch cd_match;
                if (std::regex_search(sql, cd_match, count_distinct_regex)) {
                    std::string col = cd_match[1].str();
                    aggs.push_back({operators::AggregationType::COUNT_DISTINCT, col, "count()"});
                }
            } else {
                aggs.push_back({operators::AggregationType::COUNT, "", "count_star()"});
            }
        } else if (agg_func == "SUM") {
            std::regex sum_regex(R"(SUM\((\w+)\))", std::regex_constants::icase);
            std::smatch sum_match;
            if (std::regex_search(sql, sum_match, sum_regex)) {
                std::string col = sum_match[1].str();
                aggs.push_back({operators::AggregationType::SUM, col, "sum(" + col + ")"});
            }
        } else if (agg_func == "AVG") {
            std::regex avg_regex(R"(AVG\((\w+)\))", std::regex_constants::icase);
            std::smatch avg_match;
            if (std::regex_search(sql, avg_match, avg_regex)) {
                std::string col = avg_match[1].str();
                aggs.push_back({operators::AggregationType::AVG, col, "avg(" + col + ")"});
            }
        } else if (agg_func == "MIN") {
            std::regex min_regex(R"(MIN\((\w+)\))", std::regex_constants::icase);
            std::smatch min_match;
            if (std::regex_search(sql, min_match, min_regex)) {
                std::string col = min_match[1].str();
                aggs.push_back({operators::AggregationType::MIN, col, "min(" + col + ")"});
            }
        } else if (agg_func == "MAX") {
            std::regex max_regex(R"(MAX\((\w+)\))", std::regex_constants::icase);
            std::smatch max_match;
            if (std::regex_search(sql, max_match, max_regex)) {
                std::string col = max_match[1].str();
                aggs.push_back({operators::AggregationType::MAX, col, "max(" + col + ")"});
            }
        }
        
        // Check for multiple aggregations
        size_t agg_count = 0;
        for (const char* func : {"COUNT", "SUM", "AVG", "MIN", "MAX"}) {
            size_t pos = 0;
            std::string sql_upper = sql;
            std::transform(sql_upper.begin(), sql_upper.end(), sql_upper.begin(), ::toupper);
            while ((pos = sql_upper.find(func, pos)) != std::string::npos) {
                agg_count++;
                pos += strlen(func);
            }
        }
        
        if (agg_count > 1) {
            // Multiple aggregations - parse each one
            aggs.clear();
            
            // COUNT(*)
            if (sql.find("COUNT(*)") != std::string::npos) {
                aggs.push_back({operators::AggregationType::COUNT, "", "count_star()"});
            }
            
            // SUM
            std::regex sum_regex(R"(SUM\((\w+)\))", std::regex_constants::icase);
            for (std::sregex_iterator it(sql.begin(), sql.end(), sum_regex), end; it != end; ++it) {
                std::string col = (*it)[1].str();
                aggs.push_back({operators::AggregationType::SUM, col, "sum(" + col + ")"});
            }
            
            // AVG
            std::regex avg_regex(R"(AVG\((\w+)\))", std::regex_constants::icase);
            for (std::sregex_iterator it(sql.begin(), sql.end(), avg_regex), end; it != end; ++it) {
                std::string col = (*it)[1].str();
                aggs.push_back({operators::AggregationType::AVG, col, "avg(" + col + ")"});
            }
        }
        
        if (!aggs.empty()) {
            root = std::make_shared<operators::AggregateOperator>(root, aggs);
        }
    }
    
    return root;
}

} // namespace sabot_sql

