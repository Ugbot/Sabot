#pragma once

#include <arrow/api.h>
#include <memory>
#include <string>
#include <map>

// Forward declare Kuzu types (from vendored/)
namespace kuzu {
    namespace main { 
        class Connection; 
        class Database; 
        class QueryResult;
    }
    namespace planner { class LogicalPlan; }
}

namespace sabot_cypher {
namespace cypher {

// Forward declarations
struct ArrowPlan;

struct CypherResult {
    std::shared_ptr<arrow::Table> table;
    std::string query;
    double execution_time_ms;
    size_t num_rows;
};

class SabotCypherBridge {
public:
    static arrow::Result<std::shared_ptr<SabotCypherBridge>> Create();
    
    // Execute Cypher query and return results as Arrow table
    arrow::Result<CypherResult> ExecuteCypher(
        const std::string& query,
        const std::map<std::string, std::string>& params = {});
    
    // Execute ArrowPlan directly (for integration with external parsers)
    arrow::Result<CypherResult> ExecutePlan(const cypher::ArrowPlan& plan);
    
    // Explain query plan
    arrow::Result<std::string> Explain(const std::string& query);
    
    // Register graph data (vertices and edges)
    arrow::Status RegisterGraph(
        std::shared_ptr<arrow::Table> vertices,
        std::shared_ptr<arrow::Table> edges);

private:
    SabotCypherBridge();
    
    std::shared_ptr<kuzu::main::Database> kuzu_db_;
    std::shared_ptr<kuzu::main::Connection> kuzu_conn_;
    std::shared_ptr<arrow::Table> vertices_;
    std::shared_ptr<arrow::Table> edges_;
};

}}  // namespace sabot_cypher::cypher

