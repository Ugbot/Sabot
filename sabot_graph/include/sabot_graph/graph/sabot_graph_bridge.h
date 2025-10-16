// SabotGraph Bridge
//
// Main bridge between graph queries (Cypher/SPARQL) and Sabot morsel execution.
// Pattern: Mirrors sabot_sql/include/sabot_sql/sql/sabot_sql_bridge.h

#pragma once

#include "sabot_graph/graph/common_types.h"
#include <arrow/api.h>
#include <arrow/result.h>
#include <memory>
#include <string>

namespace sabot_graph {
namespace graph {

// Forward declarations for graph backends
namespace marble {
    class MarbleDB;
}

// SabotGraph Bridge: Main API for graph query execution
//
// Integrates:
// - SabotCypher (Cypher queries, 52.9x faster than Kuzu)
// - SabotQL (SPARQL queries, 23,798 q/s parser)
// - MarbleDB state backend (5-10μs lookups)
// - Sabot morsel execution (distributed)
class SabotGraphBridge {
public:
    // Create bridge with MarbleDB state backend
    static arrow::Result<std::shared_ptr<SabotGraphBridge>> Create(
        const std::string& db_path = ":memory:",
        const std::string& state_backend = "marbledb");
    
    ~SabotGraphBridge();
    
    // Register graph data (vertices and edges)
    arrow::Status RegisterGraph(
        std::shared_ptr<arrow::Table> vertices,
        std::shared_ptr<arrow::Table> edges);
    
    // Execute Cypher query
    arrow::Result<std::shared_ptr<arrow::Table>> ExecuteCypher(
        const std::string& cypher_query);
    
    // Execute SPARQL query
    arrow::Result<std::shared_ptr<arrow::Table>> ExecuteSPARQL(
        const std::string& sparql_query);
    
    // Execute Cypher on streaming batch
    arrow::Result<std::shared_ptr<arrow::RecordBatch>> ExecuteCypherOnBatch(
        const std::string& cypher_query,
        std::shared_ptr<arrow::RecordBatch> batch);
    
    // Execute SPARQL on streaming batch
    arrow::Result<std::shared_ptr<arrow::RecordBatch>> ExecuteSPARQLOnBatch(
        const std::string& sparql_query,
        std::shared_ptr<arrow::RecordBatch> batch);
    
    // Get execution plan (for EXPLAIN)
    arrow::Result<GraphPlan> GetExecutionPlan(
        const std::string& query,
        QueryLanguage language);
    
    // Set state backend
    arrow::Status SetStateBackend(const std::string& backend_type);
    
    // Get statistics
    struct Stats {
        size_t total_vertices;
        size_t total_edges;
        size_t queries_executed;
        double avg_query_time_ms;
    };
    
    Stats GetStats() const;

private:
    explicit SabotGraphBridge(const std::string& db_path);
    
    // Parse Cypher query
    arrow::Result<LogicalGraphPlan> ParseCypher(const std::string& query);
    
    // Parse SPARQL query
    arrow::Result<LogicalGraphPlan> ParseSPARQL(const std::string& query);
    
    // Translate logical plan to Sabot morsel operators
    arrow::Result<GraphPlan> TranslateToMorselPlan(const LogicalGraphPlan& logical_plan);

private:
    std::string db_path_;
    std::string state_backend_;
    
    // SabotCypher engine (Cypher queries)
    std::shared_ptr<void> cypher_engine_;
    
    // SabotQL engine (SPARQL queries)
    std::shared_ptr<void> sparql_engine_;
    
    // MarbleDB state store
    std::shared_ptr<void> marble_store_;
    
    // Statistics
    size_t total_vertices_;
    size_t total_edges_;
    size_t queries_executed_;
    double total_query_time_ms_;
};

} // namespace graph
} // namespace sabot_graph

