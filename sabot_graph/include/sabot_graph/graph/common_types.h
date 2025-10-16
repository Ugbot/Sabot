// SabotGraph Common Types
//
// Graph query plan structures for Sabot morsel execution.
// Pattern: Mirrors sabot_sql/include/sabot_sql/sql/common_types.h

#pragma once

#include <string>
#include <vector>
#include <memory>
#include <unordered_map>
#include <arrow/api.h>

namespace sabot_graph {
namespace graph {

// Graph operator descriptor for Sabot morsel execution
struct GraphOperatorDescriptor {
    std::string type;  // "VertexScan", "EdgeScan", "Match2Hop", "Match3Hop", "GraphFilter", "GraphJoin", "PropertyAccess"
    std::unordered_map<std::string, std::string> params;  // Stringified parameters
    bool is_stateful = false;   // Requires MarbleDB state backend
    bool is_broadcast = false;  // Broadcast to all agents (dimension pattern)
};

// Graph execution plan for Sabot morsel operators
struct GraphPlan {
    std::string plan_type = "graph_execution";
    std::shared_ptr<void> root_operator;
    std::vector<std::shared_ptr<void>> operators;
    
    // Operator pipeline for Sabot executor
    std::vector<std::string> operator_pipeline;
    std::vector<GraphOperatorDescriptor> operator_descriptors;
    
    // Output schema
    std::shared_ptr<arrow::Schema> output_schema;
    
    // Plan features
    bool has_pattern_matching = false;
    bool has_multi_hop = false;
    bool has_aggregates = false;
    bool has_filters = false;
    bool is_streaming = false;
    
    // Execution hints
    std::vector<std::string> vertex_labels;
    std::vector<std::string> edge_types;
    size_t estimated_vertices = 0;
    size_t estimated_edges = 0;
    
    // Streaming config (when is_streaming = true)
    int max_parallelism = 1;           // Kafka partition sources
    std::string checkpoint_interval;   // e.g., "60s"
    std::string state_backend = "marbledb";  // MarbleDB with RAFT for graphs
    std::string window_type;           // "tumbling", "sliding", "session"
    std::string window_size;           // e.g., "5m"
};

// Query language type
enum class QueryLanguage {
    CYPHER,
    SPARQL
};

// Logical plan for graph queries
struct LogicalGraphPlan {
    QueryLanguage language;
    std::shared_ptr<void> parsed_query;  // Cypher AST or SPARQL Query
    std::shared_ptr<void> root_operator;
    bool has_pattern_matching = false;
    bool has_aggregates = false;
    bool has_filters = false;
    size_t estimated_cardinality = 0;
};

} // namespace graph
} // namespace sabot_graph

