#include "sabot_cypher/cypher/sabot_cypher_bridge.h"
#include "sabot_cypher/cypher/logical_plan_translator.h"
#include "sabot_cypher/execution/arrow_executor.h"
#include <chrono>

// Include ArrowPlan definition
namespace sabot_cypher {
namespace cypher {
    // ArrowPlan is defined in logical_plan_translator.h
}
}

namespace sabot_cypher {
namespace cypher {

SabotCypherBridge::SabotCypherBridge() {
    // Will initialize Kuzu database in-memory
}

arrow::Result<std::shared_ptr<SabotCypherBridge>> SabotCypherBridge::Create() {
    try {
        auto bridge = std::shared_ptr<SabotCypherBridge>(new SabotCypherBridge());
        
        // TODO: Initialize Kuzu database and connection
        // For now, return placeholder
        
        return bridge;
    } catch (const std::exception& e) {
        return arrow::Status::IOError(
            "Failed to create SabotCypherBridge: " + std::string(e.what()));
    }
}

arrow::Result<CypherResult> SabotCypherBridge::ExecuteCypher(
    const std::string& query,
    const std::map<std::string, std::string>& params) {
    
    // For now, return not implemented
    // Use ExecutePlan() with external parser (Lark) instead
    
    return arrow::Status::NotImplemented(
        "ExecuteCypher requires Cypher parser integration. "
        "Use ExecutePlan() with external parser (Lark) for now. "
        "Query: " + query);
}

arrow::Result<CypherResult> SabotCypherBridge::ExecutePlan(const cypher::ArrowPlan& plan) {
    auto start = std::chrono::high_resolution_clock::now();
    
    if (!vertices_ || !edges_) {
        return arrow::Status::Invalid("Graph not registered. Call RegisterGraph() first.");
    }
    
    // Create executor
    auto executor_result = execution::ArrowExecutor::Create();
    if (!executor_result.ok()) {
        return executor_result.status();
    }
    auto executor = executor_result.ValueOrDie();
    
    // Execute plan
    ARROW_ASSIGN_OR_RAISE(auto result_table, 
        executor->Execute(plan, vertices_, edges_));
    
    auto end = std::chrono::high_resolution_clock::now();
    double elapsed_ms = std::chrono::duration<double, std::milli>(end - start).count();
    
    CypherResult result;
    result.table = result_table;
    result.query = "<direct_plan>";
    result.execution_time_ms = elapsed_ms;
    result.num_rows = result_table->num_rows();
    
    return result;
}

arrow::Result<std::string> SabotCypherBridge::Explain(const std::string& query) {
    // TODO: Parse query and return logical plan string
    return "EXPLAIN not yet implemented for query: " + query;
}

arrow::Status SabotCypherBridge::RegisterGraph(
    std::shared_ptr<arrow::Table> vertices,
    std::shared_ptr<arrow::Table> edges) {
    
    if (!vertices) {
        return arrow::Status::Invalid("Vertices table cannot be null");
    }
    if (!edges) {
        return arrow::Status::Invalid("Edges table cannot be null");
    }
    
    vertices_ = vertices;
    edges_ = edges;
    
    return arrow::Status::OK();
}

}}  // namespace sabot_cypher::cypher

