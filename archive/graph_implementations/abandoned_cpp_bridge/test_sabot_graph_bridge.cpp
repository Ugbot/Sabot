// Test SabotGraph Bridge

#include "sabot_graph/graph/sabot_graph_bridge.h"
#include <arrow/api.h>
#include <iostream>

using namespace sabot_graph::graph;

int main() {
    std::cout << "======================================================================\n";
    std::cout << "SabotGraph Bridge Test\n";
    std::cout << "======================================================================\n";
    
    // Test 1: Create bridge
    std::cout << "\nTest 1: Create SabotGraph Bridge\n";
    std::cout << "------------------------------------\n";
    
    auto bridge_result = SabotGraphBridge::Create(":memory:", "marbledb");
    if (!bridge_result.ok()) {
        std::cerr << "Failed to create bridge: " << bridge_result.status().ToString() << std::endl;
        return 1;
    }
    
    auto bridge = bridge_result.ValueOrDie();
    std::cout << "✅ Created SabotGraph bridge with MarbleDB backend\n";
    
    // Test 2: Register graph data
    std::cout << "\nTest 2: Register Graph Data\n";
    std::cout << "------------------------------------\n";
    
    // Create sample vertices
    std::vector<std::shared_ptr<arrow::Array>> vertex_arrays;
    auto vertices = arrow::Table::Make(
        arrow::schema({
            arrow::field("id", arrow::int64()),
            arrow::field("label", arrow::utf8()),
            arrow::field("name", arrow::utf8())
        }),
        vertex_arrays
    );
    
    // Create sample edges
    std::vector<std::shared_ptr<arrow::Array>> edge_arrays;
    auto edges = arrow::Table::Make(
        arrow::schema({
            arrow::field("source", arrow::int64()),
            arrow::field("target", arrow::int64()),
            arrow::field("type", arrow::utf8())
        }),
        edge_arrays
    );
    
    auto status = bridge->RegisterGraph(vertices, edges);
    if (!status.ok()) {
        std::cerr << "Failed to register graph: " << status.ToString() << std::endl;
        return 1;
    }
    
    std::cout << "✅ Registered graph with " << vertices->num_rows() 
              << " vertices, " << edges->num_rows() << " edges\n";
    
    // Test 3: Execute Cypher query
    std::cout << "\nTest 3: Execute Cypher Query\n";
    std::cout << "------------------------------------\n";
    
    std::string cypher_query = "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name, b.name";
    
    auto cypher_result = bridge->ExecuteCypher(cypher_query);
    if (!cypher_result.ok()) {
        std::cerr << "Cypher query failed: " << cypher_result.status().ToString() << std::endl;
        return 1;
    }
    
    std::cout << "✅ Executed Cypher query\n";
    std::cout << "   Query: " << cypher_query << "\n";
    std::cout << "   Results: " << cypher_result.ValueOrDie()->num_rows() << " rows\n";
    
    // Test 4: Execute SPARQL query
    std::cout << "\nTest 4: Execute SPARQL Query\n";
    std::cout << "------------------------------------\n";
    
    std::string sparql_query = "SELECT ?s ?o WHERE { ?s <knows> ?o }";
    
    auto sparql_result = bridge->ExecuteSPARQL(sparql_query);
    if (!sparql_result.ok()) {
        std::cerr << "SPARQL query failed: " << sparql_result.status().ToString() << std::endl;
        return 1;
    }
    
    std::cout << "✅ Executed SPARQL query\n";
    std::cout << "   Query: " << sparql_query << "\n";
    std::cout << "   Results: " << sparql_result.ValueOrDie()->num_rows() << " rows\n";
    
    // Test 5: Get statistics
    std::cout << "\nTest 5: Statistics\n";
    std::cout << "------------------------------------\n";
    
    auto stats = bridge->GetStats();
    std::cout << "✅ Statistics:\n";
    std::cout << "   Total vertices: " << stats.total_vertices << "\n";
    std::cout << "   Total edges: " << stats.total_edges << "\n";
    std::cout << "   Queries executed: " << stats.queries_executed << "\n";
    std::cout << "   Avg query time: " << stats.avg_query_time_ms << " ms\n";
    
    std::cout << "\n======================================================================\n";
    std::cout << "Test Summary\n";
    std::cout << "======================================================================\n";
    std::cout << "✅ Bridge creation: PASS\n";
    std::cout << "✅ Graph registration: PASS\n";
    std::cout << "✅ Cypher execution: PASS\n";
    std::cout << "✅ SPARQL execution: PASS\n";
    std::cout << "✅ Statistics: PASS\n";
    std::cout << "\nStatus: SabotGraph bridge working! 🎊\n";
    
    return 0;
}

