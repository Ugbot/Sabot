#include "sabot_cypher/cypher/sabot_cypher_bridge.h"
#include "sabot_cypher/cypher/logical_plan_translator.h"
#include <arrow/api.h>
#include <iostream>

int main() {
    std::cout << "Testing SabotCypher Property Access" << std::endl;
    std::cout << "==================================" << std::endl;
    
    // Create bridge
    auto bridge_result = sabot_cypher::cypher::SabotCypherBridge::Create();
    if (!bridge_result.ok()) {
        std::cerr << "Failed to create bridge: " << bridge_result.status().ToString() << std::endl;
        return 1;
    }
    auto bridge = bridge_result.ValueOrDie();
    
    // Create test graph with properties
    auto vertices_schema = arrow::schema({
        arrow::field("id", arrow::int64()),
        arrow::field("name", arrow::utf8()),
        arrow::field("age", arrow::int32()),
        arrow::field("city", arrow::utf8())
    });
    std::vector<std::shared_ptr<arrow::Array>> empty_vertex_arrays;
    auto vertices = arrow::Table::Make(vertices_schema, empty_vertex_arrays);
    
    auto edges_schema = arrow::schema({
        arrow::field("source", arrow::int64()),
        arrow::field("target", arrow::int64()),
        arrow::field("type", arrow::utf8())
    });
    std::vector<std::shared_ptr<arrow::Array>> empty_edge_arrays;
    auto edges = arrow::Table::Make(edges_schema, empty_edge_arrays);
    
    // Register graph
    auto reg_status = bridge->RegisterGraph(vertices, edges);
    if (!reg_status.ok()) {
        std::cerr << "Failed to register graph: " << reg_status.ToString() << std::endl;
        return 1;
    }
    
    std::cout << "✅ Graph registered with properties: name, age, city" << std::endl;
    
    // Test property access
    std::cout << "\n🔍 Testing property access..." << std::endl;
    sabot_cypher::cypher::ArrowPlan plan;
    sabot_cypher::cypher::ArrowOperatorDesc op;
    op.type = "PropertyAccess";
    op.params["variable"] = "a";
    op.params["property"] = "name";
    op.params["alias"] = "person_name";
    plan.operators.push_back(op);
    
    auto result = bridge->ExecutePlan(plan);
    if (result.ok()) {
        auto res = result.ValueOrDie();
        std::cout << "✅ Property access: " << res.num_rows << " rows" << std::endl;
        std::cout << "   Schema: " << res.table->schema()->ToString() << std::endl;
    } else {
        std::cerr << "❌ Property access failed: " << result.status().ToString() << std::endl;
    }
    
    // Test property access in Project operator
    std::cout << "\n🔍 Testing property access in Project..." << std::endl;
    sabot_cypher::cypher::ArrowPlan plan2;
    sabot_cypher::cypher::ArrowOperatorDesc op2;
    op2.type = "Project";
    op2.params["columns"] = "a.name,b.age";
    plan2.operators.push_back(op2);
    
    auto result2 = bridge->ExecutePlan(plan2);
    if (result2.ok()) {
        auto res = result2.ValueOrDie();
        std::cout << "✅ Project with property access: " << res.num_rows << " rows" << std::endl;
        std::cout << "   Schema: " << res.table->schema()->ToString() << std::endl;
    } else {
        std::cerr << "❌ Project with property access failed: " << result2.status().ToString() << std::endl;
    }
    
    std::cout << "\n==================================" << std::endl;
    std::cout << "Property access test complete!" << std::endl;
    
    return 0;
}
