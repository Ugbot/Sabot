#include "sabot_cypher/cypher/sabot_cypher_bridge.h"
#include "sabot_cypher/cypher/logical_plan_translator.h"
#include <arrow/api.h>
#include <iostream>

int main() {
    std::cout << "Testing SabotCypher Pattern Matching Operators" << std::endl;
    std::cout << "=============================================" << std::endl;
    
    // Create bridge
    auto bridge_result = sabot_cypher::cypher::SabotCypherBridge::Create();
    if (!bridge_result.ok()) {
        std::cerr << "Failed to create bridge: " << bridge_result.status().ToString() << std::endl;
        return 1;
    }
    auto bridge = bridge_result.ValueOrDie();
    
    // Create simple test graph (demo data)
    // For now, just test the pattern matching operators with empty tables
    auto vertices_schema = arrow::schema({
        arrow::field("id", arrow::int64()),
        arrow::field("label", arrow::utf8()),
        arrow::field("name", arrow::utf8())
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
    
    std::cout << "✅ Graph registered: " << vertices->num_rows() << " vertices, " 
              << edges->num_rows() << " edges" << std::endl;
    
    // Test 2-hop pattern
    std::cout << "\n🔍 Testing 2-hop pattern matching..." << std::endl;
    sabot_cypher::cypher::ArrowPlan plan2hop;
    sabot_cypher::cypher::ArrowOperatorDesc op2hop;
    op2hop.type = "Match2Hop";
    op2hop.params["source_name"] = "a";
    op2hop.params["intermediate_name"] = "b";
    op2hop.params["target_name"] = "c";
    plan2hop.operators.push_back(op2hop);
    
    auto result2hop = bridge->ExecutePlan(plan2hop);
    if (result2hop.ok()) {
        auto res = result2hop.ValueOrDie();
        std::cout << "✅ 2-hop pattern: " << res.num_rows << " matches" << std::endl;
        std::cout << "   Schema: " << res.table->schema()->ToString() << std::endl;
    } else {
        std::cerr << "❌ 2-hop pattern failed: " << result2hop.status().ToString() << std::endl;
    }
    
    // Test 3-hop pattern
    std::cout << "\n🔍 Testing 3-hop pattern matching..." << std::endl;
    sabot_cypher::cypher::ArrowPlan plan3hop;
    sabot_cypher::cypher::ArrowOperatorDesc op3hop;
    op3hop.type = "Match3Hop";
    plan3hop.operators.push_back(op3hop);
    
    auto result3hop = bridge->ExecutePlan(plan3hop);
    if (result3hop.ok()) {
        auto res = result3hop.ValueOrDie();
        std::cout << "✅ 3-hop pattern: " << res.num_rows << " matches" << std::endl;
        std::cout << "   Schema: " << res.table->schema()->ToString() << std::endl;
    } else {
        std::cerr << "❌ 3-hop pattern failed: " << result3hop.status().ToString() << std::endl;
    }
    
    // Test variable-length pattern
    std::cout << "\n🔍 Testing variable-length pattern matching..." << std::endl;
    sabot_cypher::cypher::ArrowPlan planVarLen;
    sabot_cypher::cypher::ArrowOperatorDesc opVarLen;
    opVarLen.type = "MatchVariableLength";
    opVarLen.params["source_vertex"] = "1";
    opVarLen.params["min_hops"] = "1";
    opVarLen.params["max_hops"] = "3";
    planVarLen.operators.push_back(opVarLen);
    
    auto resultVarLen = bridge->ExecutePlan(planVarLen);
    if (resultVarLen.ok()) {
        auto res = resultVarLen.ValueOrDie();
        std::cout << "✅ Variable-length pattern: " << res.num_rows << " matches" << std::endl;
        std::cout << "   Schema: " << res.table->schema()->ToString() << std::endl;
    } else {
        std::cerr << "❌ Variable-length pattern failed: " << resultVarLen.status().ToString() << std::endl;
    }
    
    // Test triangle pattern
    std::cout << "\n🔍 Testing triangle pattern matching..." << std::endl;
    sabot_cypher::cypher::ArrowPlan planTriangle;
    sabot_cypher::cypher::ArrowOperatorDesc opTriangle;
    opTriangle.type = "MatchTriangle";
    planTriangle.operators.push_back(opTriangle);
    
    auto resultTriangle = bridge->ExecutePlan(planTriangle);
    if (resultTriangle.ok()) {
        auto res = resultTriangle.ValueOrDie();
        std::cout << "✅ Triangle pattern: " << res.num_rows << " matches" << std::endl;
        std::cout << "   Schema: " << res.table->schema()->ToString() << std::endl;
    } else {
        std::cerr << "❌ Triangle pattern failed: " << resultTriangle.status().ToString() << std::endl;
    }
    
    std::cout << "\n=============================================" << std::endl;
    std::cout << "Pattern matching test complete!" << std::endl;
    
    return 0;
}
