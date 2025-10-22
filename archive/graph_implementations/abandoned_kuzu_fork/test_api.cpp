#include "sabot_cypher/cypher/sabot_cypher_bridge.h"
#include "sabot_cypher/cypher/logical_plan_translator.h"
#include "sabot_cypher/execution/arrow_executor.h"
#include <iostream>
#include <arrow/api.h>

using namespace sabot_cypher;

int main() {
    std::cout << "SabotCypher API Test" << std::endl;
    std::cout << "====================" << std::endl << std::endl;
    
    // Test 1: Create bridge
    std::cout << "1. Testing SabotCypherBridge::Create()..." << std::endl;
    auto bridge_result = cypher::SabotCypherBridge::Create();
    if (!bridge_result.ok()) {
        std::cerr << "   Error: " << bridge_result.status() << std::endl;
        return 1;
    }
    auto bridge = bridge_result.ValueOrDie();
    std::cout << "   ✅ Bridge created" << std::endl << std::endl;
    
    // Test 2: Create sample graph data
    std::cout << "2. Creating sample graph data..." << std::endl;
    auto schema_vertices = arrow::schema({
        arrow::field("id", arrow::int64()),
        arrow::field("label", arrow::utf8()),
        arrow::field("name", arrow::utf8()),
    });
    
    auto schema_edges = arrow::schema({
        arrow::field("source", arrow::int64()),
        arrow::field("target", arrow::int64()),
        arrow::field("type", arrow::utf8()),
    });
    
    // Build vertices
    arrow::Int64Builder id_builder;
    arrow::StringBuilder label_builder;
    arrow::StringBuilder name_builder;
    
    id_builder.AppendValues({1, 2, 3});
    label_builder.AppendValues({"Person", "Person", "City"});
    name_builder.AppendValues({"Alice", "Bob", "NYC"});
    
    std::shared_ptr<arrow::Array> id_array, label_array, name_array;
    id_builder.Finish(&id_array);
    label_builder.Finish(&label_array);
    name_builder.Finish(&name_array);
    
    auto vertices = arrow::Table::Make(schema_vertices, {id_array, label_array, name_array});
    
    // Build edges
    arrow::Int64Builder src_builder, tgt_builder;
    arrow::StringBuilder type_builder;
    
    src_builder.AppendValues({1, 2});
    tgt_builder.AppendValues({2, 3});
    type_builder.AppendValues({"KNOWS", "LIVES_IN"});
    
    std::shared_ptr<arrow::Array> src_array, tgt_array, type_array;
    src_builder.Finish(&src_array);
    tgt_builder.Finish(&tgt_array);
    type_builder.Finish(&type_array);
    
    auto edges = arrow::Table::Make(schema_edges, {src_array, tgt_array, type_array});
    
    std::cout << "   ✅ Created " << vertices->num_rows() << " vertices, " 
              << edges->num_rows() << " edges" << std::endl << std::endl;
    
    // Test 3: Register graph
    std::cout << "3. Testing RegisterGraph()..." << std::endl;
    auto status = bridge->RegisterGraph(vertices, edges);
    if (!status.ok()) {
        std::cerr << "   Error: " << status << std::endl;
        return 1;
    }
    std::cout << "   ✅ Graph registered" << std::endl << std::endl;
    
    // Test 4: Create translator
    std::cout << "4. Testing LogicalPlanTranslator::Create()..." << std::endl;
    auto translator_result = cypher::LogicalPlanTranslator::Create();
    if (!translator_result.ok()) {
        std::cerr << "   Error: " << translator_result.status() << std::endl;
        return 1;
    }
    auto translator = translator_result.ValueOrDie();
    std::cout << "   ✅ Translator created" << std::endl << std::endl;
    
    // Test 5: Create executor
    std::cout << "5. Testing ArrowExecutor::Create()..." << std::endl;
    auto executor_result = execution::ArrowExecutor::Create();
    if (!executor_result.ok()) {
        std::cerr << "   Error: " << executor_result.status() << std::endl;
        return 1;
    }
    auto executor = executor_result.ValueOrDie();
    std::cout << "   ✅ Executor created" << std::endl << std::endl;
    
    // Test 6: Create and execute a simple plan
    std::cout << "6. Testing execution pipeline..." << std::endl;
    cypher::ArrowPlan plan;
    
    cypher::ArrowOperatorDesc scan_op;
    scan_op.type = "Scan";
    scan_op.params["table"] = "vertices";
    plan.operators.push_back(scan_op);
    
    cypher::ArrowOperatorDesc limit_op;
    limit_op.type = "Limit";
    limit_op.params["limit"] = "2";
    limit_op.params["offset"] = "0";
    plan.operators.push_back(limit_op);
    
    auto result = executor->Execute(plan, vertices, edges);
    if (!result.ok()) {
        std::cerr << "   Error: " << result.status() << std::endl;
        return 1;
    }
    
    auto result_table = result.ValueOrDie();
    std::cout << "   ✅ Execution successful" << std::endl;
    std::cout << "   Result: " << result_table->num_rows() << " rows" << std::endl;
    std::cout << "   Schema: " << result_table->schema()->ToString() << std::endl;
    std::cout << std::endl;
    
    // Test 7: Execute query (will fail - not implemented yet)
    std::cout << "7. Testing ExecuteCypher() (expected to fail)..." << std::endl;
    auto query_result = bridge->ExecuteCypher("MATCH (a) RETURN a");
    if (!query_result.ok()) {
        std::cout << "   ⚠️  Expected failure: " << query_result.status().message() << std::endl;
    } else {
        auto cypher_result = query_result.ValueOrDie();
        std::cout << "   Unexpected success!" << std::endl;
    }
    std::cout << std::endl;
    
    // Summary
    std::cout << "========================================" << std::endl;
    std::cout << "API Test Summary" << std::endl;
    std::cout << "========================================" << std::endl;
    std::cout << "✅ Bridge creation: PASS" << std::endl;
    std::cout << "✅ Graph registration: PASS" << std::endl;
    std::cout << "✅ Translator creation: PASS" << std::endl;
    std::cout << "✅ Executor creation: PASS" << std::endl;
    std::cout << "✅ Plan execution: PASS" << std::endl;
    std::cout << "⚠️  Cypher execution: Not implemented (expected)" << std::endl;
    std::cout << std::endl;
    std::cout << "Overall: API skeleton working correctly!" << std::endl;
    
    return 0;
}

