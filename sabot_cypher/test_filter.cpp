#include "sabot_cypher/execution/arrow_executor.h"
#include "sabot_cypher/cypher/expression_evaluator.h"
#include <iostream>
#include <arrow/api.h>

using namespace sabot_cypher;

int main() {
    std::cout << "SabotCypher Filter Operator Test" << std::endl;
    std::cout << "=================================" << std::endl << std::endl;
    
    // Create test data with ages
    auto schema = arrow::schema({
        arrow::field("id", arrow::int64()),
        arrow::field("name", arrow::utf8()),
        arrow::field("age", arrow::int64()),
    });
    
    arrow::Int64Builder id_builder;
    arrow::StringBuilder name_builder;
    arrow::Int64Builder age_builder;
    
    id_builder.AppendValues({1, 2, 3, 4, 5});
    name_builder.AppendValues({"Alice", "Bob", "Charlie", "David", "Eve"});
    age_builder.AppendValues({25, 30, 35, 20, 45});
    
    std::shared_ptr<arrow::Array> id_array, name_array, age_array;
    id_builder.Finish(&id_array);
    name_builder.Finish(&name_array);
    age_builder.Finish(&age_array);
    
    auto table = arrow::Table::Make(schema, {id_array, name_array, age_array});
    
    std::cout << "Input table: " << table->num_rows() << " rows" << std::endl;
    std::cout << table->ToString() << std::endl << std::endl;
    
    // Test 1: Simple filter (age > 25)
    std::cout << "Test 1: Filter 'age > 25'" << std::endl;
    {
        cypher::ArrowPlan plan;
        cypher::ArrowOperatorDesc filter_op;
        filter_op.type = "Filter";
        filter_op.params["predicate"] = "age > 25";
        plan.operators.push_back(filter_op);
        
        auto executor = execution::ArrowExecutor::Create().ValueOrDie();
        auto result = executor->Execute(plan, table, table);  // Use same table for vertices/edges
        
        if (result.ok()) {
            auto filtered = result.ValueOrDie();
            std::cout << "   ✅ Filter executed successfully" << std::endl;
            std::cout << "   Result: " << filtered->num_rows() << " rows (expected 3)" << std::endl;
            std::cout << filtered->ToString() << std::endl;
        } else {
            std::cout << "   ❌ Filter failed: " << result.status() << std::endl;
        }
    }
    std::cout << std::endl;
    
    // Test 2: Comparison filter (age <= 30)
    std::cout << "Test 2: Filter 'age <= 30'" << std::endl;
    {
        cypher::ArrowPlan plan;
        cypher::ArrowOperatorDesc filter_op;
        filter_op.type = "Filter";
        filter_op.params["predicate"] = "age <= 30";
        plan.operators.push_back(filter_op);
        
        auto executor = execution::ArrowExecutor::Create().ValueOrDie();
        auto result = executor->Execute(plan, table, table);
        
        if (result.ok()) {
            auto filtered = result.ValueOrDie();
            std::cout << "   ✅ Filter executed successfully" << std::endl;
            std::cout << "   Result: " << filtered->num_rows() << " rows (expected 3)" << std::endl;
        } else {
            std::cout << "   ❌ Filter failed: " << result.status() << std::endl;
        }
    }
    std::cout << std::endl;
    
    // Test 3: Combined filters (age > 25 AND age < 40)
    std::cout << "Test 3: Filter 'age > 25 AND age < 40'" << std::endl;
    {
        cypher::ArrowPlan plan;
        cypher::ArrowOperatorDesc filter_op;
        filter_op.type = "Filter";
        filter_op.params["predicate"] = "age > 25 AND age < 40";
        plan.operators.push_back(filter_op);
        
        auto executor = execution::ArrowExecutor::Create().ValueOrDie();
        auto result = executor->Execute(plan, table, table);
        
        if (result.ok()) {
            auto filtered = result.ValueOrDie();
            std::cout << "   ✅ Filter executed successfully" << std::endl;
            std::cout << "   Result: " << filtered->num_rows() << " rows (expected 2)" << std::endl;
        } else {
            std::cout << "   ❌ Filter failed: " << result.status() << std::endl;
        }
    }
    std::cout << std::endl;
    
    // Test 4: Filter + Limit pipeline
    std::cout << "Test 4: Pipeline 'age >= 30' → Limit 2" << std::endl;
    {
        cypher::ArrowPlan plan;
        
        cypher::ArrowOperatorDesc filter_op;
        filter_op.type = "Filter";
        filter_op.params["predicate"] = "age >= 30";
        plan.operators.push_back(filter_op);
        
        cypher::ArrowOperatorDesc limit_op;
        limit_op.type = "Limit";
        limit_op.params["limit"] = "2";
        plan.operators.push_back(limit_op);
        
        auto executor = execution::ArrowExecutor::Create().ValueOrDie();
        auto result = executor->Execute(plan, table, table);
        
        if (result.ok()) {
            auto filtered = result.ValueOrDie();
            std::cout << "   ✅ Pipeline executed successfully" << std::endl;
            std::cout << "   Result: " << filtered->num_rows() << " rows (expected 2)" << std::endl;
            std::cout << filtered->ToString() << std::endl;
        } else {
            std::cout << "   ❌ Pipeline failed: " << result.status() << std::endl;
        }
    }
    std::cout << std::endl;
    
    std::cout << "========================================" << std::endl;
    std::cout << "Filter Operator Test Complete" << std::endl;
    std::cout << "========================================" << std::endl;
    
    return 0;
}

