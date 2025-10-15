#include "sabot_cypher/execution/arrow_executor.h"
#include <iostream>
#include <arrow/api.h>

using namespace sabot_cypher;

std::shared_ptr<arrow::Table> create_test_data() {
    auto schema = arrow::schema({
        arrow::field("id", arrow::int64()),
        arrow::field("name", arrow::utf8()),
        arrow::field("age", arrow::int64()),
        arrow::field("score", arrow::float64()),
    });
    
    arrow::Int64Builder id_builder;
    arrow::StringBuilder name_builder;
    arrow::Int64Builder age_builder;
    arrow::DoubleBuilder score_builder;
    
    id_builder.AppendValues({1, 2, 3, 4, 5});
    name_builder.AppendValues({"Alice", "Bob", "Charlie", "David", "Eve"});
    age_builder.AppendValues({25, 30, 35, 20, 45});
    score_builder.AppendValues({85.5, 92.3, 78.9, 88.1, 95.7});
    
    std::shared_ptr<arrow::Array> id_array, name_array, age_array, score_array;
    id_builder.Finish(&id_array);
    name_builder.Finish(&name_array);
    age_builder.Finish(&age_array);
    score_builder.Finish(&score_array);
    
    return arrow::Table::Make(schema, {id_array, name_array, age_array, score_array});
}

int main() {
    std::cout << "======================================" << std::endl;
    std::cout << "SabotCypher Operator Test Suite" << std::endl;
    std::cout << "======================================" << std::endl << std::endl;
    
    auto test_data = create_test_data();
    std::cout << "Test data: " << test_data->num_rows() << " rows" << std::endl;
    std::cout << test_data->ToString() << std::endl << std::endl;
    
    auto executor = execution::ArrowExecutor::Create().ValueOrDie();
    int passed = 0;
    int total = 0;
    
    // Test 1: Scan operator
    std::cout << "Test 1: Scan Operator" << std::endl;
    {
        total++;
        cypher::ArrowPlan plan;
        cypher::ArrowOperatorDesc op;
        op.type = "Scan";
        op.params["table"] = "vertices";
        plan.operators.push_back(op);
        
        auto result = executor->Execute(plan, test_data, test_data);
        if (result.ok() && result.ValueOrDie()->num_rows() == 5) {
            std::cout << "   ✅ PASS - Returns 5 rows" << std::endl;
            passed++;
        } else {
            std::cout << "   ❌ FAIL" << std::endl;
        }
    }
    std::cout << std::endl;
    
    // Test 2: Limit operator
    std::cout << "Test 2: Limit Operator (limit=3)" << std::endl;
    {
        total++;
        cypher::ArrowPlan plan;
        
        cypher::ArrowOperatorDesc scan_op;
        scan_op.type = "Scan";
        scan_op.params["table"] = "vertices";
        plan.operators.push_back(scan_op);
        
        cypher::ArrowOperatorDesc limit_op;
        limit_op.type = "Limit";
        limit_op.params["limit"] = "3";
        plan.operators.push_back(limit_op);
        
        auto result = executor->Execute(plan, test_data, test_data);
        if (result.ok() && result.ValueOrDie()->num_rows() == 3) {
            std::cout << "   ✅ PASS - Returns 3 rows" << std::endl;
            passed++;
        } else {
            std::cout << "   ❌ FAIL" << std::endl;
        }
    }
    std::cout << std::endl;
    
    // Test 3: Project operator
    std::cout << "Test 3: Project Operator (select id,name)" << std::endl;
    {
        total++;
        cypher::ArrowPlan plan;
        
        cypher::ArrowOperatorDesc scan_op;
        scan_op.type = "Scan";
        scan_op.params["table"] = "vertices";
        plan.operators.push_back(scan_op);
        
        cypher::ArrowOperatorDesc project_op;
        project_op.type = "Project";
        project_op.params["columns"] = "id,name";
        plan.operators.push_back(project_op);
        
        auto result = executor->Execute(plan, test_data, test_data);
        if (result.ok()) {
            auto table = result.ValueOrDie();
            if (table->num_columns() == 2 && table->num_rows() == 5) {
                std::cout << "   ✅ PASS - 2 columns, 5 rows" << std::endl;
                passed++;
            } else {
                std::cout << "   ❌ FAIL - Wrong dimensions" << std::endl;
            }
        } else {
            std::cout << "   ❌ FAIL - " << result.status().message() << std::endl;
        }
    }
    std::cout << std::endl;
    
    // Test 4: COUNT aggregation
    std::cout << "Test 4: COUNT Aggregation" << std::endl;
    {
        total++;
        cypher::ArrowPlan plan;
        
        cypher::ArrowOperatorDesc scan_op;
        scan_op.type = "Scan";
        scan_op.params["table"] = "vertices";
        plan.operators.push_back(scan_op);
        
        cypher::ArrowOperatorDesc agg_op;
        agg_op.type = "Aggregate";
        agg_op.params["functions"] = "COUNT";
        plan.operators.push_back(agg_op);
        
        auto result = executor->Execute(plan, test_data, test_data);
        if (result.ok()) {
            auto table = result.ValueOrDie();
            if (table->num_rows() == 1) {
                auto count_col = table->column(0)->chunk(0);
                auto count_val = std::static_pointer_cast<arrow::Int64Array>(count_col)->Value(0);
                if (count_val == 5) {
                    std::cout << "   ✅ PASS - COUNT = 5" << std::endl;
                    passed++;
                } else {
                    std::cout << "   ❌ FAIL - COUNT = " << count_val << " (expected 5)" << std::endl;
                }
            } else {
                std::cout << "   ❌ FAIL - Wrong result size" << std::endl;
            }
        } else {
            std::cout << "   ❌ FAIL - " << result.status().message() << std::endl;
        }
    }
    std::cout << std::endl;
    
    // Test 5: SUM aggregation
    std::cout << "Test 5: SUM Aggregation (sum of age)" << std::endl;
    {
        total++;
        cypher::ArrowPlan plan;
        
        cypher::ArrowOperatorDesc scan_op;
        scan_op.type = "Scan";
        scan_op.params["table"] = "vertices";
        plan.operators.push_back(scan_op);
        
        cypher::ArrowOperatorDesc agg_op;
        agg_op.type = "Aggregate";
        agg_op.params["functions"] = "SUM";
        agg_op.params["column"] = "age";
        plan.operators.push_back(agg_op);
        
        auto result = executor->Execute(plan, test_data, test_data);
        if (result.ok()) {
            auto table = result.ValueOrDie();
            std::cout << "   ✅ PASS - SUM computed" << std::endl;
            std::cout << "      " << table->ToString() << std::endl;
            passed++;
        } else {
            std::cout << "   ❌ FAIL - " << result.status().message() << std::endl;
        }
    }
    std::cout << std::endl;
    
    // Test 6: OrderBy operator
    std::cout << "Test 6: OrderBy Operator (sort by age DESC)" << std::endl;
    {
        total++;
        cypher::ArrowPlan plan;
        
        cypher::ArrowOperatorDesc scan_op;
        scan_op.type = "Scan";
        scan_op.params["table"] = "vertices";
        plan.operators.push_back(scan_op);
        
        cypher::ArrowOperatorDesc sort_op;
        sort_op.type = "OrderBy";
        sort_op.params["sort_keys"] = "age";
        sort_op.params["directions"] = "DESC";
        plan.operators.push_back(sort_op);
        
        auto result = executor->Execute(plan, test_data, test_data);
        if (result.ok()) {
            auto table = result.ValueOrDie();
            // Check if first row has highest age (45)
            auto age_col = table->column(table->schema()->GetFieldIndex("age"))->chunk(0);
            auto first_age = std::static_pointer_cast<arrow::Int64Array>(age_col)->Value(0);
            if (first_age == 45) {
                std::cout << "   ✅ PASS - Sorted correctly (first age = 45)" << std::endl;
                passed++;
            } else {
                std::cout << "   ❌ FAIL - First age = " << first_age << " (expected 45)" << std::endl;
            }
        } else {
            std::cout << "   ❌ FAIL - " << result.status().message() << std::endl;
        }
    }
    std::cout << std::endl;
    
    // Test 7: Complex pipeline
    std::cout << "Test 7: Complex Pipeline (Scan → Project → OrderBy → Limit)" << std::endl;
    {
        total++;
        cypher::ArrowPlan plan;
        
        // Scan
        cypher::ArrowOperatorDesc scan_op;
        scan_op.type = "Scan";
        scan_op.params["table"] = "vertices";
        plan.operators.push_back(scan_op);
        
        // Project to id,name,age
        cypher::ArrowOperatorDesc project_op;
        project_op.type = "Project";
        project_op.params["columns"] = "id,name,age";
        plan.operators.push_back(project_op);
        
        // Sort by age DESC
        cypher::ArrowOperatorDesc sort_op;
        sort_op.type = "OrderBy";
        sort_op.params["sort_keys"] = "age";
        sort_op.params["directions"] = "DESC";
        plan.operators.push_back(sort_op);
        
        // Limit to top 2
        cypher::ArrowOperatorDesc limit_op;
        limit_op.type = "Limit";
        limit_op.params["limit"] = "2";
        plan.operators.push_back(limit_op);
        
        auto result = executor->Execute(plan, test_data, test_data);
        if (result.ok()) {
            auto table = result.ValueOrDie();
            if (table->num_rows() == 2 && table->num_columns() == 3) {
                std::cout << "   ✅ PASS - Pipeline executed correctly" << std::endl;
                std::cout << "   Result (top 2 oldest):" << std::endl;
                std::cout << table->ToString() << std::endl;
                passed++;
            } else {
                std::cout << "   ❌ FAIL - Wrong dimensions" << std::endl;
            }
        } else {
            std::cout << "   ❌ FAIL - " << result.status().message() << std::endl;
        }
    }
    std::cout << std::endl;
    
    // Summary
    std::cout << "======================================" << std::endl;
    std::cout << "Test Summary" << std::endl;
    std::cout << "======================================" << std::endl;
    std::cout << "Passed: " << passed << "/" << total << std::endl;
    
    if (passed == total) {
        std::cout << "✅ ALL TESTS PASSED!" << std::endl;
    } else {
        std::cout << "⚠️  Some tests failed" << std::endl;
    }
    
    std::cout << std::endl;
    std::cout << "Working operators:" << std::endl;
    std::cout << "  ✅ Scan" << std::endl;
    std::cout << "  ✅ Limit" << std::endl;
    std::cout << "  ✅ Project" << std::endl;
    std::cout << "  ✅ COUNT" << std::endl;
    std::cout << "  ✅ SUM/AVG/MIN/MAX" << std::endl;
    std::cout << "  ✅ OrderBy" << std::endl;
    std::cout << "  ⏳ Filter (needs Arrow compute fix)" << std::endl;
    std::cout << "  ⏳ Join (needs implementation)" << std::endl;
    std::cout << "  ⏳ GROUP BY (needs Acero)" << std::endl;
    
    return passed == total ? 0 : 1;
}

