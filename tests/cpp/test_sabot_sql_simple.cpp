#include <iostream>
#include <arrow/api.h>
#include <arrow/result.h>

// Include SabotSQL headers
#include "sabot_sql/sql/simple_sabot_sql_bridge.h"

int main() {
    std::cout << "Testing renamed SabotSQL implementation..." << std::endl;
    
    try {
        // Create SabotSQL bridge
        auto bridge_result = sabot_sql::sql::SimpleSabotSQLBridge::Create();
        if (!bridge_result.ok()) {
            std::cerr << "Failed to create SabotSQL bridge: " 
                      << bridge_result.status().ToString() << std::endl;
            return 1;
        }
        
        auto bridge = bridge_result.ValueOrDie();
        std::cout << "✓ SabotSQL bridge created successfully" << std::endl;
        
        // Create a simple test table manually
        auto schema = arrow::schema({
            arrow::field("id", arrow::int64()),
            arrow::field("name", arrow::utf8()),
            arrow::field("value", arrow::float64())
        });
        
        // Create simple arrays manually
        arrow::Int64Builder id_builder;
        arrow::StringBuilder name_builder;
        arrow::DoubleBuilder value_builder;
        
        auto status = id_builder.AppendValues({1, 2, 3, 4, 5});
        if (!status.ok()) {
            std::cerr << "Failed to append id values: " << status.ToString() << std::endl;
            return 1;
        }
        
        status = name_builder.AppendValues({"Alice", "Bob", "Charlie", "David", "Eve"});
        if (!status.ok()) {
            std::cerr << "Failed to append name values: " << status.ToString() << std::endl;
            return 1;
        }
        
        status = value_builder.AppendValues({10.5, 20.3, 30.7, 40.1, 50.9});
        if (!status.ok()) {
            std::cerr << "Failed to append value values: " << status.ToString() << std::endl;
            return 1;
        }
        
        std::shared_ptr<arrow::Array> id_array, name_array, value_array;
        status = id_builder.Finish(&id_array);
        if (!status.ok()) {
            std::cerr << "Failed to finish id array: " << status.ToString() << std::endl;
            return 1;
        }
        
        status = name_builder.Finish(&name_array);
        if (!status.ok()) {
            std::cerr << "Failed to finish name array: " << status.ToString() << std::endl;
            return 1;
        }
        
        status = value_builder.Finish(&value_array);
        if (!status.ok()) {
            std::cerr << "Failed to finish value array: " << status.ToString() << std::endl;
            return 1;
        }
        
        auto table = arrow::Table::Make(schema, {id_array, name_array, value_array});
        
        // Register table
        status = bridge->RegisterTable("test_table", table);
        if (!status.ok()) {
            std::cerr << "Failed to register table: " << status.ToString() << std::endl;
            return 1;
        }
        
        std::cout << "✓ Test table registered successfully" << std::endl;
        
        // Test SQL parsing
        std::string sql = "SELECT id, name, value FROM test_table";
        auto plan_result = bridge->ParseAndOptimize(sql);
        if (!plan_result.ok()) {
            std::cerr << "Failed to parse SQL: " << plan_result.status().ToString() << std::endl;
            return 1;
        }
        
        std::cout << "✓ SQL parsing successful" << std::endl;
        
        // Test SQL execution
        auto result = bridge->ExecuteSQL(sql);
        if (!result.ok()) {
            std::cerr << "Failed to execute SQL: " << result.status().ToString() << std::endl;
            return 1;
        }
        
        auto result_table = result.ValueOrDie();
        std::cout << "✓ SQL execution successful" << std::endl;
        std::cout << "  Result: " << result_table->num_rows() << " rows, " 
                  << result_table->num_columns() << " columns" << std::endl;
        
        std::cout << "🎉 All tests passed! SabotSQL is working correctly." << std::endl;
        return 0;
        
    } catch (const std::exception& e) {
        std::cerr << "Exception: " << e.what() << std::endl;
        return 1;
    }
}
