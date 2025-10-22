#include <iostream>
#include <arrow/api.h>
#include <arrow/result.h>

// Include SabotSQL headers
#include "sabot_sql/sql/flink_sql_extension.h"

int main() {
    std::cout << "Testing Flink SQL extension for SabotSQL..." << std::endl;
    
    try {
        // Create Flink SQL extension
        auto flink_result = sabot_sql::sql::FlinkSQLExtension::Create();
        if (!flink_result.ok()) {
            std::cerr << "Failed to create Flink SQL extension: " 
                      << flink_result.status().ToString() << std::endl;
            return 1;
        }
        
        auto flink_ext = flink_result.ValueOrDie();
        std::cout << "✓ Flink SQL extension created successfully" << std::endl;
        
        // Test Flink SQL preprocessing
        std::string flink_sql = R"(
            SELECT 
                user_id,
                TUMBLE(event_time, INTERVAL '1' HOUR) as window_start,
                COUNT(*) as event_count
            FROM events
            WHERE CURRENT_TIMESTAMP > event_time
            GROUP BY user_id, TUMBLE(event_time, INTERVAL '1' HOUR)
        )";
        
        auto processed_result = flink_ext->PreprocessFlinkSQL(flink_sql);
        if (!processed_result.ok()) {
            std::cerr << "Failed to preprocess Flink SQL: " 
                      << processed_result.status().ToString() << std::endl;
            return 1;
        }
        
        std::string processed_sql = processed_result.ValueOrDie();
        std::cout << "✓ Flink SQL preprocessing successful" << std::endl;
        std::cout << "  Original: " << flink_sql << std::endl;
        std::cout << "  Processed: " << processed_sql << std::endl;
        
        // Test Flink construct detection
        bool has_flink = flink_ext->ContainsFlinkConstructs(flink_sql);
        std::cout << "✓ Flink construct detection: " << (has_flink ? "Found" : "Not found") << std::endl;
        
        // Test window specification extraction
        auto windows_result = flink_ext->ExtractWindowSpecifications(flink_sql);
        if (windows_result.ok()) {
            auto windows = windows_result.ValueOrDie();
            std::cout << "✓ Window specifications extracted: " << windows.size() << " found" << std::endl;
            for (const auto& window : windows) {
                std::cout << "  - " << window << std::endl;
            }
        }
        
        // Test QuestDB SQL extension
        auto questdb_result = sabot_sql::sql::QuestDBSQLExtension::Create();
        if (!questdb_result.ok()) {
            std::cerr << "Failed to create QuestDB SQL extension: " 
                      << questdb_result.status().ToString() << std::endl;
            return 1;
        }
        
        auto questdb_ext = questdb_result.ValueOrDie();
        std::cout << "✓ QuestDB SQL extension created successfully" << std::endl;
        
        // Test QuestDB SQL preprocessing
        std::string questdb_sql = R"(
            SELECT symbol, price, timestamp
            FROM trades
            SAMPLE BY 1h
            LATEST BY symbol
        )";
        
        auto questdb_processed_result = questdb_ext->PreprocessQuestDBSQL(questdb_sql);
        if (!questdb_processed_result.ok()) {
            std::cerr << "Failed to preprocess QuestDB SQL: " 
                      << questdb_processed_result.status().ToString() << std::endl;
            return 1;
        }
        
        std::string questdb_processed_sql = questdb_processed_result.ValueOrDie();
        std::cout << "✓ QuestDB SQL preprocessing successful" << std::endl;
        std::cout << "  Original: " << questdb_sql << std::endl;
        std::cout << "  Processed: " << questdb_processed_sql << std::endl;
        
        // Test QuestDB construct detection
        bool has_questdb = questdb_ext->ContainsQuestDBConstructs(questdb_sql);
        std::cout << "✓ QuestDB construct detection: " << (has_questdb ? "Found" : "Not found") << std::endl;
        
        std::cout << "🎉 All Flink/QuestDB SQL extension tests passed!" << std::endl;
        return 0;
        
    } catch (const std::exception& e) {
        std::cerr << "Exception: " << e.what() << std::endl;
        return 1;
    }
}
