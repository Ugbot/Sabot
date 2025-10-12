#include <iostream>
#include <string>
#include <vector>
#include "sabot_sql/sql/enhanced_parser.h"
#include "sabot_sql/sql/streaming_operators.h"

using namespace sabot_sql::sql;

int main() {
    std::cout << "🚀 SabotSQL Enhanced Parser Test" << std::endl;
    std::cout << "=================================" << std::endl;
    
    // Create enhanced parser
    EnhancedParserConfig config;
    config.enable_streaming_constructs = true;
    config.enable_time_series_functions = true;
    config.enable_window_functions = true;
    config.enable_flink_syntax = true;
    config.enable_questdb_syntax = true;
    
    auto parser_result = EnhancedSQLParser::Create(config);
    if (!parser_result.ok()) {
        std::cerr << "❌ Failed to create parser: " << parser_result.status().ToString() << std::endl;
        return 1;
    }
    
    auto parser = parser_result.ValueOrDie();
    
    // Test SQL queries
    std::vector<std::string> test_queries = {
        // Flink SQL streaming constructs
        "SELECT * FROM orders WHERE order_time > CURRENT_TIMESTAMP - INTERVAL '1' HOUR",
        "SELECT TUMBLE_START(order_time, INTERVAL '1' MINUTE) as window_start, COUNT(*) as order_count FROM orders GROUP BY TUMBLE(order_time, INTERVAL '1' MINUTE)",
        "SELECT HOP_START(order_time, INTERVAL '30' SECOND, INTERVAL '1' MINUTE) as window_start, SUM(amount) as total_amount FROM orders GROUP BY HOP(order_time, INTERVAL '30' SECOND, INTERVAL '1' MINUTE)",
        
        // QuestDB time-series functions
        "SELECT * FROM trades SAMPLE BY 1h",
        "SELECT * FROM trades LATEST BY symbol",
        "SELECT * FROM trades ASOF JOIN prices ON trades.symbol = prices.symbol",
        
        // Window functions
        "SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_time) as row_num FROM orders",
        "SELECT *, RANK() OVER (PARTITION BY customer_id ORDER BY amount DESC) as rank FROM orders",
        "SELECT *, LAG(amount, 1) OVER (PARTITION BY customer_id ORDER BY order_time) as prev_amount FROM orders",
        
        // Standard SQL
        "SELECT customer_id, COUNT(*) as order_count, SUM(amount) as total_amount FROM orders GROUP BY customer_id",
        "SELECT o.*, c.name FROM orders o JOIN customers c ON o.customer_id = c.id"
    };
    
    std::cout << "\n📝 Testing SQL Queries:" << std::endl;
    std::cout << "========================" << std::endl;
    
    for (size_t i = 0; i < test_queries.size(); ++i) {
        const auto& sql = test_queries[i];
        std::cout << "\n" << (i + 1) << ". " << sql << std::endl;
        
        // Check for streaming constructs
        if (parser->HasStreamingConstructs(sql)) {
            std::cout << "   ✅ Contains streaming constructs" << std::endl;
        }
        
        // Check for time-series functions
        if (parser->HasTimeSeriesFunctions(sql)) {
            std::cout << "   ✅ Contains time-series functions" << std::endl;
        }
        
        // Check for window functions
        if (parser->HasWindowFunctions(sql)) {
            std::cout << "   ✅ Contains window functions" << std::endl;
        }
        
        // Try to parse
        auto parse_result = parser->ParseEnhancedSQL(sql);
        if (parse_result.ok()) {
            std::cout << "   ✅ Parsed successfully" << std::endl;
        } else {
            std::cout << "   ❌ Parse failed: " << parse_result.status().ToString() << std::endl;
        }
    }
    
    // Test supported features
    std::cout << "\n🔧 Supported Features:" << std::endl;
    std::cout << "=======================" << std::endl;
    
    auto streaming_constructs = parser->GetSupportedStreamingConstructs();
    std::cout << "Streaming Constructs (" << streaming_constructs.size() << "):" << std::endl;
    for (const auto& construct : streaming_constructs) {
        std::cout << "  - " << static_cast<int>(construct) << std::endl;
    }
    
    auto timeseries_functions = parser->GetSupportedTimeSeriesFunctions();
    std::cout << "Time-Series Functions (" << timeseries_functions.size() << "):" << std::endl;
    for (const auto& function : timeseries_functions) {
        std::cout << "  - " << static_cast<int>(function) << std::endl;
    }
    
    auto window_functions = parser->GetSupportedWindowFunctions();
    std::cout << "Window Functions (" << window_functions.size() << "):" << std::endl;
    for (const auto& function : window_functions) {
        std::cout << "  - " << static_cast<int>(function) << std::endl;
    }
    
    // Test syntax help
    std::cout << "\n📚 Syntax Help:" << std::endl;
    std::cout << "===============" << std::endl;
    std::cout << parser->GetSyntaxHelp() << std::endl;
    
    // Test validation
    std::cout << "\n✅ Validation Tests:" << std::endl;
    std::cout << "===================" << std::endl;
    
    std::vector<std::string> validation_queries = {
        "SELECT * FROM orders",  // Valid
        "INVALID SQL SYNTAX",    // Invalid
        "SELECT TUMBLE(order_time, INTERVAL '1' MINUTE) FROM orders"  // Valid with streaming
    };
    
    for (const auto& sql : validation_queries) {
        auto validation_result = parser->ValidateSQL(sql);
        if (validation_result.ok()) {
            std::cout << "✅ Valid: " << sql << std::endl;
        } else {
            std::cout << "❌ Invalid: " << sql << " - " << validation_result.status().ToString() << std::endl;
        }
    }
    
    std::cout << "\n🎉 Enhanced Parser Test Complete!" << std::endl;
    return 0;
}
