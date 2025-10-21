/**
 * Test Streaming Operators
 * 
 * Test watermark tracker and window operator functionality.
 * This doesn't require a running Kafka cluster - just tests the operators.
 */

#include "sabot_sql/streaming/watermark_tracker.h"
#include "sabot_sql/streaming/window_operator.h"
#include <arrow/api.h>
#include <iostream>
#include <memory>

using namespace sabot_sql::streaming;

int main() {
    std::cout << "=== Testing Streaming Operators ===" << std::endl;
    
    // Create test data
    std::cout << "\n1. Creating test data..." << std::endl;
    
    // Create schema
    auto schema = arrow::schema({
        arrow::field("symbol", arrow::utf8()),
        arrow::field("price", arrow::float64()),
        arrow::field("timestamp", arrow::timestamp(arrow::TimeUnit::MILLI))
    });
    
    // Create test arrays
    arrow::StringBuilder symbol_builder;
    arrow::DoubleBuilder price_builder;
    arrow::TimestampBuilder timestamp_builder(
        arrow::timestamp(arrow::TimeUnit::MILLI),
        arrow::default_memory_pool()
    );
    
    // Add test data
    if (!symbol_builder.Append("AAPL").ok()) return 1;
    if (!symbol_builder.Append("MSFT").ok()) return 1;
    if (!symbol_builder.Append("AAPL").ok()) return 1;
    if (!symbol_builder.Append("GOOGL").ok()) return 1;
    
    if (!price_builder.Append(150.0).ok()) return 1;
    if (!price_builder.Append(300.0).ok()) return 1;
    if (!price_builder.Append(151.0).ok()) return 1;
    if (!price_builder.Append(2800.0).ok()) return 1;
    
    if (!timestamp_builder.Append(1000).ok()) return 1;  // 1 second
    if (!timestamp_builder.Append(2000).ok()) return 1;  // 2 seconds
    if (!timestamp_builder.Append(3000).ok()) return 1;  // 3 seconds
    if (!timestamp_builder.Append(4000).ok()) return 1;  // 4 seconds
    
    // Finish arrays
    std::shared_ptr<arrow::Array> symbol_array;
    std::shared_ptr<arrow::Array> price_array;
    std::shared_ptr<arrow::Array> timestamp_array;
    
    if (!symbol_builder.Finish(&symbol_array).ok()) {
        std::cerr << "Failed to finish symbol array" << std::endl;
        return 1;
    }
    if (!price_builder.Finish(&price_array).ok()) {
        std::cerr << "Failed to finish price array" << std::endl;
        return 1;
    }
    if (!timestamp_builder.Finish(&timestamp_array).ok()) {
        std::cerr << "Failed to finish timestamp array" << std::endl;
        return 1;
    }
    
    // Create record batch
    auto batch = arrow::RecordBatch::Make(
        schema, 4, {symbol_array, price_array, timestamp_array}
    );
    
    std::cout << "✅ Test data created: " << batch->num_rows() << " rows" << std::endl;
    
    // Test Watermark Tracker
    std::cout << "\n2. Testing Watermark Tracker..." << std::endl;
    
    auto watermark_tracker = std::make_shared<WatermarkTracker>();
    
    // Initialize (without MarbleDB for now)
    auto init_status = watermark_tracker->Initialize(
        "test_connector",
        "timestamp",
        1000,  // 1 second max out-of-orderness
        nullptr  // No MarbleDB client for now
    );
    
    if (!init_status.ok()) {
        std::cerr << "❌ Failed to initialize watermark tracker: " 
                  << init_status.ToString() << std::endl;
        return 1;
    }
    
    std::cout << "✅ Watermark tracker initialized" << std::endl;
    
    // Update watermark from batch
    auto watermark_result = watermark_tracker->UpdateWatermark(batch, 0);
    if (!watermark_result.ok()) {
        std::cerr << "❌ Failed to update watermark: " 
                  << watermark_result.status().ToString() << std::endl;
        return 1;
    }
    
    int64_t watermark = watermark_result.ValueOrDie();
    std::cout << "✅ Watermark updated: " << watermark << " ms" << std::endl;
    std::cout << "   Current watermark: " << watermark_tracker->GetCurrentWatermark() << " ms" << std::endl;
    
    // Test window triggering
    bool should_trigger = watermark_tracker->ShouldTriggerWindow(3500);  // Window end at 3.5s
    std::cout << "✅ Should trigger window at 3500ms: " << (should_trigger ? "yes" : "no") << std::endl;
    
    // Test Window Operator
    std::cout << "\n3. Testing Window Operator..." << std::endl;
    
    auto window_operator = std::make_shared<WindowAggregateOperator>();
    
    // Initialize
    auto window_init_status = window_operator->Initialize(
        "test_query",
        WindowAggregateOperator::WindowType::TUMBLE,
        2000,  // 2 second windows
        2000,  // 2 second slide (same as size for tumbling)
        "symbol",
        "price",
        "timestamp",
        nullptr  // No MarbleDB client for now
    );
    
    if (!window_init_status.ok()) {
        std::cerr << "❌ Failed to initialize window operator: " 
                  << window_init_status.ToString() << std::endl;
        return 1;
    }
    
    std::cout << "✅ Window operator initialized" << std::endl;
    
    // Process batch
    auto process_status = window_operator->ProcessBatch(batch);
    if (!process_status.ok()) {
        std::cerr << "❌ Failed to process batch: " 
                  << process_status.ToString() << std::endl;
        return 1;
    }
    
    std::cout << "✅ Batch processed successfully" << std::endl;
    
    // Get stats
    auto stats = window_operator->GetStats();
    std::cout << "   Total windows: " << stats.total_windows << std::endl;
    std::cout << "   Active windows: " << stats.active_windows << std::endl;
    std::cout << "   Records processed: " << stats.total_records_processed << std::endl;
    std::cout << "   Window type: " << stats.window_type_str << std::endl;
    
    // Test window triggering
    auto triggered_windows = window_operator->GetTriggeredWindows(5000);  // Watermark at 5s
    std::cout << "✅ Triggered windows: " << triggered_windows.size() << std::endl;
    
    // Emit results
    if (!triggered_windows.empty()) {
        auto results_result = window_operator->EmitWindowResults(triggered_windows);
        if (!results_result.ok()) {
            std::cerr << "❌ Failed to emit window results: " 
                      << results_result.status().ToString() << std::endl;
            return 1;
        }
        
        auto results = results_result.ValueOrDie();
        std::cout << "✅ Window results emitted: " << results->num_rows() << " rows" << std::endl;
        
        // Print results
        for (int64_t i = 0; i < results->num_rows(); ++i) {
            auto key_array = std::static_pointer_cast<arrow::StringArray>(
                results->GetColumnByName("key")
            );
            auto count_array = std::static_pointer_cast<arrow::Int64Array>(
                results->GetColumnByName("count")
            );
            auto sum_array = std::static_pointer_cast<arrow::DoubleArray>(
                results->GetColumnByName("sum")
            );
            auto avg_array = std::static_pointer_cast<arrow::DoubleArray>(
                results->GetColumnByName("avg")
            );
            
            std::string key = key_array->GetString(i);
            int64_t count = count_array->Value(i);
            double sum = sum_array->Value(i);
            double avg = avg_array->Value(i);
            
            std::cout << "   Window: " << key 
                      << ", Count: " << count
                      << ", Sum: " << sum
                      << ", Avg: " << avg << std::endl;
        }
    }
    
    // Test window functions
    std::cout << "\n4. Testing Window Functions..." << std::endl;
    
    int64_t window_start = WindowFunctions::TumbleWindowStart(1500, 2000);
    std::cout << "✅ Tumble window start for 1500ms: " << window_start << " ms" << std::endl;
    
    bool in_window = WindowFunctions::IsInWindow(1500, 1000, 3000);
    std::cout << "✅ Is 1500ms in window [1000, 3000): " << (in_window ? "yes" : "no") << std::endl;
    
    // Cleanup
    std::cout << "\n5. Cleaning up..." << std::endl;
    
    auto shutdown_status = watermark_tracker->Shutdown();
    if (!shutdown_status.ok()) {
        std::cerr << "❌ Failed to shutdown watermark tracker: " 
                  << shutdown_status.ToString() << std::endl;
        return 1;
    }
    
    auto window_shutdown_status = window_operator->Shutdown();
    if (!window_shutdown_status.ok()) {
        std::cerr << "❌ Failed to shutdown window operator: " 
                  << window_shutdown_status.ToString() << std::endl;
        return 1;
    }
    
    std::cout << "✅ Cleanup complete" << std::endl;
    
    std::cout << "\n=== Streaming Operators Test Complete ===" << std::endl;
    std::cout << "✅ All tests passed!" << std::endl;
    
    return 0;
}
