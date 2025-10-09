#include <marble/marble.h>
#include <iostream>
#include <memory>
#include <vector>

using namespace marble;

int main() {
    std::cout << "MarbleDB Streaming Query API Demo" << std::endl;
    std::cout << "==================================" << std::endl;

    // Demonstrate the streaming query API without a full database
    // This shows the interface that would be used with a real MarbleDB instance

    std::cout << "\n=== Streaming Query API Overview ===" << std::endl;
    std::cout << "MarbleDB provides Arrow-compatible streaming queries that return" << std::endl;
    std::cout << "RecordBatches incrementally, perfect for large datasets." << std::endl;

    // Create Arrow schema for our data
    auto id_field = arrow::field("id", arrow::int64());
    auto name_field = arrow::field("name", arrow::utf8());
    auto value_field = arrow::field("value", arrow::utf8());
    auto schema = arrow::schema({id_field, name_field, value_field});

    std::cout << "\nSchema: " << schema->ToString() << std::endl;

    // Example 1: Query Builder API
    std::cout << "\n=== Example 1: Query Builder API ===" << std::endl;
    std::cout << "Fluent API for building queries:" << std::endl;
    std::cout << "```cpp" << std::endl;
    std::cout << "// MarbleDB* db = ...; // Your database instance" << std::endl;
    std::cout << "// auto query = QueryFrom(db)" << std::endl;
    std::cout << "//     .Select({\"id\", \"name\", \"value\"})" << std::endl;
    std::cout << "//     .Where(\"id\", PredicateType::kGreaterThan, arrow::MakeScalar(100))" << std::endl;
    std::cout << "//     .Where(\"name\", PredicateType::kLike, arrow::MakeScalar(\"user_%\"))" << std::endl;
    std::cout << "//     .Limit(10000)" << std::endl;
    std::cout << "//     .Offset(500)" << std::endl;
    std::cout << "//     .Build();" << std::endl;
    std::cout << "// " << std::endl;
    std::cout << "// std::unique_ptr<QueryResult> result;" << std::endl;
    std::cout << "// query->Execute(&result);" << std::endl;
    std::cout << "```" << std::endl;

    // Example 2: Synchronous Streaming
    std::cout << "\n=== Example 2: Synchronous Streaming ===" << std::endl;
    std::cout << "Process results in batches as they become available:" << std::endl;
    std::cout << "```cpp" << std::endl;
    std::cout << "// while (result->HasNext()) {" << std::endl;
    std::cout << "//     std::shared_ptr<arrow::RecordBatch> batch;" << std::endl;
    std::cout << "//     Status status = result->Next(&batch);" << std::endl;
    std::cout << "//     if (status.ok() && batch) {" << std::endl;
    std::cout << "//         // Process batch - send over network, write to file, etc." << std::endl;
    std::cout << "//         std::cout << \"Processed \" << batch->num_rows() << \" rows\" << std::endl;" << std::endl;
    std::cout << "//     }" << std::endl;
    std::cout << "// }" << std::endl;
    std::cout << "```" << std::endl;

    // Example 3: Asynchronous Streaming
    std::cout << "\n=== Example 3: Asynchronous Streaming ===" << std::endl;
    std::cout << "Non-blocking processing with callbacks:" << std::endl;
    std::cout << "```cpp" << std::endl;
    std::cout << "// auto process_batch = [](Status status, std::shared_ptr<arrow::RecordBatch> batch) {" << std::endl;
    std::cout << "//     if (status.ok() && batch) {" << std::endl;
    std::cout << "//         // Async processing - network I/O, file writing, etc." << std::endl;
    std::cout << "//         async_write_to_network(batch);" << std::endl;
    std::cout << "//     }" << std::endl;
    std::cout << "// };" << std::endl;
    std::cout << "// " << std::endl;
    std::cout << "// while (result->HasNext()) {" << std::endl;
    std::cout << "//     result->NextAsync(process_batch);" << std::endl;
    std::cout << "// }" << std::endl;
    std::cout << "```" << std::endl;

    // Example 4: Arrow Dataset Interface
    std::cout << "\n=== Example 4: Arrow Dataset Compatibility ===" << std::endl;
    std::cout << "Looks like Arrow Dataset from the outside:" << std::endl;
    std::cout << "```cpp" << std::endl;
    std::cout << "// auto dataset = DatasetFrom(db);" << std::endl;
    std::cout << "// auto scanner = dataset->NewScanner();" << std::endl;
    std::cout << "// " << std::endl;
    std::cout << "// std::vector<ColumnPredicate> predicates = {" << std::endl;
    std::cout << "//     ColumnPredicate(\"age\", PredicateType::kGreaterThan, arrow::MakeScalar(18))" << std::endl;
    std::cout << "// };" << std::endl;
    std::cout << "// " << std::endl;
    std::cout << "// QueryOptions options;" << std::endl;
    std::cout << "// options.batch_size = 1024;" << std::endl;
    std::cout << "// options.limit = 100000;" << std::endl;
    std::cout << "// " << std::endl;
    std::cout << "// std::unique_ptr<QueryResult> result;" << std::endl;
    std::cout << "// scanner->Scan(predicates, options, &result);" << std::endl;
    std::cout << "```" << std::endl;

    // Example 5: Predicate Pushdown & Column Projection
    std::cout << "\n=== Example 5: Predicate Pushdown & Column Projection ===" << std::endl;
    std::cout << "Advanced query with filtering and selective column retrieval:" << std::endl;
    std::cout << "```cpp" << std::endl;
    std::cout << "// Create predicates for pushdown" << std::endl;
    std::cout << "std::vector<ColumnPredicate> predicates = {" << std::endl;
    std::cout << "    ColumnPredicate(\"id\", PredicateType::kGreaterThan, arrow::MakeScalar<int64_t>(50))," << std::endl;
    std::cout << "    ColumnPredicate(\"id\", PredicateType::kLessThan, arrow::MakeScalar<int64_t>(200))" << std::endl;
    std::cout << "};" << std::endl;
    std::cout << "// " << std::endl;
    std::cout << "// Query options with column projection" << std::endl;
    std::cout << "QueryOptions options;" << std::endl;
    std::cout << "options.projection_columns = {\"id\", \"name\"}; // Only select id and name" << std::endl;
    std::cout << "options.batch_size = 50;" << std::endl;
    std::cout << "options.use_predicate_pushdown = true;" << std::endl;
    std::cout << "// " << std::endl;
    std::cout << "// Execute query with pushdown and projection" << std::endl;
    std::cout << "auto query = QueryFrom(db)" << std::endl;
    std::cout << "    .Where(\"id\", PredicateType::kGreaterThan, arrow::MakeScalar<int64_t>(50))" << std::endl;
    std::cout << "    .Where(\"id\", PredicateType::kLessThan, arrow::MakeScalar<int64_t>(200))" << std::endl;
    std::cout << "    .Select({\"id\", \"name\"})" << std::endl;
    std::cout << "    .Limit(100);" << std::endl;
    std::cout << "// " << std::endl;
    std::cout << "std::unique_ptr<QueryResult> result;" << std::endl;
    std::cout << "query->Execute(&result);" << std::endl;
    std::cout << "// " << std::endl;
    std::cout << "// Stream results with only projected columns" << std::endl;
    std::cout << "while (result->HasNext()) {" << std::endl;
    std::cout << "    std::shared_ptr<arrow::RecordBatch> batch;" << std::endl;
    std::cout << "    result->Next(&batch);" << std::endl;
    std::cout << "    // batch contains only 'id' and 'name' columns" << std::endl;
    std::cout << "    // predicates were pushed down to reduce I/O" << std::endl;
    std::cout << "}" << std::endl;
    std::cout << "```" << std::endl;

    // Example 6: Memory Efficiency
    std::cout << "\n=== Example 6: Memory Efficiency ===" << std::endl;
    std::cout << "Process billions of rows without loading everything:" << std::endl;
    std::cout << "```cpp" << std::endl;
    std::cout << "// QueryOptions options;" << std::endl;
    std::cout << "// options.batch_size = 1000;  // Process 1000 rows at a time" << std::endl;
    std::cout << "// options.limit = 10000000;  // But limit total to 10M rows" << std::endl;
    std::cout << "// " << std::endl;
    std::cout << "// // Memory usage stays constant regardless of dataset size" << std::endl;
    std::cout << "// // Perfect for streaming analytics and ETL pipelines" << std::endl;
    std::cout << "```" << std::endl;

    // Example 6: Reverse Scan Support
    std::cout << "\n=== Example 6: Reverse Scan Support ===" << std::endl;
    std::cout << "Scan data in descending order for time-series or ranking queries:" << std::endl;
    std::cout << "```cpp" << std::endl;
    std::cout << "// QueryOptions options;" << std::endl;
    std::cout << "// options.reverse_order = true; // Enable reverse/descending order" << std::endl;
    std::cout << "// options.limit = 100;" << std::endl;
    std::cout << "// " << std::endl;
    std::cout << "// auto query = QueryFrom(db)" << std::endl;
    std::cout << "//     .Select({\"timestamp\", \"user_id\", \"score\"})" << std::endl;
    std::cout << "//     .Where(\"timestamp\", PredicateType::kGreaterThan, arrow::MakeScalar(timestamp_min))" << std::endl;
    std::cout << "//     .Reverse(); // Alternative fluent API for reverse order" << std::endl;
    std::cout << "// " << std::endl;
    std::cout << "// std::unique_ptr<QueryResult> result;" << std::endl;
    std::cout << "// query->Execute(&result);" << std::endl;
    std::cout << "// " << std::endl;
    std::cout << "// // Process results in reverse chronological order" << std::endl;
    std::cout << "// while (result->HasNext()) {" << std::endl;
    std::cout << "//     std::shared_ptr<arrow::RecordBatch> batch;" << std::endl;
    std::cout << "//     result->Next(&batch);" << std::endl;
    std::cout << "//     // Process most recent records first" << std::endl;
    std::cout << "// }" << std::endl;
    std::cout << "```" << std::endl;
    std::cout << "Use cases: Recent activity feeds, leaderboards, time-series analysis." << std::endl;

    std::cout << "\n=== Key Features Implemented ===" << std::endl;
    std::cout << "✓ Arrow RecordBatch streaming" << std::endl;
    std::cout << "✓ Synchronous and asynchronous processing" << std::endl;
    std::cout << "✓ Predicate pushdown to key ranges" << std::endl;
    std::cout << "✓ Column projection (SELECT specific columns)" << std::endl;
    std::cout << "✓ Reverse scan support (descending order)" << std::endl;
    std::cout << "✓ Arrow Dataset-compatible API" << std::endl;
    std::cout << "✓ Memory-efficient batch processing" << std::endl;
    std::cout << "✓ Fluent query builder interface" << std::endl;
    std::cout << "✓ Configurable batch sizes and limits" << std::endl;
    std::cout << "✓ Query optimization with pushdown" << std::endl;
    std::cout << "✓ Integration with LSM tree scanning" << std::endl;

    std::cout << "\n=== Architecture Benefits ===" << std::endl;
    std::cout << "• Storage Layer: MarbleDB handles LSM tree efficiency" << std::endl;
    std::cout << "• Query Layer: Arrow-compatible streaming interface" << std::endl;
    std::cout << "• Integration: Seamless with existing Arrow ecosystem" << std::endl;
    std::cout << "• Performance: Predicate pushdown reduces I/O" << std::endl;
    std::cout << "• Scalability: Handle datasets larger than memory" << std::endl;

    std::cout << "\nStreaming query API demonstration completed!" << std::endl;
    std::cout << "This API enables MarbleDB to serve as a high-performance" << std::endl;
    std::cout << "storage layer that streams Arrow data for analytics workloads." << std::endl;

    return 0;
}
