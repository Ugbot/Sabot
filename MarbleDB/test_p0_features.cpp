#include <marble/db.h>
#include <arrow/api.h>
#include <iostream>
#include <memory>

int main() {
    std::cout << "Testing MarbleDB P0 Features" << std::endl;

    // Open database
    marble::DBOptions options;
    options.db_path = "/tmp/marble_test";
    std::unique_ptr<marble::MarbleDB> db;
    auto status = marble::MarbleDB::Open(options, nullptr, &db);

    if (!status.ok()) {
        std::cerr << "Failed to open database: " << status.ToString() << std::endl;
        return 1;
    }

    std::cout << "✓ Database opened successfully" << std::endl;

    // Create SPO column family (like SabotQL requirements)
    auto spo_schema = arrow::schema({
        arrow::field("subject", arrow::int64()),
        arrow::field("predicate", arrow::int64()),
        arrow::field("object", arrow::int64())
    });

    marble::ColumnFamilyOptions spo_options;
    spo_options.schema = spo_schema;
    marble::ColumnFamilyDescriptor spo_descriptor("SPO", spo_options);

    marble::ColumnFamilyHandle* spo_handle = nullptr;
    status = db->CreateColumnFamily(spo_descriptor, &spo_handle);

    if (!status.ok()) {
        std::cerr << "Failed to create SPO column family: " << status.ToString() << std::endl;
        return 1;
    }

    std::cout << "✓ SPO column family created successfully" << std::endl;

    // Create some test triples (Alice knows Bob, Alice knows Carol)
    arrow::Int64Builder s_builder, p_builder, o_builder;

    // Triple 1: (42, 100, 99) - Alice(42) knows(100) Bob(99)
    s_builder.Append(42);
    p_builder.Append(100);
    o_builder.Append(99);

    // Triple 2: (42, 100, 98) - Alice(42) knows(100) Carol(98)
    s_builder.Append(42);
    p_builder.Append(100);
    o_builder.Append(98);

    std::shared_ptr<arrow::Array> s_array, p_array, o_array;
    s_builder.Finish(&s_array);
    p_builder.Finish(&p_array);
    o_builder.Finish(&o_array);

    auto batch = arrow::RecordBatch::Make(spo_schema, 2, {s_array, p_array, o_array});

    // Insert batch
    status = db->InsertBatch("SPO", batch);

    if (!status.ok()) {
        std::cerr << "Failed to insert batch: " << status.ToString() << std::endl;
        return 1;
    }

    std::cout << "✓ Inserted 2 triples successfully" << std::endl;

    // Scan table
    std::unique_ptr<marble::QueryResult> result;
    status = db->ScanTable("SPO", &result);

    if (!status.ok()) {
        std::cerr << "Failed to scan table: " << status.ToString() << std::endl;
        return 1;
    }

    std::cout << "✓ Scan table completed successfully" << std::endl;

    // Read results
    int64_t total_rows = 0;
    while (result->HasNext()) {
        std::shared_ptr<arrow::RecordBatch> batch_result;
        status = result->Next(&batch_result);
        if (!status.ok()) {
            std::cerr << "Failed to get next batch: " << status.ToString() << std::endl;
            return 1;
        }

        total_rows += batch_result->num_rows();
        std::cout << "  Batch with " << batch_result->num_rows() << " rows" << std::endl;

        // Print the triples
        auto s_col = std::static_pointer_cast<arrow::Int64Array>(batch_result->column(0));
        auto p_col = std::static_pointer_cast<arrow::Int64Array>(batch_result->column(1));
        auto o_col = std::static_pointer_cast<arrow::Int64Array>(batch_result->column(2));

        for (int64_t i = 0; i < batch_result->num_rows(); ++i) {
            std::cout << "    (" << s_col->Value(i) << ", "
                      << p_col->Value(i) << ", "
                      << o_col->Value(i) << ")" << std::endl;
        }
    }

    std::cout << "✓ Retrieved " << total_rows << " rows total" << std::endl;

    // Check column families list
    auto cf_list = db->ListColumnFamilies();
    std::cout << "✓ Column families: ";
    for (const auto& cf : cf_list) {
        std::cout << cf << " ";
    }
    std::cout << std::endl;

    std::cout << "🎉 All P0 features working correctly!" << std::endl;
    return 0;
}
