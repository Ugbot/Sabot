#include "marble/api.h"
#include "marble/record.h"
#include <iostream>

int main() {
    std::cout << "Testing MarbleDB basic operations..." << std::endl;
    
    // Initialize database
    MarbleDB* db = nullptr;
    DBOptions options;
    options.db_path = "/tmp/marble_test";
    options.create_if_missing = true;

    Status status = MarbleDB::Open(options, nullptr, &db);
    if (!status.ok()) {
        std::cerr << "Failed to open database: " << status.ToString() << std::endl;
        return 1;
    }
    std::cout << "Database opened successfully!" << std::endl;

    // Create column family
    ColumnFamilyOptions cf_options;
    cf_options.schema = arrow::schema({
        arrow::field("id", arrow::int64()),
        arrow::field("name", arrow::utf8()),
        arrow::field("value", arrow::float64())
    });

    ColumnFamilyDescriptor cf_desc("users", cf_options);
    ColumnFamilyHandle* cf_handle = nullptr;
    status = db->CreateColumnFamily(cf_desc, &cf_handle);
    if (!status.ok()) {
        std::cerr << "Failed to create column family: " << status.ToString() << std::endl;
        return 1;
    }
    std::cout << "Column family created successfully!" << std::endl;

    // Test Put operation
    auto record = std::make_shared<SimpleRecord>(
        std::make_shared<Int64Key>(1),
        arrow::RecordBatch::Make(cf_options.schema, 1, {
            arrow::ArrayFromJSON(arrow::int64(), "[100]"),
            arrow::ArrayFromJSON(arrow::utf8(), "[\"Alice\"]"),
            arrow::ArrayFromJSON(arrow::float64(), "[10.5]")
        }).ValueOrDie(),
        0
    );

    status = db->Put(WriteOptions(), record);
    if (!status.ok()) {
        std::cerr << "Failed to Put record: " << status.ToString() << std::endl;
        return 1;
    }
    std::cout << "Put operation successful!" << std::endl;

    // Test Get operation
    std::shared_ptr<Record> retrieved_record;
    status = db->Get(ReadOptions(), Int64Key(1), &retrieved_record);
    if (!status.ok()) {
        std::cerr << "Failed to Get record: " << status.ToString() << std::endl;
        return 1;
    }
    std::cout << "Get operation successful!" << std::endl;

    // Test Delete operation
    status = db->Delete(WriteOptions(), Int64Key(1));
    if (!status.ok()) {
        std::cerr << "Failed to Delete record: " << status.ToString() << std::endl;
        return 1;
    }
    std::cout << "Delete operation successful!" << std::endl;

    // Verify deletion
    status = db->Get(ReadOptions(), Int64Key(1), &retrieved_record);
    if (!status.IsNotFound()) {
        std::cerr << "Record should have been deleted!" << std::endl;
        return 1;
    }
    std::cout << "Record deletion verified!" << std::endl;

    // Cleanup
    status = db->DestroyColumnFamilyHandle(cf_handle);
    status = db->Close();

    std::cout << "All tests passed! MarbleDB is working!" << std::endl;
    return 0;
}
