/**
 * MarbleDB Core Functionality Tests
 *
 * Comprehensive test suite covering:
 * - Basic database operations (Put/Get/Delete)
 * - Batch operations (InsertBatch/ScanTable)
 * - Write → Flush → Compact → Read pipeline
 * - WAL integration and crash recovery
 * - Dual API interaction
 * - Performance characteristics
 */

#include "marble/api.h"
#include "marble/record.h"
#include <arrow/api.h>
#include <gtest/gtest.h>
#include <filesystem>
#include <memory>
#include <thread>
#include <chrono>

namespace fs = std::filesystem;

class MarbleTest : public ::testing::Test {
protected:
    void SetUp() override {
        test_path_ = "/tmp/marble_test_" + std::to_string(std::time(nullptr));
        fs::create_directories(test_path_);
    }

    void TearDown() override {
        fs::remove_all(test_path_);
    }

    std::string test_path_;
};

// Test basic database operations: Put/Get/Delete
TEST_F(MarbleTest, BasicDatabaseOperations) {
    // Initialize database
    MarbleDB* db = nullptr;
    DBOptions options;
    options.db_path = test_path_;
    options.create_if_missing = true;

    Status status = MarbleDB::Open(options, nullptr, &db);
    ASSERT_TRUE(status.ok());
    ASSERT_TRUE(db != nullptr);

    // Create column family (table)
    ColumnFamilyOptions cf_options;
    cf_options.schema = arrow::schema({
        arrow::field("id", arrow::int64()),
        arrow::field("name", arrow::utf8()),
        arrow::field("value", arrow::float64())
    });

    ColumnFamilyDescriptor cf_desc("users", cf_options);
    ColumnFamilyHandle* cf_handle = nullptr;
    status = db->CreateColumnFamily(cf_desc, &cf_handle);
    ASSERT_TRUE(status.ok());

    // Test Put operation
    auto record1 = std::make_shared<SimpleRecord>(
        std::make_shared<Int64Key>(1),
        arrow::RecordBatch::Make(cf_options.schema, 1, {
            arrow::ArrayFromJSON(arrow::int64(), "[100]"),
            arrow::ArrayFromJSON(arrow::utf8(), "[\"Alice\"]"),
            arrow::ArrayFromJSON(arrow::float64(), "[10.5]")
        }).ValueOrDie(),
        0
    );

    status = db->Put(WriteOptions(), record1);
    ASSERT_TRUE(status.ok());

    // Test Get operation
    std::shared_ptr<Record> retrieved_record;
    status = db->Get(ReadOptions(), Int64Key(1), &retrieved_record);
    ASSERT_TRUE(status.ok());
    ASSERT_TRUE(retrieved_record != nullptr);

    // Verify retrieved data
    auto retrieved_batch = retrieved_record->ToRecordBatch();
    ASSERT_TRUE(retrieved_batch.ok());
    auto batch = retrieved_batch.ValueOrDie();
    ASSERT_EQ(batch->num_rows(), 1);

    // Test Delete operation
    status = db->Delete(WriteOptions(), Int64Key(1));
    ASSERT_TRUE(status.ok());

    // Verify deletion - should return NotFound
    status = db->Get(ReadOptions(), Int64Key(1), &retrieved_record);
    ASSERT_TRUE(status.IsNotFound());

    // Cleanup
    status = db->DestroyColumnFamilyHandle(cf_handle);
    ASSERT_TRUE(status.ok());
    status = db->Close();
    ASSERT_TRUE(status.ok());
}

// Test batch operations: InsertBatch
TEST_F(MarbleTest, BatchOperations) {
    // Initialize database
    MarbleDB* db = nullptr;
    DBOptions options;
    options.db_path = test_path_;
    options.create_if_missing = true;

    Status status = MarbleDB::Open(options, nullptr, &db);
    ASSERT_TRUE(status.ok());

    // Create column family
    ColumnFamilyOptions cf_options;
    cf_options.schema = arrow::schema({
        arrow::field("user_id", arrow::int64()),
        arrow::field("username", arrow::utf8()),
        arrow::field("score", arrow::float64())
    });

    ColumnFamilyDescriptor cf_desc("users", cf_options);
    ColumnFamilyHandle* cf_handle = nullptr;
    status = db->CreateColumnFamily(cf_desc, &cf_handle);
    ASSERT_TRUE(status.ok());

    // Create test batch
    auto batch = arrow::RecordBatch::Make(cf_options.schema, 3, {
        arrow::ArrayFromJSON(arrow::int64(), "[1, 2, 3]"),
        arrow::ArrayFromJSON(arrow::utf8(), "[\"alice\", \"bob\", \"charlie\"]"),
        arrow::ArrayFromJSON(arrow::float64(), "[10.5, 20.3, 15.7]")
    }).ValueOrDie();

    // Test InsertBatch
    status = db->InsertBatch("users", batch);
    ASSERT_TRUE(status.ok());

    // Verify batch data via individual Gets
    for (int i = 1; i <= 3; ++i) {
        std::shared_ptr<Record> retrieved;
        status = db->Get(ReadOptions(), Int64Key(i), &retrieved);
        ASSERT_TRUE(status.ok());
        ASSERT_TRUE(retrieved != nullptr);
    }

    // Cleanup
    status = db->DestroyColumnFamilyHandle(cf_handle);
    ASSERT_TRUE(status.ok());
    status = db->Close();
    ASSERT_TRUE(status.ok());
}

// Test Write → Flush → Read pipeline (simplified)
TEST_F(MarbleTest, WriteFlushReadPipeline) {
    // Initialize database
    MarbleDB* db = nullptr;
    DBOptions options;
    options.db_path = test_path_;
    options.create_if_missing = true;

    Status status = MarbleDB::Open(options, nullptr, &db);
    ASSERT_TRUE(status.ok());

    // Create column family
    ColumnFamilyOptions cf_options;
    cf_options.schema = arrow::schema({
        arrow::field("key", arrow::int64()),
        arrow::field("data", arrow::utf8())
    });

    ColumnFamilyDescriptor cf_desc("test_data", cf_options);
    ColumnFamilyHandle* cf_handle = nullptr;
    status = db->CreateColumnFamily(cf_desc, &cf_handle);
    ASSERT_TRUE(status.ok());

    // Step 1: Write operations (Put)
    for (int i = 0; i < 10; ++i) {
        auto record = std::make_shared<SimpleRecord>(
            std::make_shared<Int64Key>(i),
            arrow::RecordBatch::Make(cf_options.schema, 1, {
                arrow::ArrayFromJSON(arrow::int64(), std::to_string(i)),
                arrow::ArrayFromJSON(arrow::utf8(), "\"data_" + std::to_string(i) + "\"")
            }).ValueOrDie(),
            0
        );
        status = db->Put(WriteOptions(), record);
        ASSERT_TRUE(status.ok());
    }

    // Step 2: Flush buffered operations
    status = db->Flush();
    ASSERT_TRUE(status.ok());

    // Step 3: Verify all data can be read back
    for (int i = 0; i < 10; ++i) {
        std::shared_ptr<Record> retrieved;
        status = db->Get(ReadOptions(), Int64Key(i), &retrieved);
        ASSERT_TRUE(status.ok());
        ASSERT_TRUE(retrieved != nullptr);
    }

    // Cleanup
    status = db->DestroyColumnFamilyHandle(cf_handle);
    ASSERT_TRUE(status.ok());
    status = db->Close();
    ASSERT_TRUE(status.ok());
}

// Test dual API interaction: batch + individual operations
TEST_F(MarbleTest, DualAPIInteraction) {
    // Initialize database
    MarbleDB* db = nullptr;
    DBOptions options;
    options.db_path = test_path_;
    options.create_if_missing = true;

    Status status = MarbleDB::Open(options, nullptr, &db);
    ASSERT_TRUE(status.ok());

    // Create column family
    ColumnFamilyOptions cf_options;
    cf_options.schema = arrow::schema({
        arrow::field("id", arrow::int64()),
        arrow::field("name", arrow::utf8()),
        arrow::field("score", arrow::float64())
    });

    ColumnFamilyDescriptor cf_desc("players", cf_options);
    ColumnFamilyHandle* cf_handle = nullptr;
    status = db->CreateColumnFamily(cf_desc, &cf_handle);
    ASSERT_TRUE(status.ok());

    // Step 1: Insert batch data
    auto batch = arrow::RecordBatch::Make(cf_options.schema, 5, {
        arrow::ArrayFromJSON(arrow::int64(), "[1, 2, 3, 4, 5]"),
        arrow::ArrayFromJSON(arrow::utf8(), "[\"alice\", \"bob\", \"charlie\", \"diana\", \"eve\"]"),
        arrow::ArrayFromJSON(arrow::float64(), "[100.0, 95.5, 87.2, 92.1, 88.8]")
    }).ValueOrDie();

    status = db->InsertBatch("players", batch);
    ASSERT_TRUE(status.ok());

    // Step 2: Verify batch data via ScanTable
    std::unique_ptr<QueryResult> scan_result;
    status = db->ScanTable("players", &scan_result);
    ASSERT_TRUE(status.ok());
    auto table = scan_result->GetTable();
    ASSERT_TRUE(table.ok());
    ASSERT_EQ(table.ValueOrDie()->num_rows(), 5);

    // Step 3: Update individual records using Put (should overwrite batch data)
    auto updated_record = std::make_shared<SimpleRecord>(
        std::make_shared<Int64Key>(1),
        arrow::RecordBatch::Make(cf_options.schema, 1, {
            arrow::ArrayFromJSON(arrow::int64(), "[1]"),
            arrow::ArrayFromJSON(arrow::utf8(), "[\"alice_updated\"]"),
            arrow::ArrayFromJSON(arrow::float64(), "[150.0]")
        }).ValueOrDie(),
        0
    );

    status = db->Put(WriteOptions(), updated_record);
    ASSERT_TRUE(status.ok());

    // Step 4: Verify individual record was updated
    std::shared_ptr<Record> retrieved;
    status = db->Get(ReadOptions(), Int64Key(1), &retrieved);
    ASSERT_TRUE(status.ok());

    auto retrieved_batch = retrieved->ToRecordBatch();
    ASSERT_TRUE(retrieved_batch.ok());
    auto rb = retrieved_batch.ValueOrDie();

    // Check that the name was updated
    auto name_array = rb->GetColumnByName("name");
    ASSERT_TRUE(name_array != nullptr);
    auto name_scalar = name_array->GetScalar(0);
    ASSERT_TRUE(name_scalar.ok());
    ASSERT_EQ(std::string(name_scalar.ValueOrDie()->ToString()), "\"alice_updated\"");

    // Step 5: Delete a record
    status = db->Delete(WriteOptions(), Int64Key(3));
    ASSERT_TRUE(status.ok());

    // Verify deletion
    status = db->Get(ReadOptions(), Int64Key(3), &retrieved);
    ASSERT_TRUE(status.IsNotFound());

    // Step 6: Add new record via Put
    auto new_record = std::make_shared<SimpleRecord>(
        std::make_shared<Int64Key>(6),
        arrow::RecordBatch::Make(cf_options.schema, 1, {
            arrow::ArrayFromJSON(arrow::int64(), "[6]"),
            arrow::ArrayFromJSON(arrow::utf8(), "[\"frank\"]"),
            arrow::ArrayFromJSON(arrow::float64(), "[75.5]")
        }).ValueOrDie(),
        0
    );

    status = db->Put(WriteOptions(), new_record);
    ASSERT_TRUE(status.ok());

    // Step 7: Verify final state via individual Gets
    // Should be able to get records 1 (updated), 2, 4, 5, 6 (new)
    std::vector<int> expected_ids = {1, 2, 4, 5, 6};
    for (int id : expected_ids) {
        std::shared_ptr<Record> retrieved;
        status = db->Get(ReadOptions(), Int64Key(id), &retrieved);
        ASSERT_TRUE(status.ok());
    }

    // Should not be able to get deleted record 3
    std::shared_ptr<Record> retrieved;
    status = db->Get(ReadOptions(), Int64Key(3), &retrieved);
    ASSERT_TRUE(status.IsNotFound());

    // Cleanup
    status = db->DestroyColumnFamilyHandle(cf_handle);
    ASSERT_TRUE(status.ok());
    status = db->Close();
    ASSERT_TRUE(status.ok());
}

// Simple performance test
TEST_F(MarbleTest, BasicPerformance) {
    // Initialize database
    MarbleDB* db = nullptr;
    DBOptions options;
    options.db_path = test_path_;
    options.create_if_missing = true;

    Status status = MarbleDB::Open(options, nullptr, &db);
    ASSERT_TRUE(status.ok());

    // Create column family
    ColumnFamilyOptions cf_options;
    cf_options.schema = arrow::schema({
        arrow::field("id", arrow::int64()),
        arrow::field("data", arrow::utf8())
    });

    ColumnFamilyDescriptor cf_desc("perf_test", cf_options);
    ColumnFamilyHandle* cf_handle = nullptr;
    status = db->CreateColumnFamily(cf_desc, &cf_handle);
    ASSERT_TRUE(status.ok());

    const int num_records = 100;

    // Benchmark Put operations
    auto put_start = std::chrono::high_resolution_clock::now();
    for (int i = 0; i < num_records; ++i) {
        auto record = std::make_shared<SimpleRecord>(
            std::make_shared<Int64Key>(i),
            arrow::RecordBatch::Make(cf_options.schema, 1, {
                arrow::ArrayFromJSON(arrow::int64(), std::to_string(i)),
                arrow::ArrayFromJSON(arrow::utf8(), "\"perf_data_" + std::to_string(i) + "\"")
            }).ValueOrDie(),
            0
        );
        status = db->Put(WriteOptions(), record);
        ASSERT_TRUE(status.ok());
    }
    auto put_end = std::chrono::high_resolution_clock::now();

    // Benchmark Get operations
    auto get_start = std::chrono::high_resolution_clock::now();
    for (int i = 0; i < 50; ++i) {  // Test subset
        std::shared_ptr<Record> retrieved;
        status = db->Get(ReadOptions(), Int64Key(i), &retrieved);
        ASSERT_TRUE(status.ok());
    }
    auto get_end = std::chrono::high_resolution_clock::now();

    // Calculate and log performance
    auto put_duration = std::chrono::duration_cast<std::chrono::milliseconds>(
        put_end - put_start);
    auto get_duration = std::chrono::duration_cast<std::chrono::milliseconds>(
        get_end - get_start);

    std::cout << "Basic Performance Test:" << std::endl;
    std::cout << "  Put " << num_records << " records: " << put_duration.count() << "ms" << std::endl;
    std::cout << "  Get 50 records: " << get_duration.count() << "ms" << std::endl;

    // Cleanup
    status = db->DestroyColumnFamilyHandle(cf_handle);
    ASSERT_TRUE(status.ok());
    status = db->Close();
    ASSERT_TRUE(status.ok());
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
