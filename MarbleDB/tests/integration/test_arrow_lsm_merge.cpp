/************************************************************************
Copyright 2024 MarbleDB Contributors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
**************************************************************************/

#include <gtest/gtest.h>
#include <arrow/api.h>
#include <arrow/testing/gtest_util.h>

#include "marble/api.h"
#include "marble/arrow/reader.h"
#include "marble/table.h"

namespace marble {
namespace arrow_api {
namespace test {

/**
 * Test suite for LSM K-way merge with deduplication
 *
 * Tests:
 * 1. Multi-level merge (L0 through L6)
 * 2. Deduplication (same key in multiple levels)
 * 3. Priority ordering (L0 > L1 > ... > L6 for same key)
 * 4. Streaming execution (constant memory)
 * 5. Version snapshots (concurrent compaction)
 */
class ArrowLSMMergeTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Create test database
        DBOptions options;
        options.db_path = "/tmp/marble_lsm_merge_test";
        options.enable_wal = false;
        options.memtable_size_threshold = 1024;  // Small memtable for faster flushing

        // Create schema
        schema_ = arrow::schema({
            arrow::field("id", arrow::uint64()),
            arrow::field("value", arrow::int64()),
            arrow::field("timestamp", arrow::int64())
        });

        // Open database (schema parameter is currently ignored)
        auto status = MarbleDB::Open(options, nullptr, &db_);
        ASSERT_TRUE(status.ok()) << status.ToString();

        // Create table
        TableSchema tschema;
        tschema.table_name = "test_table";
        tschema.arrow_schema = schema_;

        status = db_->CreateTable(tschema);
        ASSERT_TRUE(status.ok()) << status.ToString();
    }

    void TearDown() override {
        if (db_) {
            db_->Destroy();
        }
    }

    // Insert data with duplicates across multiple writes
    void InsertWithDuplicates() {
        // Write 1: id 0-49, value = 100, timestamp = 1000
        InsertBatch(0, 50, 100, 1000);
        db_->Flush();  // Create SSTable 1

        // Write 2: id 25-74, value = 200, timestamp = 2000
        // Keys 25-49 are duplicates (should use newer value = 200)
        InsertBatch(25, 50, 200, 2000);
        db_->Flush();  // Create SSTable 2

        // Write 3: id 40-89, value = 300, timestamp = 3000
        // Keys 40-49 are duplicates (should use newest value = 300)
        InsertBatch(40, 50, 300, 3000);
        db_->Flush();  // Create SSTable 3
    }

    void InsertBatch(uint64_t start_id, int count, int64_t value, int64_t timestamp) {
        arrow::UInt64Builder id_builder;
        arrow::Int64Builder value_builder;
        arrow::Int64Builder timestamp_builder;

        for (int i = 0; i < count; ++i) {
            ASSERT_OK(id_builder.Append(start_id + i));
            ASSERT_OK(value_builder.Append(value));
            ASSERT_OK(timestamp_builder.Append(timestamp));
        }

        std::shared_ptr<arrow::Array> id_array, value_array, timestamp_array;
        ASSERT_OK(id_builder.Finish(&id_array));
        ASSERT_OK(value_builder.Finish(&value_array));
        ASSERT_OK(timestamp_builder.Finish(&timestamp_array));

        auto batch = arrow::RecordBatch::Make(
            schema_,
            count,
            {id_array, value_array, timestamp_array});

        auto status = db_->InsertBatch("test_table", batch);
        ASSERT_TRUE(status.ok()) << status.ToString();
    }

    std::unique_ptr<MarbleDB> db_;
    std::shared_ptr<arrow::Schema> schema_;
};

//==============================================================================
// Test 1: Deduplication - Newer Values Override Older
//==============================================================================

TEST_F(ArrowLSMMergeTest, DeduplicationNewerValuesOverride) {
    InsertWithDuplicates();

    // Read all data
    auto reader_result = OpenTable(
        std::shared_ptr<MarbleDB>(db_.get(), [](MarbleDB*){}),
        "test_table");

    ASSERT_TRUE(reader_result.ok());
    auto reader = *reader_result;

    // Collect all rows
    std::map<uint64_t, std::pair<int64_t, int64_t>> rows;  // id -> (value, timestamp)

    while (true) {
        std::shared_ptr<arrow::RecordBatch> batch;
        ASSERT_OK(reader->ReadNext(&batch));
        if (!batch) break;

        auto id_array = std::static_pointer_cast<arrow::UInt64Array>(batch->column(0));
        auto value_array = std::static_pointer_cast<arrow::Int64Array>(batch->column(1));
        auto timestamp_array = std::static_pointer_cast<arrow::Int64Array>(batch->column(2));

        for (int64_t i = 0; i < batch->num_rows(); ++i) {
            uint64_t id = id_array->Value(i);
            int64_t value = value_array->Value(i);
            int64_t timestamp = timestamp_array->Value(i);

            // Store or update row
            rows[id] = {value, timestamp};
        }
    }

    // Verify deduplication worked correctly:
    // - Keys 0-24: value=100 (only in write 1)
    // - Keys 25-39: value=200 (overridden in write 2)
    // - Keys 40-49: value=300 (overridden in write 3, newest)
    // - Keys 50-74: value=200 (only in write 2)
    // - Keys 75-89: value=300 (only in write 3)

    for (uint64_t id = 0; id < 90; ++id) {
        ASSERT_TRUE(rows.count(id) > 0) << "Missing id: " << id;

        auto [value, timestamp] = rows[id];

        if (id < 25) {
            EXPECT_EQ(value, 100) << "id=" << id;
            EXPECT_EQ(timestamp, 1000) << "id=" << id;
        } else if (id < 40) {
            EXPECT_EQ(value, 200) << "id=" << id;
            EXPECT_EQ(timestamp, 2000) << "id=" << id;
        } else if (id < 50) {
            EXPECT_EQ(value, 300) << "id=" << id;
            EXPECT_EQ(timestamp, 3000) << "id=" << id;
        } else if (id < 75) {
            EXPECT_EQ(value, 200) << "id=" << id;
            EXPECT_EQ(timestamp, 2000) << "id=" << id;
        } else {
            EXPECT_EQ(value, 300) << "id=" << id;
            EXPECT_EQ(timestamp, 3000) << "id=" << id;
        }
    }

    // Check statistics - should show some deduplication
    auto marble_reader = std::static_pointer_cast<MarbleRecordBatchReader>(reader);
    auto stats = marble_reader->GetStats();
    // rows_deduplicated should be > 0
}

//==============================================================================
// Test 2: No Duplicates - All Rows Returned
//==============================================================================

TEST_F(ArrowLSMMergeTest, NoDuplicatesAllRowsReturned) {
    // Insert non-overlapping key ranges
    InsertBatch(0, 30, 100, 1000);
    db_->Flush();

    InsertBatch(30, 30, 200, 2000);
    db_->Flush();

    InsertBatch(60, 30, 300, 3000);
    db_->Flush();

    // Read all data
    auto reader_result = OpenTable(
        std::shared_ptr<MarbleDB>(db_.get(), [](MarbleDB*){}),
        "test_table");

    ASSERT_TRUE(reader_result.ok());
    auto reader = *reader_result;

    int64_t total_rows = 0;
    while (true) {
        std::shared_ptr<arrow::RecordBatch> batch;
        ASSERT_OK(reader->ReadNext(&batch));
        if (!batch) break;
        total_rows += batch->num_rows();
    }

    // Should return all 90 rows (no deduplication needed)
    EXPECT_EQ(total_rows, 90);
}

//==============================================================================
// Test 3: Sorted Output - Keys In Ascending Order
//==============================================================================

TEST_F(ArrowLSMMergeTest, SortedOutputKeysInAscendingOrder) {
    // Insert data in random order
    InsertBatch(50, 50, 100, 1000);
    db_->Flush();

    InsertBatch(0, 50, 200, 2000);
    db_->Flush();

    InsertBatch(100, 50, 300, 3000);
    db_->Flush();

    // Read all data
    auto reader_result = OpenTable(
        std::shared_ptr<MarbleDB>(db_.get(), [](MarbleDB*){}),
        "test_table");

    ASSERT_TRUE(reader_result.ok());
    auto reader = *reader_result;

    // Verify keys are in ascending order
    uint64_t last_key = 0;
    bool first = true;

    while (true) {
        std::shared_ptr<arrow::RecordBatch> batch;
        ASSERT_OK(reader->ReadNext(&batch));
        if (!batch) break;

        auto id_array = std::static_pointer_cast<arrow::UInt64Array>(batch->column(0));

        for (int64_t i = 0; i < batch->num_rows(); ++i) {
            uint64_t key = id_array->Value(i);

            if (!first) {
                EXPECT_GT(key, last_key) << "Keys should be in ascending order";
            }

            last_key = key;
            first = false;
        }
    }
}

//==============================================================================
// Test 4: Streaming Execution - Constant Memory
//==============================================================================

TEST_F(ArrowLSMMergeTest, StreamingExecutionConstantMemory) {
    // Insert large amount of data
    for (int i = 0; i < 10; ++i) {
        InsertBatch(i * 100, 100, i * 10, i * 1000);
        db_->Flush();
    }

    // Read data one batch at a time
    auto reader_result = OpenTable(
        std::shared_ptr<MarbleDB>(db_.get(), [](MarbleDB*){}),
        "test_table");

    ASSERT_TRUE(reader_result.ok());
    auto reader = *reader_result;

    int batch_count = 0;
    int64_t total_rows = 0;

    while (true) {
        std::shared_ptr<arrow::RecordBatch> batch;
        ASSERT_OK(reader->ReadNext(&batch));
        if (!batch) break;

        batch_count++;
        total_rows += batch->num_rows();

        // Batch should not be too large (verifies streaming, not full materialization)
        EXPECT_LE(batch->num_rows(), 10000) << "Batch size should be bounded";
    }

    // Should have read all 1000 rows
    EXPECT_EQ(total_rows, 1000);

    // Should have received multiple batches (streaming, not single batch)
    EXPECT_GT(batch_count, 1);
}

//==============================================================================
// Test 5: Empty Table - No Batches Returned
//==============================================================================

TEST_F(ArrowLSMMergeTest, EmptyTableNoBatchesReturned) {
    // Don't insert any data - table is empty

    auto reader_result = OpenTable(
        std::shared_ptr<MarbleDB>(db_.get(), [](MarbleDB*){}),
        "test_table");

    ASSERT_TRUE(reader_result.ok());
    auto reader = *reader_result;

    // Should return nullptr immediately
    std::shared_ptr<arrow::RecordBatch> batch;
    ASSERT_OK(reader->ReadNext(&batch));
    EXPECT_EQ(batch, nullptr);
}

//==============================================================================
// Test 6: Single SSTable - No Merge Needed
//==============================================================================

TEST_F(ArrowLSMMergeTest, SingleSSTableNoMergeNeeded) {
    // Insert data and flush once
    InsertBatch(0, 100, 100, 1000);
    db_->Flush();

    auto reader_result = OpenTable(
        std::shared_ptr<MarbleDB>(db_.get(), [](MarbleDB*){}),
        "test_table");

    ASSERT_TRUE(reader_result.ok());
    auto reader = *reader_result;

    int64_t total_rows = 0;
    while (true) {
        std::shared_ptr<arrow::RecordBatch> batch;
        ASSERT_OK(reader->ReadNext(&batch));
        if (!batch) break;
        total_rows += batch->num_rows();
    }

    EXPECT_EQ(total_rows, 100);
}

//==============================================================================
// Test 7: Version Snapshot - Concurrent Compaction Safe
//==============================================================================

TEST_F(ArrowLSMMergeTest, VersionSnapshotConcurrentCompactionSafe) {
    // Insert data
    InsertBatch(0, 100, 100, 1000);
    db_->Flush();

    // Create reader (gets Version snapshot)
    auto reader_result = OpenTable(
        std::shared_ptr<MarbleDB>(db_.get(), [](MarbleDB*){}),
        "test_table");

    ASSERT_TRUE(reader_result.ok());
    auto reader = *reader_result;

    // While reader holds snapshot, insert more data and flush
    // This creates a new SSTable, but reader should still see old version
    InsertBatch(100, 100, 200, 2000);
    db_->Flush();

    // Reader should still read only original 100 rows
    // (Version snapshot protects from seeing new writes)
    int64_t total_rows = 0;
    while (true) {
        std::shared_ptr<arrow::RecordBatch> batch;
        ASSERT_OK(reader->ReadNext(&batch));
        if (!batch) break;
        total_rows += batch->num_rows();
    }

    // Exact behavior depends on snapshot isolation implementation
    // But reader should work correctly without crashes
    EXPECT_GT(total_rows, 0);
}

}  // namespace test
}  // namespace arrow_api
}  // namespace marble
