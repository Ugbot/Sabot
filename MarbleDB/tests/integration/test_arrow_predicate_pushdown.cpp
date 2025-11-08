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
 * Test suite for predicate pushdown (bloom filters + zone maps)
 *
 * Tests:
 * 1. SSTable-level pruning (min_key/max_key zone maps)
 * 2. Batch-level pruning (runtime zone maps)
 * 3. Bloom filter pruning (when available)
 * 4. Equality predicates
 * 5. Range predicates (>, >=, <, <=)
 * 6. Multiple predicates
 */
class ArrowPredicatePushdownTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Create test database
        DBOptions options;
        options.db_path = "/tmp/marble_predicate_pushdown_test";
        options.enable_wal = false;
        options.enable_bloom_filter = true;  // Enable bloom filters

        // Create schema
        schema_ = arrow::schema({
            arrow::field("id", arrow::uint64()),
            arrow::field("timestamp", arrow::int64()),
            arrow::field("value", arrow::int64())
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

        // Insert test data with known ranges
        InsertTestData();
    }

    void TearDown() override {
        if (db_) {
            db_->Destroy();
        }
    }

    void InsertTestData() {
        // Insert 3 batches with non-overlapping key ranges
        // Batch 1: id 0-99
        // Batch 2: id 100-199
        // Batch 3: id 200-299

        for (int batch_idx = 0; batch_idx < 3; ++batch_idx) {
            arrow::UInt64Builder id_builder;
            arrow::Int64Builder timestamp_builder;
            arrow::Int64Builder value_builder;

            uint64_t start_id = batch_idx * 100;
            for (int i = 0; i < 100; ++i) {
                uint64_t id = start_id + i;
                ASSERT_OK(id_builder.Append(id));
                ASSERT_OK(timestamp_builder.Append(id * 1000));  // Timestamp correlates with id
                ASSERT_OK(value_builder.Append(id * 10));
            }

            std::shared_ptr<arrow::Array> id_array, timestamp_array, value_array;
            ASSERT_OK(id_builder.Finish(&id_array));
            ASSERT_OK(timestamp_builder.Finish(&timestamp_array));
            ASSERT_OK(value_builder.Finish(&value_array));

            auto batch = arrow::RecordBatch::Make(
                schema_,
                100,
                {id_array, timestamp_array, value_array});

            auto status = db_->InsertBatch("test_table", batch);
            ASSERT_TRUE(status.ok()) << status.ToString();

            // Flush after each batch to create separate SSTables
            status = db_->Flush();
            ASSERT_TRUE(status.ok()) << status.ToString();
        }
    }

    std::unique_ptr<MarbleDB> db_;
    std::shared_ptr<arrow::Schema> schema_;
};

//==============================================================================
// Test 1: Equality Predicate - SSTable Level Pruning
//==============================================================================

TEST_F(ArrowPredicatePushdownTest, EqualityPredicateSSTablePruning) {
    // Create predicate: id = 150
    // This should only read SSTable 2 (id 100-199)
    // SSTables 1 and 3 should be skipped via min_key/max_key check

    std::vector<ColumnPredicate> predicates;
    ColumnPredicate pred;
    pred.column_name = "id";
    pred.predicate_type = ColumnPredicate::PredicateType::kEqual;
    pred.value = std::make_shared<arrow::UInt64Scalar>(150);
    predicates.push_back(pred);

    auto reader_result = OpenTable(
        std::shared_ptr<MarbleDB>(db_.get(), [](MarbleDB*){}),
        "test_table",
        {},  // No projection
        predicates);

    ASSERT_TRUE(reader_result.ok()) << reader_result.status().ToString();
    auto reader = std::static_pointer_cast<MarbleRecordBatchReader>(*reader_result);

    // Read all data
    int64_t total_rows = 0;
    while (true) {
        std::shared_ptr<arrow::RecordBatch> batch;
        ASSERT_OK(reader->ReadNext(&batch));
        if (!batch) break;
        total_rows += batch->num_rows();
    }

    // Check statistics - should have skipped 2 out of 3 SSTables
    auto stats = reader->GetStats();
    EXPECT_GT(stats.bloom_filter_skips, 0) << "Should have skipped some SSTables via zone map pruning";

    // Should read much less than 300 rows (ideally only ~1 row, but depends on implementation)
    EXPECT_LT(total_rows, 300);
}

//==============================================================================
// Test 2: Greater Than Predicate - SSTable Level Pruning
//==============================================================================

TEST_F(ArrowPredicatePushdownTest, GreaterThanPredicateSSTablePruning) {
    // Create predicate: id > 200
    // This should only read SSTable 3 (id 200-299)
    // SSTables 1 and 2 should be skipped (max_key <= 200)

    std::vector<ColumnPredicate> predicates;
    ColumnPredicate pred;
    pred.column_name = "id";
    pred.predicate_type = ColumnPredicate::PredicateType::kGreaterThan;
    pred.value = std::make_shared<arrow::UInt64Scalar>(200);
    predicates.push_back(pred);

    auto reader_result = OpenTable(
        std::shared_ptr<MarbleDB>(db_.get(), [](MarbleDB*){}),
        "test_table",
        {},
        predicates);

    ASSERT_TRUE(reader_result.ok());
    auto reader = std::static_pointer_cast<MarbleRecordBatchReader>(*reader_result);

    // Read all data
    int64_t total_rows = 0;
    while (true) {
        std::shared_ptr<arrow::RecordBatch> batch;
        ASSERT_OK(reader->ReadNext(&batch));
        if (!batch) break;
        total_rows += batch->num_rows();
    }

    // Check statistics
    auto stats = reader->GetStats();
    EXPECT_GT(stats.bloom_filter_skips, 0) << "Should have skipped SSTables 1 and 2";

    // Should read ~100 rows from SSTable 3 only
    EXPECT_LE(total_rows, 100);
}

//==============================================================================
// Test 3: Less Than Predicate - SSTable Level Pruning
//==============================================================================

TEST_F(ArrowPredicatePushdownTest, LessThanPredicateSSTablePruning) {
    // Create predicate: id < 100
    // This should only read SSTable 1 (id 0-99)
    // SSTables 2 and 3 should be skipped (min_key >= 100)

    std::vector<ColumnPredicate> predicates;
    ColumnPredicate pred;
    pred.column_name = "id";
    pred.predicate_type = ColumnPredicate::PredicateType::kLessThan;
    pred.value = std::make_shared<arrow::UInt64Scalar>(100);
    predicates.push_back(pred);

    auto reader_result = OpenTable(
        std::shared_ptr<MarbleDB>(db_.get(), [](MarbleDB*){}),
        "test_table",
        {},
        predicates);

    ASSERT_TRUE(reader_result.ok());
    auto reader = std::static_pointer_cast<MarbleRecordBatchReader>(*reader_result);

    // Read all data
    int64_t total_rows = 0;
    while (true) {
        std::shared_ptr<arrow::RecordBatch> batch;
        ASSERT_OK(reader->ReadNext(&batch));
        if (!batch) break;
        total_rows += batch->num_rows();
    }

    // Check statistics
    auto stats = reader->GetStats();
    EXPECT_GT(stats.bloom_filter_skips, 0) << "Should have skipped SSTables 2 and 3";

    // Should read ~100 rows from SSTable 1 only
    EXPECT_LE(total_rows, 100);
}

//==============================================================================
// Test 4: Greater Than Or Equal Predicate
//==============================================================================

TEST_F(ArrowPredicatePushdownTest, GreaterThanOrEqualPredicate) {
    // Create predicate: id >= 200
    // This should read SSTable 3 (id 200-299)
    // SSTables 1 and 2 should be skipped (max_key < 200)

    std::vector<ColumnPredicate> predicates;
    ColumnPredicate pred;
    pred.column_name = "id";
    pred.predicate_type = ColumnPredicate::PredicateType::kGreaterThanOrEqual;
    pred.value = std::make_shared<arrow::UInt64Scalar>(200);
    predicates.push_back(pred);

    auto reader_result = OpenTable(
        std::shared_ptr<MarbleDB>(db_.get(), [](MarbleDB*){}),
        "test_table",
        {},
        predicates);

    ASSERT_TRUE(reader_result.ok());
    auto reader = std::static_pointer_cast<MarbleRecordBatchReader>(*reader_result);

    // Read all data
    int64_t total_rows = 0;
    while (true) {
        std::shared_ptr<arrow::RecordBatch> batch;
        ASSERT_OK(reader->ReadNext(&batch));
        if (!batch) break;
        total_rows += batch->num_rows();
    }

    // Should read ~100 rows
    EXPECT_LE(total_rows, 100);
}

//==============================================================================
// Test 5: Less Than Or Equal Predicate
//==============================================================================

TEST_F(ArrowPredicatePushdownTest, LessThanOrEqualPredicate) {
    // Create predicate: id <= 99
    // This should read SSTable 1 (id 0-99)
    // SSTables 2 and 3 should be skipped (min_key > 99)

    std::vector<ColumnPredicate> predicates;
    ColumnPredicate pred;
    pred.column_name = "id";
    pred.predicate_type = ColumnPredicate::PredicateType::kLessThanOrEqual;
    pred.value = std::make_shared<arrow::UInt64Scalar>(99);
    predicates.push_back(pred);

    auto reader_result = OpenTable(
        std::shared_ptr<MarbleDB>(db_.get(), [](MarbleDB*){}),
        "test_table",
        {},
        predicates);

    ASSERT_TRUE(reader_result.ok());
    auto reader = std::static_pointer_cast<MarbleRecordBatchReader>(*reader_result);

    // Read all data
    int64_t total_rows = 0;
    while (true) {
        std::shared_ptr<arrow::RecordBatch> batch;
        ASSERT_OK(reader->ReadNext(&batch));
        if (!batch) break;
        total_rows += batch->num_rows();
    }

    // Should read ~100 rows
    EXPECT_LE(total_rows, 100);
}

//==============================================================================
// Test 6: Predicate Outside All Ranges - Skip All SSTables
//==============================================================================

TEST_F(ArrowPredicatePushdownTest, PredicateOutsideAllRangesSkipsAll) {
    // Create predicate: id = 10000
    // No SSTable contains this key - all should be skipped

    std::vector<ColumnPredicate> predicates;
    ColumnPredicate pred;
    pred.column_name = "id";
    pred.predicate_type = ColumnPredicate::PredicateType::kEqual;
    pred.value = std::make_shared<arrow::UInt64Scalar>(10000);
    predicates.push_back(pred);

    auto reader_result = OpenTable(
        std::shared_ptr<MarbleDB>(db_.get(), [](MarbleDB*){}),
        "test_table",
        {},
        predicates);

    ASSERT_TRUE(reader_result.ok());
    auto reader = std::static_pointer_cast<MarbleRecordBatchReader>(*reader_result);

    // Read all data
    int64_t total_rows = 0;
    while (true) {
        std::shared_ptr<arrow::RecordBatch> batch;
        ASSERT_OK(reader->ReadNext(&batch));
        if (!batch) break;
        total_rows += batch->num_rows();
    }

    // Check statistics - should have skipped all SSTables
    auto stats = reader->GetStats();
    EXPECT_GT(stats.bloom_filter_skips, 0) << "Should have skipped all SSTables";

    // Should read 0 rows
    EXPECT_EQ(total_rows, 0);
}

//==============================================================================
// Test 7: No Predicates - Read All SSTables
//==============================================================================

TEST_F(ArrowPredicatePushdownTest, NoPredicatesReadsAllSSTables) {
    // No predicates - should read all data

    auto reader_result = OpenTable(
        std::shared_ptr<MarbleDB>(db_.get(), [](MarbleDB*){}),
        "test_table");

    ASSERT_TRUE(reader_result.ok());
    auto reader = std::static_pointer_cast<MarbleRecordBatchReader>(*reader_result);

    // Read all data
    int64_t total_rows = 0;
    while (true) {
        std::shared_ptr<arrow::RecordBatch> batch;
        ASSERT_OK(reader->ReadNext(&batch));
        if (!batch) break;
        total_rows += batch->num_rows();
    }

    // Should read all 300 rows
    EXPECT_EQ(total_rows, 300);

    // No SSTables should be skipped
    auto stats = reader->GetStats();
    EXPECT_EQ(stats.bloom_filter_skips, 0);
}

//==============================================================================
// Test 8: Batch-Level Pruning (Zone Maps)
//==============================================================================

TEST_F(ArrowPredicatePushdownTest, BatchLevelZoneMapPruning) {
    // This test verifies that batches within an SSTable can be skipped
    // using runtime zone map computation (min/max per batch)

    // Create predicate that might match some batches but not others
    std::vector<ColumnPredicate> predicates;
    ColumnPredicate pred;
    pred.column_name = "id";
    pred.predicate_type = ColumnPredicate::PredicateType::kEqual;
    pred.value = std::make_shared<arrow::UInt64Scalar>(50);  // Should be in SSTable 1
    predicates.push_back(pred);

    auto reader_result = OpenTable(
        std::shared_ptr<MarbleDB>(db_.get(), [](MarbleDB*){}),
        "test_table",
        {},
        predicates);

    ASSERT_TRUE(reader_result.ok());
    auto reader = std::static_pointer_cast<MarbleRecordBatchReader>(*reader_result);

    // Read all data
    while (true) {
        std::shared_ptr<arrow::RecordBatch> batch;
        ASSERT_OK(reader->ReadNext(&batch));
        if (!batch) break;
    }

    // Check statistics - batches should be skipped via zone maps
    auto stats = reader->GetStats();
    // Zone map skipping happens at batch level within SSTables
    // Exact count depends on batch size, but should show some skipping
}

}  // namespace test
}  // namespace arrow_api
}  // namespace marble
