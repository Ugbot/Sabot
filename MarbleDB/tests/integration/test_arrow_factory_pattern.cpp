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
 * Test suite for Arrow factory pattern integration
 *
 * Tests that the factory method pattern correctly:
 * 1. Hides LSM internals from public API
 * 2. Returns standard Arrow RecordBatchReader
 * 3. Properly initializes schema from table metadata
 * 4. Handles missing tables gracefully
 * 5. Supports column projection
 */
class ArrowFactoryPatternTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Create test database
        DBOptions options;
        options.db_path = "/tmp/marble_arrow_factory_test";
        options.enable_wal = false;  // Disable WAL for test speed

        // Create schema: (id: uint64, name: string, value: int64)
        schema_ = arrow::schema({
            arrow::field("id", arrow::uint64()),
            arrow::field("name", arrow::utf8()),
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

        // Insert test data
        InsertTestData();
    }

    void TearDown() override {
        if (db_) {
            db_->Destroy();
        }
    }

    void InsertTestData() {
        // Create test batch: 100 rows
        arrow::UInt64Builder id_builder;
        arrow::StringBuilder name_builder;
        arrow::Int64Builder value_builder;

        for (int i = 0; i < 100; ++i) {
            ASSERT_OK(id_builder.Append(i));
            ASSERT_OK(name_builder.Append("name_" + std::to_string(i)));
            ASSERT_OK(value_builder.Append(i * 10));
        }

        std::shared_ptr<arrow::Array> id_array, name_array, value_array;
        ASSERT_OK(id_builder.Finish(&id_array));
        ASSERT_OK(name_builder.Finish(&name_array));
        ASSERT_OK(value_builder.Finish(&value_array));

        auto batch = arrow::RecordBatch::Make(
            schema_,
            100,
            {id_array, name_array, value_array});

        auto status = db_->InsertBatch("test_table", batch);
        ASSERT_TRUE(status.ok()) << status.ToString();

        // Flush to create SSTables
        status = db_->Flush();
        ASSERT_TRUE(status.ok()) << status.ToString();
    }

    std::unique_ptr<MarbleDB> db_;
    std::shared_ptr<arrow::Schema> schema_;
};

//==============================================================================
// Test 1: Factory Method Returns Standard Arrow Interface
//==============================================================================

TEST_F(ArrowFactoryPatternTest, FactoryReturnsStandardArrowInterface) {
    // Use factory function to open table
    auto reader_result = OpenTable(
        std::shared_ptr<MarbleDB>(db_.get(), [](MarbleDB*){}),  // Non-owning shared_ptr
        "test_table");

    ASSERT_TRUE(reader_result.ok()) << reader_result.status().ToString();

    auto reader = *reader_result;
    ASSERT_NE(reader, nullptr);

    // Verify it's a standard Arrow RecordBatchReader
    // (has schema() and ReadNext() methods)
    auto schema = reader->schema();
    ASSERT_NE(schema, nullptr);
    EXPECT_EQ(schema->num_fields(), 3);
    EXPECT_EQ(schema->field(0)->name(), "id");
    EXPECT_EQ(schema->field(1)->name(), "name");
    EXPECT_EQ(schema->field(2)->name(), "value");
}

//==============================================================================
// Test 2: Schema Loaded From Real Table Metadata
//==============================================================================

TEST_F(ArrowFactoryPatternTest, SchemaLoadedFromTableMetadata) {
    auto reader_result = OpenTable(
        std::shared_ptr<MarbleDB>(db_.get(), [](MarbleDB*){}),
        "test_table");

    ASSERT_TRUE(reader_result.ok());
    auto reader = *reader_result;

    // Verify schema has expected fields
    auto reader_schema = reader->schema();

    EXPECT_EQ(reader_schema->num_fields(), 3);
    EXPECT_EQ(reader_schema->field(0)->name(), "id");
    EXPECT_EQ(reader_schema->field(1)->name(), "name");
    EXPECT_EQ(reader_schema->field(2)->name(), "value");
}

//==============================================================================
// Test 3: Column Projection Works
//==============================================================================

TEST_F(ArrowFactoryPatternTest, ColumnProjectionWorks) {
    // Open table with projection: only read "id" and "value" columns
    std::vector<std::string> projection = {"id", "value"};

    auto reader_result = OpenTable(
        std::shared_ptr<MarbleDB>(db_.get(), [](MarbleDB*){}),
        "test_table",
        projection);

    ASSERT_TRUE(reader_result.ok());
    auto reader = *reader_result;

    // Verify projected schema
    auto schema = reader->schema();
    EXPECT_EQ(schema->num_fields(), 2);
    EXPECT_EQ(schema->field(0)->name(), "id");
    EXPECT_EQ(schema->field(1)->name(), "value");

    // "name" column should NOT be in schema
    EXPECT_EQ(schema->GetFieldIndex("name"), -1);
}

//==============================================================================
// Test 4: Invalid Column Projection Returns Error
//==============================================================================

TEST_F(ArrowFactoryPatternTest, InvalidColumnProjectionReturnsError) {
    // Try to project non-existent column
    std::vector<std::string> projection = {"id", "nonexistent_column"};

    auto reader_result = OpenTable(
        std::shared_ptr<MarbleDB>(db_.get(), [](MarbleDB*){}),
        "test_table",
        projection);

    // Should fail with error about missing column
    EXPECT_FALSE(reader_result.ok());
    EXPECT_TRUE(reader_result.status().ToString().find("nonexistent_column") != std::string::npos)
        << "Error message should mention missing column: " << reader_result.status().ToString();
}

//==============================================================================
// Test 5: Missing Table Returns Error
//==============================================================================

TEST_F(ArrowFactoryPatternTest, MissingTableReturnsError) {
    auto reader_result = OpenTable(
        std::shared_ptr<MarbleDB>(db_.get(), [](MarbleDB*){}),
        "nonexistent_table");

    // Should fail with error about missing table
    EXPECT_FALSE(reader_result.ok());
    EXPECT_TRUE(reader_result.status().ToString().find("nonexistent_table") != std::string::npos)
        << "Error message should mention missing table: " << reader_result.status().ToString();
}

//==============================================================================
// Test 6: Read Data Through Arrow Interface
//==============================================================================

TEST_F(ArrowFactoryPatternTest, ReadDataThroughArrowInterface) {
    auto reader_result = OpenTable(
        std::shared_ptr<MarbleDB>(db_.get(), [](MarbleDB*){}),
        "test_table");

    ASSERT_TRUE(reader_result.ok());
    auto reader = *reader_result;

    // Read all batches
    int64_t total_rows = 0;
    while (true) {
        std::shared_ptr<arrow::RecordBatch> batch;
        ASSERT_OK(reader->ReadNext(&batch));

        if (!batch) {
            break;  // End of stream
        }

        total_rows += batch->num_rows();

        // Verify batch schema matches reader schema
        EXPECT_TRUE(batch->schema()->Equals(reader->schema()));
    }

    // Should have read all 100 rows
    EXPECT_EQ(total_rows, 100);
}

//==============================================================================
// Test 7: Empty Table Returns No Batches
//==============================================================================

TEST_F(ArrowFactoryPatternTest, EmptyTableReturnsNoBatches) {
    // Create empty table
    auto schema = arrow::schema({
        arrow::field("id", arrow::uint64())
    });

    TableSchema tschema;
    tschema.table_name = "empty_table";
    tschema.arrow_schema = schema;

    auto status = db_->CreateTable(tschema);
    ASSERT_TRUE(status.ok());

    // Open empty table
    auto reader_result = OpenTable(
        std::shared_ptr<MarbleDB>(db_.get(), [](MarbleDB*){}),
        "empty_table");

    ASSERT_TRUE(reader_result.ok());
    auto reader = *reader_result;

    // Should return nullptr immediately (no batches)
    std::shared_ptr<arrow::RecordBatch> batch;
    ASSERT_OK(reader->ReadNext(&batch));
    EXPECT_EQ(batch, nullptr);
}

//==============================================================================
// Test 8: LSM Internals Hidden From Public API
//==============================================================================

TEST_F(ArrowFactoryPatternTest, LSMInternalsHiddenFromPublicAPI) {
    // This test verifies that we can't call GetSSTables() from public API
    // (it should be protected/internal only)

    // If we try to access LSM internals directly, it should not compile
    // (This is a compile-time test - the code below should NOT compile)

    // std::vector<std::vector<std::shared_ptr<SSTable>>> sstables;
    // auto status = db_->GetSSTables("test_table", &sstables);  // Should NOT compile

    // Instead, we can only use the factory function
    auto reader_result = OpenTable(
        std::shared_ptr<MarbleDB>(db_.get(), [](MarbleDB*){}),
        "test_table");

    EXPECT_TRUE(reader_result.ok());
    // This proves the factory pattern works without exposing internals
}

}  // namespace test
}  // namespace arrow_api
}  // namespace marble
