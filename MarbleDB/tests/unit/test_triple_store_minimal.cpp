/**
 * Minimal Triple Store Test - Reproduces RDF SPARQL Issue
 * =========================================================
 *
 * This test reproduces the exact pattern used by sabot_ql RDF triple store:
 * - 3 int64 columns (subject, predicate, object)
 * - InsertBatch API
 * - Flush to disk
 * - Scan back using ScanTable
 *
 * Expected: All tests should pass (rows returned after insert)
 * Actual: If bug exists, scans will return 0 rows
 */

#include "marble/api.h"
#include "marble/db.h"
#include "marble/column_family.h"
#include "marble/lsm_storage.h"
#include <arrow/api.h>
#include <gtest/gtest.h>
#include <iostream>
#include <memory>
#include <filesystem>

namespace fs = std::filesystem;

using namespace marble;

// Forward declaration of CreateDatabase (implemented in api.cpp)
extern Status CreateDatabase(const std::string& path, std::unique_ptr<MarbleDB>* db);

class TripleStoreMinimalTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Use temp directory for test database
        db_path_ = "/tmp/marble_triple_test";

        // Clean up any existing test database
        std::system(("rm -rf " + db_path_).c_str());
    }

    void TearDown() override {
        if (db_) {
            db_.reset();
        }
        // Clean up test database
        std::system(("rm -rf " + db_path_).c_str());
    }

    // Create a RecordBatch with triple data (subject, predicate, object)
    std::shared_ptr<arrow::RecordBatch> CreateTripleBatch(
        const std::vector<std::tuple<int64_t, int64_t, int64_t>>& triples
    ) {
        arrow::Int64Builder subject_builder;
        arrow::Int64Builder predicate_builder;
        arrow::Int64Builder object_builder;

        for (const auto& [s, p, o] : triples) {
            subject_builder.Append(s);
            predicate_builder.Append(p);
            object_builder.Append(o);
        }

        std::shared_ptr<arrow::Array> subject_array;
        std::shared_ptr<arrow::Array> predicate_array;
        std::shared_ptr<arrow::Array> object_array;

        subject_builder.Finish(&subject_array);
        predicate_builder.Finish(&predicate_array);
        object_builder.Finish(&object_array);

        auto schema = arrow::schema({
            arrow::field("subject", arrow::int64()),
            arrow::field("predicate", arrow::int64()),
            arrow::field("object", arrow::int64())
        });

        return arrow::RecordBatch::Make(
            schema,
            triples.size(),
            {subject_array, predicate_array, object_array}
        );
    }

    std::string db_path_;
    std::unique_ptr<MarbleDB> db_;
};

TEST_F(TripleStoreMinimalTest, SingleTripleInsertAndScan) {
    std::cout << "\n========================================" << std::endl;
    std::cout << "TEST: Single Triple Insert and Scan" << std::endl;
    std::cout << "========================================" << std::endl;

    // Step 1: Create database
    std::cout << "\nStep 1: Creating database at " << db_path_ << std::endl;
    auto status = CreateDatabase(db_path_, &db_);
    ASSERT_TRUE(status.ok()) << "Failed to create database: " << status.ToString();
    std::cout << "✓ Database created" << std::endl;

    // Step 2: Create SPO column family
    std::cout << "\nStep 2: Creating SPO column family (3 int64 columns)" << std::endl;
    ColumnFamilyOptions cf_options;
    cf_options.schema = arrow::schema({
        arrow::field("subject", arrow::int64()),
        arrow::field("predicate", arrow::int64()),
        arrow::field("object", arrow::int64())
    });

    ColumnFamilyDescriptor cf_desc("SPO", cf_options);
    ColumnFamilyHandle* cf_handle = nullptr;
    status = db_->CreateColumnFamily(cf_desc, &cf_handle);
    ASSERT_TRUE(status.ok()) << "Failed to create column family: " << status.ToString();
    std::cout << "✓ SPO column family created" << std::endl;

    // Step 3: Insert single triple (1, 2, 3)
    std::cout << "\nStep 3: Inserting single triple (1, 2, 3)" << std::endl;
    auto batch = CreateTripleBatch({{1, 2, 3}});
    ASSERT_NE(batch, nullptr);
    std::cout << "  Batch created: " << batch->num_rows() << " rows, "
              << batch->num_columns() << " columns" << std::endl;

    status = db_->InsertBatch("SPO", batch);
    ASSERT_TRUE(status.ok()) << "Failed to insert batch: " << status.ToString();
    std::cout << "✓ Batch inserted successfully" << std::endl;

    // Step 4: Flush to disk
    std::cout << "\nStep 4: Flushing to disk" << std::endl;
    status = db_->Flush();
    ASSERT_TRUE(status.ok()) << "Failed to flush: " << status.ToString();
    std::cout << "✓ Flushed successfully" << std::endl;

    // Step 5: Scan table back
    std::cout << "\nStep 5: Scanning table back" << std::endl;
    std::unique_ptr<QueryResult> result;
    status = db_->ScanTable("SPO", &result);
    ASSERT_TRUE(status.ok()) << "Failed to scan table: " << status.ToString();

    int64_t total_rows = result->num_rows();
    std::cout << "  Result: " << total_rows << " rows" << std::endl;

    // Step 6: Verify 1 row returned
    EXPECT_EQ(total_rows, 1) << "Expected 1 row, got " << total_rows;

    if (total_rows == 1) {
        // Step 7: Verify values match
        std::cout << "\nStep 6: Verifying values" << std::endl;

        // Get table from result
        auto table_result = result->GetTable();
        ASSERT_TRUE(table_result.ok()) << "Failed to get table: " << table_result.status().ToString();
        auto table = table_result.ValueOrDie();

        auto subject_col = table->column(0);
        auto predicate_col = table->column(1);
        auto object_col = table->column(2);

        auto subject_array = std::static_pointer_cast<arrow::Int64Array>(subject_col->chunk(0));
        auto predicate_array = std::static_pointer_cast<arrow::Int64Array>(predicate_col->chunk(0));
        auto object_array = std::static_pointer_cast<arrow::Int64Array>(object_col->chunk(0));

        EXPECT_EQ(subject_array->Value(0), 1);
        EXPECT_EQ(predicate_array->Value(0), 2);
        EXPECT_EQ(object_array->Value(0), 3);

        std::cout << "  Read back: (" << subject_array->Value(0) << ", "
                  << predicate_array->Value(0) << ", "
                  << object_array->Value(0) << ")" << std::endl;
        std::cout << "✓ Values match!" << std::endl;
    } else {
        std::cout << "\n❌ CRITICAL: Scan returned " << total_rows << " rows instead of 1" << std::endl;
    }
}

TEST_F(TripleStoreMinimalTest, SmallDatasetInsertAndScan) {
    std::cout << "\n========================================" << std::endl;
    std::cout << "TEST: Small Dataset (100 triples)" << std::endl;
    std::cout << "========================================" << std::endl;

    // Create database
    auto status = CreateDatabase(db_path_, &db_);
    ASSERT_TRUE(status.ok());

    // Create SPO column family
    ColumnFamilyOptions cf_options;
    cf_options.schema = arrow::schema({
        arrow::field("subject", arrow::int64()),
        arrow::field("predicate", arrow::int64()),
        arrow::field("object", arrow::int64())
    });

    ColumnFamilyDescriptor cf_desc("SPO", cf_options);
    ColumnFamilyHandle* cf_handle = nullptr;
    status = db_->CreateColumnFamily(cf_desc, &cf_handle);
    ASSERT_TRUE(status.ok());

    // Insert 100 triples
    std::cout << "\nInserting 100 triples..." << std::endl;
    std::vector<std::tuple<int64_t, int64_t, int64_t>> triples;
    for (int i = 0; i < 100; ++i) {
        triples.emplace_back(i, i % 10, i * 2);  // Pattern with duplicate predicates
    }

    auto batch = CreateTripleBatch(triples);
    status = db_->InsertBatch("SPO", batch);
    ASSERT_TRUE(status.ok());
    std::cout << "✓ Inserted 100 triples" << std::endl;

    // Flush
    status = db_->Flush();
    ASSERT_TRUE(status.ok());

    // Scan back
    std::cout << "\nScanning table..." << std::endl;
    std::unique_ptr<QueryResult> result;
    status = db_->ScanTable("SPO", &result);
    ASSERT_TRUE(status.ok());

    int64_t total_rows = result->num_rows();
    std::cout << "  Result: " << total_rows << " rows" << std::endl;

    EXPECT_EQ(total_rows, 100) << "Expected 100 rows, got " << total_rows;

    if (total_rows == 100) {
        std::cout << "✓ All 100 rows returned correctly!" << std::endl;
    } else {
        std::cout << "❌ CRITICAL: Expected 100 rows, got " << total_rows << std::endl;
    }
}

TEST_F(TripleStoreMinimalTest, DebugOutputSteps) {
    std::cout << "\n========================================" << std::endl;
    std::cout << "TEST: Debug Output Steps (5 triples)" << std::endl;
    std::cout << "========================================" << std::endl;

    // Create database with debug output
    std::cout << "\n[DEBUG] Creating database..." << std::endl;
    auto status = CreateDatabase(db_path_, &db_);
    std::cout << "[DEBUG] CreateDatabase status: " << status.ToString() << std::endl;
    ASSERT_TRUE(status.ok());

    // Create column family
    std::cout << "\n[DEBUG] Creating column family..." << std::endl;
    ColumnFamilyOptions cf_options;
    cf_options.schema = arrow::schema({
        arrow::field("subject", arrow::int64()),
        arrow::field("predicate", arrow::int64()),
        arrow::field("object", arrow::int64())
    });

    ColumnFamilyDescriptor cf_desc("SPO", cf_options);
    ColumnFamilyHandle* cf_handle = nullptr;
    status = db_->CreateColumnFamily(cf_desc, &cf_handle);
    std::cout << "[DEBUG] CreateColumnFamily status: " << status.ToString() << std::endl;
    std::cout << "[DEBUG] CF Handle: " << (void*)cf_handle << std::endl;
    ASSERT_TRUE(status.ok());

    // Create batch
    std::cout << "\n[DEBUG] Creating batch with 5 triples..." << std::endl;
    std::vector<std::tuple<int64_t, int64_t, int64_t>> triples = {
        {1, 100, 200},
        {2, 100, 201},
        {3, 101, 202},
        {4, 101, 203},
        {5, 102, 204}
    };
    auto batch = CreateTripleBatch(triples);
    std::cout << "[DEBUG] Batch: " << batch->num_rows() << " rows, "
              << batch->num_columns() << " columns" << std::endl;
    std::cout << "[DEBUG] Schema: " << batch->schema()->ToString() << std::endl;

    // Insert batch
    std::cout << "\n[DEBUG] Inserting batch..." << std::endl;
    status = db_->InsertBatch("SPO", batch);
    std::cout << "[DEBUG] InsertBatch status: " << status.ToString() << std::endl;
    ASSERT_TRUE(status.ok());

    // Flush
    std::cout << "\n[DEBUG] Flushing..." << std::endl;
    status = db_->Flush();
    std::cout << "[DEBUG] Flush status: " << status.ToString() << std::endl;
    ASSERT_TRUE(status.ok());

    // Scan table
    std::cout << "\n[DEBUG] Scanning table..." << std::endl;
    std::unique_ptr<QueryResult> result;
    status = db_->ScanTable("SPO", &result);
    std::cout << "[DEBUG] ScanTable status: " << status.ToString() << std::endl;
    ASSERT_TRUE(status.ok());

    int64_t total_rows = result->num_rows();
    std::cout << "[DEBUG] Result rows: " << total_rows << std::endl;

    if (total_rows > 0) {
        // Get table from result
        auto table_result = result->GetTable();
        ASSERT_TRUE(table_result.ok()) << "Failed to get table: " << table_result.status().ToString();
        auto table = table_result.ValueOrDie();

        std::cout << "[DEBUG] Result columns: " << table->num_columns() << std::endl;
        std::cout << "\n[DEBUG] First few rows:" << std::endl;

        for (int64_t i = 0; i < std::min(total_rows, static_cast<int64_t>(5)); ++i) {
            auto subject_col = std::static_pointer_cast<arrow::Int64Array>(table->column(0)->chunk(0));
            auto predicate_col = std::static_pointer_cast<arrow::Int64Array>(table->column(1)->chunk(0));
            auto object_col = std::static_pointer_cast<arrow::Int64Array>(table->column(2)->chunk(0));

            std::cout << "[DEBUG]   Row " << i << ": ("
                      << subject_col->Value(i) << ", "
                      << predicate_col->Value(i) << ", "
                      << object_col->Value(i) << ")" << std::endl;
        }
    }

    EXPECT_EQ(total_rows, 5) << "Expected 5 rows, got " << total_rows;

    if (total_rows != 5) {
        std::cout << "\n❌ CRITICAL FAILURE: Expected 5 rows, got " << total_rows << std::endl;
        std::cout << "This indicates the MarbleDB Arrow API is not working correctly." << std::endl;
    } else {
        std::cout << "\n✓ All 5 rows returned successfully!" << std::endl;
    }
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
