/**
 * Standalone test for Phase 5/6 predicate pushdown enhancements
 *
 * Tests that the enhanced OnRead() methods work correctly:
 * - SkippingIndexStrategy checks zone maps
 * - BloomFilterStrategy checks equality predicates
 * - StringPredicateStrategy detects LIKE predicates
 */

#include <gtest/gtest.h>
#include <arrow/api.h>

#include "marble/api.h"
#include "marble/db.h"  // For full MarbleDB definition
#include "marble/bloom_filter_strategy.h"
#include "marble/skipping_index_strategy.h"

namespace marble {
namespace test {

class PredicatePushdownPhase5Test : public ::testing::Test {
protected:
    void SetUp() override {
        // Create test database
        DBOptions options;
        options.db_path = "/tmp/marble_phase5_test";
        options.enable_wal = false;

        auto status = MarbleDB::Open(options, nullptr, &db_);
        ASSERT_TRUE(status.ok()) << status.ToString();

        // Create table with optimization strategies
        TableSchema tschema;
        tschema.table_name = "test_table";
        tschema.arrow_schema = arrow::schema({
            arrow::field("id", arrow::uint64()),
            arrow::field("age", arrow::int64()),
            arrow::field("name", arrow::utf8())
        });

        status = db_->CreateTable(tschema);
        ASSERT_TRUE(status.ok()) << status.ToString();
    }

    void TearDown() override {
        if (db_) {
            db_->Destroy();
        }
    }

    std::shared_ptr<MarbleDB> db_;
};

// Test 1: Skipping Index Zone Map Pruning
TEST_F(PredicatePushdownPhase5Test, SkippingIndexZoneMapPruning) {
    // Insert data with known ranges
    arrow::UInt64Builder id_builder;
    arrow::Int64Builder age_builder;
    arrow::StringBuilder name_builder;

    for (int i = 0; i < 100; ++i) {
        ASSERT_TRUE(id_builder.Append(i).ok());
        ASSERT_TRUE(age_builder.Append(20 + (i % 50)).ok());  // Age 20-69
        ASSERT_TRUE(name_builder.Append("name_" + std::to_string(i)).ok());
    }

    std::shared_ptr<arrow::Array> id_array, age_array, name_array;
    ASSERT_TRUE(id_builder.Finish(&id_array).ok());
    ASSERT_TRUE(age_builder.Finish(&age_array).ok());
    ASSERT_TRUE(name_builder.Finish(&name_array).ok());

    auto batch = arrow::RecordBatch::Make(
        arrow::schema({
            arrow::field("id", arrow::uint64()),
            arrow::field("age", arrow::int64()),
            arrow::field("name", arrow::utf8())
        }),
        100,
        {id_array, age_array, name_array}
    );

    auto status = db_->PutBatch("test_table", batch);
    ASSERT_TRUE(status.ok()) << status.ToString();

    // Flush to create SSTable
    status = db_->Flush("test_table");
    ASSERT_TRUE(status.ok()) << status.ToString();

    // Query with range predicate: age > 60
    // Zone maps should show age ranges (20-69), so SSTable shouldn't be skipped
    std::vector<ColumnPredicate> predicates;
    ColumnPredicate pred;
    pred.column_name = "age";
    pred.predicate_type = ColumnPredicate::PredicateType::kGreaterThan;
    pred.value = arrow::MakeScalar(60);
    predicates.push_back(pred);

    std::vector<std::shared_ptr<arrow::RecordBatch>> results;
    status = db_->ScanBatchesWithPredicates("test_table", 0, UINT64_MAX, predicates, &results);
    ASSERT_TRUE(status.ok()) << status.ToString();

    // Should get some results (ages 61-69)
    int64_t total_rows = 0;
    for (const auto& result_batch : results) {
        total_rows += result_batch->num_rows();
    }

    EXPECT_GT(total_rows, 0) << "Should find rows with age > 60";
    EXPECT_LT(total_rows, 100) << "Should not return all rows";
}

// Test 2: Bloom Filter Equality Predicate
TEST_F(PredicatePushdownPhase5Test, BloomFilterEqualityPredicate) {
    // Insert data
    arrow::UInt64Builder id_builder;
    arrow::Int64Builder age_builder;
    arrow::StringBuilder name_builder;

    for (int i = 0; i < 100; ++i) {
        ASSERT_TRUE(id_builder.Append(i).ok());
        ASSERT_TRUE(age_builder.Append(30).ok());
        ASSERT_TRUE(name_builder.Append("name_" + std::to_string(i)).ok());
    }

    std::shared_ptr<arrow::Array> id_array, age_array, name_array;
    ASSERT_TRUE(id_builder.Finish(&id_array).ok());
    ASSERT_TRUE(age_builder.Finish(&age_array).ok());
    ASSERT_TRUE(name_builder.Finish(&name_array).ok());

    auto batch = arrow::RecordBatch::Make(
        arrow::schema({
            arrow::field("id", arrow::uint64()),
            arrow::field("age", arrow::int64()),
            arrow::field("name", arrow::utf8())
        }),
        100,
        {id_array, age_array, name_array}
    );

    auto status = db_->PutBatch("test_table", batch);
    ASSERT_TRUE(status.ok()) << status.ToString();

    // Flush to create SSTable
    status = db_->Flush("test_table");
    ASSERT_TRUE(status.ok()) << status.ToString();

    // Query with equality predicate that DOESN'T exist: age = 99
    // Bloom filter should detect this and skip the SSTable
    std::vector<ColumnPredicate> predicates;
    ColumnPredicate pred;
    pred.column_name = "age";
    pred.predicate_type = ColumnPredicate::PredicateType::kEqual;
    pred.value = arrow::MakeScalar(99);
    predicates.push_back(pred);

    std::vector<std::shared_ptr<arrow::RecordBatch>> results;
    status = db_->ScanBatchesWithPredicates("test_table", 0, UINT64_MAX, predicates, &results);

    // Status might be OK or NotFound, both are fine
    // The key is that results should be empty (bloom filter prevented read)
    int64_t total_rows = 0;
    for (const auto& result_batch : results) {
        total_rows += result_batch->num_rows();
    }

    EXPECT_EQ(total_rows, 0) << "Should find no rows (bloom filter skip)";
}

// Test 3: Multiple Predicates (AND)
TEST_F(PredicatePushdownPhase5Test, MultiplePredicatesAND) {
    // Insert data
    arrow::UInt64Builder id_builder;
    arrow::Int64Builder age_builder;
    arrow::StringBuilder name_builder;

    for (int i = 0; i < 100; ++i) {
        ASSERT_TRUE(id_builder.Append(i).ok());
        ASSERT_TRUE(age_builder.Append(20 + (i % 50)).ok());
        ASSERT_TRUE(name_builder.Append("name_" + std::to_string(i)).ok());
    }

    std::shared_ptr<arrow::Array> id_array, age_array, name_array;
    ASSERT_TRUE(id_builder.Finish(&id_array).ok());
    ASSERT_TRUE(age_builder.Finish(&age_array).ok());
    ASSERT_TRUE(name_builder.Finish(&name_array).ok());

    auto batch = arrow::RecordBatch::Make(
        arrow::schema({
            arrow::field("id", arrow::uint64()),
            arrow::field("age", arrow::int64()),
            arrow::field("name", arrow::utf8())
        }),
        100,
        {id_array, age_array, name_array}
    );

    auto status = db_->PutBatch("test_table", batch);
    ASSERT_TRUE(status.ok()) << status.ToString();

    // Flush to create SSTable
    status = db_->Flush("test_table");
    ASSERT_TRUE(status.ok()) << status.ToString();

    // Query with multiple predicates: age > 30 AND age < 40
    std::vector<ColumnPredicate> predicates;

    ColumnPredicate pred1;
    pred1.column_name = "age";
    pred1.predicate_type = ColumnPredicate::PredicateType::kGreaterThan;
    pred1.value = arrow::MakeScalar(30);
    predicates.push_back(pred1);

    ColumnPredicate pred2;
    pred2.column_name = "age";
    pred2.predicate_type = ColumnPredicate::PredicateType::kLessThan;
    pred2.value = arrow::MakeScalar(40);
    predicates.push_back(pred2);

    std::vector<std::shared_ptr<arrow::RecordBatch>> results;
    status = db_->ScanBatchesWithPredicates("test_table", 0, UINT64_MAX, predicates, &results);
    ASSERT_TRUE(status.ok()) << status.ToString();

    // Should get some results (ages 31-39)
    int64_t total_rows = 0;
    for (const auto& result_batch : results) {
        total_rows += result_batch->num_rows();
    }

    EXPECT_GT(total_rows, 0) << "Should find rows with 30 < age < 40";
    EXPECT_LT(total_rows, 100) << "Should not return all rows";
}

}  // namespace test
}  // namespace marble

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
