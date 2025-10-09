/**
 * Unit Tests for Projection & Predicate Pushdown
 *
 * Tests SSTable-level filtering and column selection,
 * column statistics, and query optimization.
 */

#include <gtest/gtest.h>
#include <marble/sstable_arrow.h>
#include <marble/query.h>
#include <arrow/api.h>

namespace marble {

class PushdownTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Create test data - employee records
        CreateTestData();
    }

    void TearDown() override {}

    void CreateTestData() {
        // Create schema
        schema_ = arrow::schema({
            arrow::field("id", arrow::int64()),
            arrow::field("name", arrow::utf8()),
            arrow::field("age", arrow::int64()),
            arrow::field("salary", arrow::float64()),
            arrow::field("department", arrow::utf8())
        });

        // Create sample data - 100 employees
        arrow::Int64Builder id_builder, age_builder;
        arrow::StringBuilder name_builder, dept_builder;
        arrow::DoubleBuilder salary_builder;

        for (int64_t i = 0; i < 100; ++i) {
            id_builder.Append(i);
            name_builder.Append("Employee_" + std::to_string(i));
            age_builder.Append(25 + (i % 35)); // Ages 25-59
            salary_builder.Append(50000.0 + (i % 50000)); // Salaries 50k-99k
            dept_builder.Append((i % 4 == 0) ? "Engineering" :
                              (i % 4 == 1) ? "Sales" :
                              (i % 4 == 2) ? "Marketing" : "HR");
        }

        std::shared_ptr<arrow::Array> id_array, name_array, age_array, salary_array, dept_array;
        id_builder.Finish(&id_array);
        name_builder.Finish(&name_array);
        age_builder.Finish(&age_array);
        salary_builder.Finish(&salary_array);
        dept_builder.Finish(&dept_array);

        // Create RecordBatch
        batch_ = arrow::RecordBatch::Make(schema_, 100,
            {id_array, name_array, age_array, salary_array, dept_array});
    }

    std::shared_ptr<arrow::Schema> schema_;
    std::shared_ptr<arrow::RecordBatch> batch_;
};

//==============================================================================
// Test ArrowPredicateEvaluator
//==============================================================================

TEST_F(PushdownTest, ArrowPredicateEvaluatorCreation) {
    std::vector<ColumnPredicate> predicates = {
        ColumnPredicate("age", PredicateType::kGreaterThan,
                       arrow::MakeScalar<int64_t>(30)),
        ColumnPredicate("department", PredicateType::kEqual,
                       arrow::MakeScalar<std::string>("Engineering"))
    };

    auto evaluator = CreateArrowPredicateEvaluator(predicates);
    ASSERT_TRUE(evaluator != nullptr);
    EXPECT_TRUE(evaluator->HasPredicates());
}

TEST_F(PushdownTest, ArrowPredicateEvaluatorEvaluateBatch) {
    // Test simple predicate: age > 30
    std::vector<ColumnPredicate> predicates = {
        ColumnPredicate("age", PredicateType::kGreaterThan,
                       arrow::MakeScalar<int64_t>(30))
    };

    auto evaluator = CreateArrowPredicateEvaluator(predicates);
    std::shared_ptr<arrow::BooleanArray> mask;

    auto status = evaluator->EvaluateBatch(batch_, &mask);
    ASSERT_TRUE(status.ok());
    ASSERT_TRUE(mask != nullptr);
    EXPECT_EQ(mask->length(), 100);

    // Count how many rows match (should be most rows since ages range from 25-59)
    int64_t match_count = 0;
    for (int64_t i = 0; i < mask->length(); ++i) {
        if (mask->Value(i)) match_count++;
    }
    EXPECT_GT(match_count, 0); // Should have some matches
    EXPECT_LT(match_count, 100); // Should not match all
}

TEST_F(PushdownTest, ArrowPredicateEvaluatorFilterBatch) {
    // Test filtering with department = "Engineering"
    std::vector<ColumnPredicate> predicates = {
        ColumnPredicate("department", PredicateType::kEqual,
                       arrow::MakeScalar<std::string>("Engineering"))
    };

    auto evaluator = CreateArrowPredicateEvaluator(predicates);
    std::shared_ptr<arrow::RecordBatch> filtered_batch;

    auto status = evaluator->FilterBatch(batch_, &filtered_batch);
    ASSERT_TRUE(status.ok());
    ASSERT_TRUE(filtered_batch != nullptr);

    // Should have ~25 Engineering employees (100/4 = 25)
    EXPECT_EQ(filtered_batch->num_rows(), 25);
    EXPECT_EQ(filtered_batch->num_columns(), 5); // All columns

    // Verify department column contains only "Engineering"
    auto dept_array = std::static_pointer_cast<arrow::StringArray>(
        filtered_batch->GetColumnByName("department"));
    for (int64_t i = 0; i < filtered_batch->num_rows(); ++i) {
        EXPECT_EQ(dept_array->GetString(i), "Engineering");
    }
}

TEST_F(PushdownTest, ArrowPredicateEvaluatorComplexPredicates) {
    // Test complex predicate: age > 40 AND salary > 70000
    std::vector<ColumnPredicate> predicates = {
        ColumnPredicate("age", PredicateType::kGreaterThan,
                       arrow::MakeScalar<int64_t>(40)),
        ColumnPredicate("salary", PredicateType::kGreaterThan,
                       arrow::MakeScalar<double>(70000.0))
    };

    auto evaluator = CreateArrowPredicateEvaluator(predicates);
    std::shared_ptr<arrow::BooleanArray> mask;

    auto status = evaluator->EvaluateBatch(batch_, &mask);
    ASSERT_TRUE(status.ok());

    int64_t match_count = 0;
    for (int64_t i = 0; i < mask->length(); ++i) {
        if (mask->Value(i)) match_count++;
    }

    // Should have some matches but not too many
    EXPECT_GT(match_count, 0);
    EXPECT_LT(match_count, 50);
}

TEST_F(PushdownTest, ArrowPredicateEvaluatorBetweenPredicate) {
    // Test BETWEEN predicate: salary BETWEEN 60000 AND 80000
    std::vector<ColumnPredicate> predicates = {
        ColumnPredicate("salary", PredicateType::kBetween,
                       arrow::MakeScalar<double>(60000.0),
                       arrow::MakeScalar<double>(80000.0))
    };

    auto evaluator = CreateArrowPredicateEvaluator(predicates);
    std::shared_ptr<arrow::BooleanArray> mask;

    auto status = evaluator->EvaluateBatch(batch_, &mask);
    ASSERT_TRUE(status.ok());

    int64_t match_count = 0;
    for (int64_t i = 0; i < mask->length(); ++i) {
        if (mask->Value(i)) match_count++;
    }

    // Should have matches within the salary range
    EXPECT_GT(match_count, 0);
}

//==============================================================================
// Test ArrowColumnProjector
//==============================================================================

TEST_F(PushdownTest, ArrowColumnProjectorCreation) {
    std::vector<std::string> columns = {"name", "salary"};
    auto projector = CreateArrowColumnProjector(columns);
    ASSERT_TRUE(projector != nullptr);
    EXPECT_TRUE(projector->NeedsProjection());
}

TEST_F(PushdownTest, ArrowColumnProjectorFullProjection) {
    std::vector<std::string> columns = {}; // Empty = all columns
    auto projector = CreateArrowColumnProjector(columns);
    EXPECT_FALSE(projector->NeedsProjection());
}

TEST_F(PushdownTest, ArrowColumnProjectorProjection) {
    std::vector<std::string> columns = {"id", "name", "salary"}; // 3 out of 5 columns
    auto projector = CreateArrowColumnProjector(columns);

    std::shared_ptr<arrow::RecordBatch> projected_batch;
    auto status = projector->ProjectBatch(batch_, &projected_batch);

    ASSERT_TRUE(status.ok());
    ASSERT_TRUE(projected_batch != nullptr);
    EXPECT_EQ(projected_batch->num_rows(), 100); // Same rows
    EXPECT_EQ(projected_batch->num_columns(), 3); // Only selected columns

    // Verify column names
    EXPECT_EQ(projected_batch->schema()->field(0)->name(), "id");
    EXPECT_EQ(projected_batch->schema()->field(1)->name(), "name");
    EXPECT_EQ(projected_batch->schema()->field(2)->name(), "salary");
}

TEST_F(PushdownTest, ArrowColumnProjectorProjectionIndices) {
    std::vector<std::string> columns = {"age", "department"}; // Columns at indices 2 and 4
    auto projector = CreateArrowColumnProjector(columns);

    std::vector<int> indices;
    auto status = projector->GetProjectionIndices(batch_->schema(), &indices);

    ASSERT_TRUE(status.ok());
    EXPECT_EQ(indices.size(), 2);
    EXPECT_EQ(indices[0], 2); // age column
    EXPECT_EQ(indices[1], 4); // department column
}

TEST_F(PushdownTest, ArrowColumnProjectorProjectedSchema) {
    std::vector<std::string> columns = {"name", "salary"};
    auto projector = CreateArrowColumnProjector(columns);

    auto projected_schema = projector->GetProjectedSchema(batch_->schema());

    EXPECT_EQ(projected_schema->num_fields(), 2);
    EXPECT_EQ(projected_schema->field(0)->name(), "name");
    EXPECT_EQ(projected_schema->field(1)->name(), "salary");
}

//==============================================================================
// Test Column Statistics
//==============================================================================

TEST_F(PushdownTest, ColumnStatisticsBasic) {
    ColumnStatistics stats("test_column");
    EXPECT_EQ(stats.column_name, "test_column");
    EXPECT_FALSE(stats.has_nulls);
    EXPECT_EQ(stats.null_count, 0);
}

TEST_F(PushdownTest, ColumnStatisticsBuildInt64) {
    // Create test data for age column
    auto age_column = batch_->GetColumnByName("age");
    std::vector<std::shared_ptr<arrow::Array>> chunks = {age_column};
    auto chunked_array = std::make_shared<arrow::ChunkedArray>(chunks);

    ColumnStatistics stats("age");
    auto status = BuildColumnStatistics(chunked_array, "age", &stats);

    ASSERT_TRUE(status.ok());
    EXPECT_EQ(stats.column_name, "age");
    EXPECT_FALSE(stats.has_nulls); // Our test data has no nulls
    EXPECT_EQ(stats.null_count, 0);
    EXPECT_GE(stats.min_value, arrow::MakeScalar<int64_t>(25));
    EXPECT_LE(stats.max_value, arrow::MakeScalar<int64_t>(59));
}

TEST_F(PushdownTest, ColumnStatisticsBuildString) {
    // Create test data for department column
    auto dept_column = batch_->GetColumnByName("department");
    std::vector<std::shared_ptr<arrow::Array>> chunks = {dept_column};
    auto chunked_array = std::make_shared<arrow::ChunkedArray>(chunks);

    ColumnStatistics stats("department");
    auto status = BuildColumnStatistics(chunked_array, "department", &stats);

    ASSERT_TRUE(status.ok());
    EXPECT_EQ(stats.column_name, "department");
    EXPECT_FALSE(stats.has_nulls);
    EXPECT_EQ(stats.null_count, 0);
    EXPECT_EQ(stats.min_length, 2); // "HR" is shortest
    EXPECT_EQ(stats.max_length, 11); // "Engineering" is longest
}

TEST_F(PushdownTest, ColumnStatisticsBuildDouble) {
    // Create test data for salary column
    auto salary_column = batch_->GetColumnByName("salary");
    std::vector<std::shared_ptr<arrow::Array>> chunks = {salary_column};
    auto chunked_array = std::make_shared<arrow::ChunkedArray>(chunks);

    ColumnStatistics stats("salary");
    auto status = BuildColumnStatistics(chunked_array, "salary", &stats);

    ASSERT_TRUE(status.ok());
    EXPECT_EQ(stats.column_name, "salary");
    EXPECT_FALSE(stats.has_nulls);
    EXPECT_EQ(stats.null_count, 0);
    EXPECT_GE(stats.mean, 50000.0); // Minimum salary
    EXPECT_LE(stats.mean, 100000.0); // Maximum salary
    EXPECT_GE(stats.min_value->ViewAs<arrow::DoubleScalar>()->value, 50000.0);
    EXPECT_LE(stats.max_value->ViewAs<arrow::DoubleScalar>()->value, 99999.0);
}

TEST_F(PushdownTest, ColumnStatisticsCanSatisfyPredicate) {
    ColumnStatistics age_stats("age");
    // Set up stats to simulate age range 25-59
    age_stats.min_value = arrow::MakeScalar<int64_t>(25);
    age_stats.max_value = arrow::MakeScalar<int64_t>(59);

    // Test predicates that can be satisfied
    ColumnPredicate age_gt_20("age", PredicateType::kGreaterThan, arrow::MakeScalar<int64_t>(20));
    EXPECT_TRUE(age_stats.CanSatisfyPredicate(age_gt_20));

    ColumnPredicate age_lt_30("age", PredicateType::kLessThan, arrow::MakeScalar<int64_t>(30));
    EXPECT_TRUE(age_stats.CanSatisfyPredicate(age_lt_30));

    // Test predicates that cannot be satisfied
    ColumnPredicate age_gt_100("age", PredicateType::kGreaterThan, arrow::MakeScalar<int64_t>(100));
    EXPECT_FALSE(age_stats.CanSatisfyPredicate(age_gt_100)); // Max age is 59

    ColumnPredicate age_lt_10("age", PredicateType::kLessThan, arrow::MakeScalar<int64_t>(10));
    EXPECT_FALSE(age_stats.CanSatisfyPredicate(age_lt_10)); // Min age is 25

    // Test equality that's impossible
    ColumnPredicate age_eq_100("age", PredicateType::kEqual, arrow::MakeScalar<int64_t>(100));
    EXPECT_FALSE(age_stats.CanSatisfyPredicate(age_eq_100));
}

//==============================================================================
// Test Combined Pushdown Operations
//==============================================================================

TEST_F(PushdownTest, CombinedProjectionAndPredicatePushdown) {
    // Test both projection and predicate pushdown together
    std::vector<std::string> projection_columns = {"name", "salary", "department"};
    std::vector<ColumnPredicate> predicates = {
        ColumnPredicate("age", PredicateType::kGreaterThan, arrow::MakeScalar<int64_t>(40)),
        ColumnPredicate("department", PredicateType::kEqual, arrow::MakeScalar<std::string>("Engineering"))
    };

    // First apply predicate evaluation
    auto evaluator = CreateArrowPredicateEvaluator(predicates);
    std::shared_ptr<arrow::RecordBatch> filtered_batch;
    auto eval_status = evaluator->FilterBatch(batch_, &filtered_batch);
    ASSERT_TRUE(eval_status.ok());

    // Then apply projection
    auto projector = CreateArrowColumnProjector(projection_columns);
    std::shared_ptr<arrow::RecordBatch> final_batch;
    auto proj_status = projector->ProjectBatch(filtered_batch, &final_batch);
    ASSERT_TRUE(proj_status.ok());

    // Verify results
    ASSERT_TRUE(final_batch != nullptr);
    EXPECT_EQ(final_batch->num_columns(), 3); // Only projected columns
    EXPECT_LT(final_batch->num_rows(), 100); // Fewer rows due to filtering

    // Verify column names
    EXPECT_EQ(final_batch->schema()->field(0)->name(), "name");
    EXPECT_EQ(final_batch->schema()->field(1)->name(), "salary");
    EXPECT_EQ(final_batch->schema()->field(2)->name(), "department");

    // Verify department filtering worked
    auto dept_array = std::static_pointer_cast<arrow::StringArray>(
        final_batch->GetColumnByName("department"));
    for (int64_t i = 0; i < final_batch->num_rows(); ++i) {
        EXPECT_EQ(dept_array->GetString(i), "Engineering");
    }
}

TEST_F(PushdownTest, EmptyProjectionColumns) {
    // Test with empty projection (should return all columns)
    std::vector<std::string> projection_columns = {};
    std::vector<ColumnPredicate> predicates = {
        ColumnPredicate("id", PredicateType::kLessThan, arrow::MakeScalar<int64_t>(10))
    };

    auto evaluator = CreateArrowPredicateEvaluator(predicates);
    auto projector = CreateArrowColumnProjector(projection_columns);

    std::shared_ptr<arrow::RecordBatch> filtered_batch, final_batch;

    auto eval_status = evaluator->FilterBatch(batch_, &filtered_batch);
    ASSERT_TRUE(eval_status.ok());

    auto proj_status = projector->ProjectBatch(filtered_batch, &final_batch);
    ASSERT_TRUE(proj_status.ok());

    // Should have all 5 columns but only 10 rows (id < 10)
    EXPECT_EQ(final_batch->num_columns(), 5);
    EXPECT_EQ(final_batch->num_rows(), 10);
}

TEST_F(PushdownTest, NoPredicates) {
    // Test projection only (no predicates)
    std::vector<std::string> projection_columns = {"id", "name"};
    std::vector<ColumnPredicate> predicates = {};

    auto evaluator = CreateArrowPredicateEvaluator(predicates);
    auto projector = CreateArrowColumnProjector(projection_columns);

    std::shared_ptr<arrow::RecordBatch> filtered_batch, final_batch;

    auto eval_status = evaluator->FilterBatch(batch_, &filtered_batch);
    ASSERT_TRUE(eval_status.ok());

    auto proj_status = projector->ProjectBatch(filtered_batch, &final_batch);
    ASSERT_TRUE(proj_status.ok());

    // Should have 2 columns and all 100 rows
    EXPECT_EQ(final_batch->num_columns(), 2);
    EXPECT_EQ(final_batch->num_rows(), 100);
}

} // namespace marble
