/**
 * Integration Tests for Query Execution
 *
 * Tests end-to-end query execution with pushdown,
 * table operations, and complex analytical queries.
 */

#include <gtest/gtest.h>
#include <marble/table.h>
#include <marble/analytics.h>
#include <marble/temporal.h>
#include <marble/query.h>
#include <marble/sstable_arrow.h>
#include <filesystem>
#include <memory>

namespace fs = std::filesystem;

class QueryExecutionTest : public ::testing::Test {
protected:
    void SetUp() override {
        test_path_ = "/tmp/marble_query_test_" + std::to_string(std::time(nullptr));
        fs::create_directories(test_path_);

        // Create test database
        db_ = CreateDatabase(test_path_);

        // Create test schema
        schema_ = arrow::schema({
            arrow::field("id", arrow::int64()),
            arrow::field("name", arrow::utf8()),
            arrow::field("age", arrow::int64()),
            arrow::field("salary", arrow::float64()),
            arrow::field("department", arrow::utf8()),
            arrow::field("hire_date", arrow::timestamp(arrow::TimeUnit::MICRO))
        });

        // Create test data
        CreateTestData();
    }

    void TearDown() override {
        db_.reset();
        fs::remove_all(test_path_);
    }

    void CreateTestData() {
        // Create 1000 employee records
        for (int64_t i = 0; i < 1000; ++i) {
            // Create record batch for each employee
            arrow::Int64Builder id_builder, age_builder;
            arrow::StringBuilder name_builder, dept_builder;
            arrow::DoubleBuilder salary_builder;
            arrow::TimestampBuilder hire_date_builder(arrow::timestamp(arrow::TimeUnit::MICRO), arrow::default_memory_pool());

            id_builder.Append(i);
            name_builder.Append("Employee_" + std::to_string(i));
            age_builder.Append(25 + (i % 35)); // Ages 25-59
            salary_builder.Append(50000.0 + (i % 50000)); // Salaries 50k-99k
            dept_builder.Append((i % 4 == 0) ? "Engineering" :
                              (i % 4 == 1) ? "Sales" :
                              (i % 4 == 2) ? "Marketing" : "HR");

            // Hire date: some time in the past 10 years
            auto hire_timestamp = std::chrono::system_clock::now() -
                                std::chrono::hours(24 * 365 * (i % 10));
            auto hire_micros = std::chrono::duration_cast<std::chrono::microseconds>(
                hire_timestamp.time_since_epoch()).count();
            hire_date_builder.Append(hire_micros);

            std::shared_ptr<arrow::Array> arrays[6];
            id_builder.Finish(&arrays[0]);
            name_builder.Finish(&arrays[1]);
            age_builder.Finish(&arrays[2]);
            salary_builder.Finish(&arrays[3]);
            dept_builder.Finish(&arrays[4]);
            hire_date_builder.Finish(&arrays[5]);

            auto batch = arrow::RecordBatch::Make(schema_, 1, arrays);

            // Add to database (this would normally go through the table interface)
            // For now, we'll test the components directly
            batches_.push_back(batch);
        }
    }

    std::string test_path_;
    std::shared_ptr<MarbleDB> db_;
    std::shared_ptr<arrow::Schema> schema_;
    std::vector<std::shared_ptr<arrow::RecordBatch>> batches_;
};

//==============================================================================
// Test End-to-End Query Execution with Pushdown
//==============================================================================

TEST_F(QueryExecutionTest, EndToEndProjectionPushdown) {
    // Test query: SELECT name, salary FROM employees
    std::vector<std::string> projection_columns = {"name", "salary"};
    std::vector<ColumnPredicate> predicates = {}; // No filtering

    // Create evaluator and projector
    auto evaluator = CreateArrowPredicateEvaluator(predicates);
    auto projector = CreateArrowColumnProjector(projection_columns);

    // Process all batches
    int64_t total_rows = 0;
    for (const auto& batch : batches_) {
        std::shared_ptr<arrow::RecordBatch> filtered_batch;
        auto eval_status = evaluator->FilterBatch(batch, &filtered_batch);
        ASSERT_TRUE(eval_status.ok());

        std::shared_ptr<arrow::RecordBatch> projected_batch;
        auto proj_status = projector->ProjectBatch(filtered_batch, &projected_batch);
        ASSERT_TRUE(proj_status.ok());

        total_rows += projected_batch->num_rows();

        // Verify projection worked
        EXPECT_EQ(projected_batch->num_columns(), 2);
        EXPECT_EQ(projected_batch->schema()->field(0)->name(), "name");
        EXPECT_EQ(projected_batch->schema()->field(1)->name(), "salary");
    }

    EXPECT_EQ(total_rows, 1000); // All rows, just projected columns
}

TEST_F(QueryExecutionTest, EndToEndPredicatePushdown) {
    // Test query: SELECT * FROM employees WHERE age > 50
    std::vector<std::string> projection_columns = {}; // All columns
    std::vector<ColumnPredicate> predicates = {
        ColumnPredicate("age", PredicateType::kGreaterThan, arrow::MakeScalar<int64_t>(50))
    };

    auto evaluator = CreateArrowPredicateEvaluator(predicates);
    auto projector = CreateArrowColumnProjector(projection_columns);

    int64_t total_matching_rows = 0;
    for (const auto& batch : batches_) {
        std::shared_ptr<arrow::RecordBatch> filtered_batch;
        auto eval_status = evaluator->FilterBatch(batch, &filtered_batch);
        ASSERT_TRUE(eval_status.ok());

        std::shared_ptr<arrow::RecordBatch> final_batch;
        auto proj_status = projector->ProjectBatch(filtered_batch, &final_batch);
        ASSERT_TRUE(proj_status.ok());

        total_matching_rows += final_batch->num_rows();

        // Verify all rows in result have age > 50
        if (final_batch->num_rows() > 0) {
            auto age_array = std::static_pointer_cast<arrow::Int64Array>(
                final_batch->GetColumnByName("age"));
            for (int64_t i = 0; i < final_batch->num_rows(); ++i) {
                EXPECT_GT(age_array->Value(i), 50);
            }
        }
    }

    // Should have some matches (ages range from 25-59, so > 50 should match some)
    EXPECT_GT(total_matching_rows, 0);
    EXPECT_LT(total_matching_rows, 1000);
}

TEST_F(QueryExecutionTest, EndToEndComplexQuery) {
    // Test complex query: SELECT name, salary FROM employees
    // WHERE department = 'Engineering' AND salary > 70000
    std::vector<std::string> projection_columns = {"name", "salary"};
    std::vector<ColumnPredicate> predicates = {
        ColumnPredicate("department", PredicateType::kEqual,
                       arrow::MakeScalar<std::string>("Engineering")),
        ColumnPredicate("salary", PredicateType::kGreaterThan,
                       arrow::MakeScalar<double>(70000.0))
    };

    auto evaluator = CreateArrowPredicateEvaluator(predicates);
    auto projector = CreateArrowColumnProjector(projection_columns);

    int64_t total_matching_rows = 0;
    for (const auto& batch : batches_) {
        std::shared_ptr<arrow::RecordBatch> filtered_batch;
        auto eval_status = evaluator->FilterBatch(batch, &filtered_batch);
        ASSERT_TRUE(eval_status.ok());

        std::shared_ptr<arrow::RecordBatch> final_batch;
        auto proj_status = projector->ProjectBatch(filtered_batch, &final_batch);
        ASSERT_TRUE(proj_status.ok());

        total_matching_rows += final_batch->num_rows();

        // Verify complex conditions
        if (final_batch->num_rows() > 0) {
            auto salary_array = std::static_pointer_cast<arrow::DoubleArray>(
                final_batch->GetColumnByName("salary"));
            auto name_array = std::static_pointer_cast<arrow::StringArray>(
                final_batch->GetColumnByName("name"));

            for (int64_t i = 0; i < final_batch->num_rows(); ++i) {
                EXPECT_GT(salary_array->Value(i), 70000.0);
                // Names should be from Engineering department (every 4th employee)
                std::string name = name_array->GetString(i);
                size_t id_start = name.find("Employee_");
                if (id_start != std::string::npos) {
                    std::string id_str = name.substr(9);
                    int64_t id = std::stoll(id_str);
                    EXPECT_EQ(id % 4, 0); // Engineering employees
                }
            }
        }
    }

    // Should have some matches but not too many
    EXPECT_GT(total_matching_rows, 0);
    EXPECT_LT(total_matching_rows, 200); // Engineering is 25% of employees, high salary is subset
}

TEST_F(QueryExecutionTest, QueryWithColumnStatistics) {
    // Test using column statistics for query optimization
    ColumnStatistics age_stats("age");

    // Build statistics from our test data
    std::vector<std::shared_ptr<arrow::Array>> age_chunks;
    for (const auto& batch : batches_) {
        age_chunks.push_back(batch->GetColumnByName("age"));
    }
    auto age_chunked = std::make_shared<arrow::ChunkedArray>(age_chunks);

    auto status = BuildColumnStatistics(age_chunked, "age", &age_stats);
    ASSERT_TRUE(status.ok());

    // Verify statistics are reasonable
    EXPECT_GE(age_stats.min_value->ViewAs<arrow::Int64Scalar>()->value, 25);
    EXPECT_LE(age_stats.max_value->ViewAs<arrow::Int64Scalar>()->value, 59);

    // Test predicate elimination
    ColumnPredicate impossible_pred("age", PredicateType::kGreaterThan,
                                   arrow::MakeScalar<int64_t>(100));
    EXPECT_FALSE(age_stats.CanSatisfyPredicate(impossible_pred));

    ColumnPredicate possible_pred("age", PredicateType::kGreaterThan,
                                 arrow::MakeScalar<int64_t>(30));
    EXPECT_TRUE(age_stats.CanSatisfyPredicate(possible_pred));
}

TEST_F(QueryExecutionTest, StreamingQueryExecution) {
    // Test streaming query execution
    std::vector<std::string> projection_columns = {"id", "name", "department"};
    std::vector<ColumnPredicate> predicates = {
        ColumnPredicate("department", PredicateType::kEqual,
                       arrow::MakeScalar<std::string>("Sales"))
    };

    auto evaluator = CreateArrowPredicateEvaluator(predicates);
    auto projector = CreateArrowColumnProjector(projection_columns);

    // Simulate streaming processing
    int64_t total_processed_rows = 0;
    int64_t total_output_rows = 0;

    for (const auto& input_batch : batches_) {
        total_processed_rows += input_batch->num_rows();

        // Apply filtering
        std::shared_ptr<arrow::RecordBatch> filtered_batch;
        auto eval_status = evaluator->FilterBatch(input_batch, &filtered_batch);
        ASSERT_TRUE(eval_status.ok());

        // Apply projection
        std::shared_ptr<arrow::RecordBatch> output_batch;
        auto proj_status = projector->ProjectBatch(filtered_batch, &output_batch);
        ASSERT_TRUE(proj_status.ok());

        total_output_rows += output_batch->num_rows();

        // Verify output batch structure
        if (output_batch->num_rows() > 0) {
            EXPECT_EQ(output_batch->num_columns(), 3);
            EXPECT_EQ(output_batch->schema()->field(0)->name(), "id");
            EXPECT_EQ(output_batch->schema()->field(1)->name(), "name");
            EXPECT_EQ(output_batch->schema()->field(2)->name(), "department");

            // Verify all departments are "Sales"
            auto dept_array = std::static_pointer_cast<arrow::StringArray>(
                output_batch->GetColumnByName("department"));
            for (int64_t i = 0; i < output_batch->num_rows(); ++i) {
                EXPECT_EQ(dept_array->GetString(i), "Sales");
            }
        }
    }

    EXPECT_EQ(total_processed_rows, 1000); // All input rows processed
    EXPECT_EQ(total_output_rows, 250); // Sales department is 25% of employees
}

//==============================================================================
// Test Query Performance Characteristics
//==============================================================================

TEST_F(QueryExecutionTest, QueryPerformanceScaling) {
    // Test how query performance scales with data size
    auto start_time = std::chrono::high_resolution_clock::now();

    // Run a moderately complex query
    std::vector<std::string> projection_columns = {"name", "salary", "age"};
    std::vector<ColumnPredicate> predicates = {
        ColumnPredicate("age", PredicateType::kBetween,
                       arrow::MakeScalar<int64_t>(30),
                       arrow::MakeScalar<int64_t>(50)),
        ColumnPredicate("salary", PredicateType::kGreaterThan,
                       arrow::MakeScalar<double>(60000.0))
    };

    auto evaluator = CreateArrowPredicateEvaluator(predicates);
    auto projector = CreateArrowColumnProjector(projection_columns);

    int64_t total_output_rows = 0;
    for (const auto& batch : batches_) {
        std::shared_ptr<arrow::RecordBatch> filtered_batch;
        evaluator->FilterBatch(batch, &filtered_batch);

        std::shared_ptr<arrow::RecordBatch> output_batch;
        projector->ProjectBatch(filtered_batch, &output_batch);

        total_output_rows += output_batch->num_rows();
    }

    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
        end_time - start_time);

    // Verify we got some results
    EXPECT_GT(total_output_rows, 0);
    EXPECT_LT(total_output_rows, 1000);

    // Performance should be reasonable (less than 1 second for 1000 rows)
    EXPECT_LT(duration.count(), 1000);

    std::cout << "Query processed " << total_output_rows << " rows in "
              << duration.count() << "ms" << std::endl;
}

TEST_F(QueryExecutionTest, MemoryEfficiency) {
    // Test that pushdown operations don't create excessive memory overhead
    std::vector<std::string> projection_columns = {"id"};
    std::vector<ColumnPredicate> predicates = {
        ColumnPredicate("id", PredicateType::kLessThan, arrow::MakeScalar<int64_t>(100))
    };

    auto evaluator = CreateArrowPredicateEvaluator(predicates);
    auto projector = CreateArrowColumnProjector(projection_columns);

    // Process in streaming fashion
    size_t max_memory_usage = 0;
    int64_t total_output_rows = 0;

    for (const auto& batch : batches_) {
        std::shared_ptr<arrow::RecordBatch> filtered_batch;
        evaluator->FilterBatch(batch, &filtered_batch);

        std::shared_ptr<arrow::RecordBatch> output_batch;
        projector->ProjectBatch(filtered_batch, &output_batch);

        total_output_rows += output_batch->num_rows();

        // Rough memory estimation
        size_t batch_memory = output_batch->num_rows() * output_batch->num_columns() * 8; // Rough estimate
        max_memory_usage = std::max(max_memory_usage, batch_memory);
    }

    EXPECT_EQ(total_output_rows, 100); // id < 100 should match 100 rows
    EXPECT_LT(max_memory_usage, 1024 * 1024); // Less than 1MB per batch
}

//==============================================================================
// Test Error Handling and Edge Cases
//==============================================================================

TEST_F(QueryExecutionTest, InvalidColumnReferences) {
    // Test handling of invalid column references
    std::vector<std::string> projection_columns = {"nonexistent_column"};
    auto projector = CreateArrowColumnProjector(projection_columns);

    for (const auto& batch : batches_) {
        std::shared_ptr<arrow::RecordBatch> output_batch;
        auto status = projector->ProjectBatch(batch, &output_batch);

        // Should fail gracefully for nonexistent columns
        EXPECT_FALSE(status.ok());
        break; // Just test the first batch
    }
}

TEST_F(QueryExecutionTest, EmptyResultSets) {
    // Test queries that return no results
    std::vector<ColumnPredicate> predicates = {
        ColumnPredicate("age", PredicateType::kGreaterThan, arrow::MakeScalar<int64_t>(100))
    };

    auto evaluator = CreateArrowPredicateEvaluator(predicates);

    int64_t total_output_rows = 0;
    for (const auto& batch : batches_) {
        std::shared_ptr<arrow::RecordBatch> filtered_batch;
        evaluator->FilterBatch(batch, &filtered_batch);
        total_output_rows += filtered_batch->num_rows();
    }

    EXPECT_EQ(total_output_rows, 0); // Impossible predicate should return no rows
}

TEST_F(QueryExecutionTest, NullValueHandling) {
    // Test handling of null values in predicates
    // (Our test data doesn't have nulls, but this tests the framework)

    std::vector<ColumnPredicate> predicates = {
        ColumnPredicate("age", PredicateType::kIsNotNull, nullptr)
    };

    auto evaluator = CreateArrowPredicateEvaluator(predicates);

    int64_t total_matching_rows = 0;
    for (const auto& batch : batches_) {
        std::shared_ptr<arrow::BooleanArray> mask;
        evaluator->EvaluateBatch(batch, &mask);

        for (int64_t i = 0; i < mask->length(); ++i) {
            if (mask->Value(i)) total_matching_rows++;
        }
    }

    EXPECT_EQ(total_matching_rows, 1000); // All rows have non-null ages
}

} // namespace marble
