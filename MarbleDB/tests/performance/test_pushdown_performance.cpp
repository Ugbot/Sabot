/**
 * Performance Tests for Pushdown Operations
 *
 * Measures the performance impact of projection and predicate pushdown,
 * testing scalability and efficiency improvements.
 */

#include <gtest/gtest.h>
#include <marble/sstable_arrow.h>
#include <benchmark/benchmark.h>
#include <chrono>
#include <vector>
#include <memory>
#include <random>

namespace marble {

//==============================================================================
// Performance Test Fixtures
//==============================================================================

class PushdownPerformanceTest : public ::testing::Test {
protected:
    void SetUp() override {
        CreateLargeTestData();
    }

    void TearDown() override {}

    void CreateLargeTestData() {
        // Create a large dataset for performance testing
        schema_ = arrow::schema({
            arrow::field("id", arrow::int64()),
            arrow::field("user_id", arrow::int64()),
            arrow::field("product_id", arrow::int64()),
            arrow::field("category", arrow::utf8()),
            arrow::field("price", arrow::float64()),
            arrow::field("quantity", arrow::int32()),
            arrow::field("timestamp", arrow::timestamp(arrow::TimeUnit::MICRO)),
            arrow::field("region", arrow::utf8()),
            arrow::field("discount", arrow::float32()),
            arrow::field("tax_rate", arrow::float64())
        });

        // Generate 100,000 rows of realistic e-commerce data
        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_int_distribution<> user_dist(1, 10000);
        std::uniform_int_distribution<> product_dist(1, 5000);
        std::uniform_real_distribution<> price_dist(10.0, 1000.0);
        std::uniform_int_distribution<> quantity_dist(1, 50);
        std::uniform_real_distribution<> discount_dist(0.0, 0.3);
        std::uniform_real_distribution<> tax_dist(0.05, 0.12);

        std::vector<std::string> categories = {"Electronics", "Clothing", "Books", "Home", "Sports", "Beauty", "Toys", "Automotive"};
        std::vector<std::string> regions = {"North", "South", "East", "West", "Central"};

        const int64_t batch_size = 10000; // 10k rows per batch
        const int64_t num_batches = 10;   // Total 100k rows

        for (int64_t batch_idx = 0; batch_idx < num_batches; ++batch_idx) {
            arrow::Int64Builder id_builder, user_id_builder, product_id_builder, quantity_builder;
            arrow::StringBuilder category_builder, region_builder;
            arrow::DoubleBuilder price_builder, tax_rate_builder;
            arrow::FloatBuilder discount_builder;
            arrow::TimestampBuilder timestamp_builder(arrow::timestamp(arrow::TimeUnit::MICRO), arrow::default_memory_pool());

            for (int64_t i = 0; i < batch_size; ++i) {
                int64_t global_id = batch_idx * batch_size + i;

                id_builder.Append(global_id);
                user_id_builder.Append(user_dist(gen));
                product_id_builder.Append(product_dist(gen));
                category_builder.Append(categories[global_id % categories.size()]);
                price_builder.Append(price_dist(gen));
                quantity_builder.Append(quantity_dist(gen));
                region_builder.Append(regions[global_id % regions.size()]);
                discount_builder.Append(discount_dist(gen));
                tax_rate_builder.Append(tax_dist(gen));

                // Timestamp: last 365 days
                auto timestamp = std::chrono::system_clock::now() -
                               std::chrono::hours(24 * (global_id % 365));
                auto micros = std::chrono::duration_cast<std::chrono::microseconds>(
                    timestamp.time_since_epoch()).count();
                timestamp_builder.Append(micros);
            }

            std::shared_ptr<arrow::Array> arrays[10];
            id_builder.Finish(&arrays[0]);
            user_id_builder.Finish(&arrays[1]);
            product_id_builder.Finish(&arrays[2]);
            category_builder.Finish(&arrays[3]);
            price_builder.Finish(&arrays[4]);
            quantity_builder.Finish(&arrays[5]);
            timestamp_builder.Finish(&arrays[6]);
            region_builder.Finish(&arrays[7]);
            discount_builder.Finish(&arrays[8]);
            tax_rate_builder.Finish(&arrays[9]);

            auto batch = arrow::RecordBatch::Make(schema_, batch_size, arrays);
            batches_.push_back(batch);
        }

        total_rows_ = batch_size * num_batches;
    }

    std::shared_ptr<arrow::Schema> schema_;
    std::vector<std::shared_ptr<arrow::RecordBatch>> batches_;
    int64_t total_rows_;
};

//==============================================================================
// Performance Benchmarks
//==============================================================================

TEST_F(PushdownPerformanceTest, ProjectionPushdownPerformance) {
    // Benchmark: SELECT id, price, quantity FROM large_table (3/10 columns)

    std::vector<std::string> projection_columns = {"id", "price", "quantity"};
    auto projector = CreateArrowColumnProjector(projection_columns);

    auto start_time = std::chrono::high_resolution_clock::now();

    int64_t total_output_rows = 0;
    size_t total_output_bytes = 0;

    for (const auto& batch : batches_) {
        std::shared_ptr<arrow::RecordBatch> projected_batch;
        auto status = projector->ProjectBatch(batch, &projected_batch);
        ASSERT_TRUE(status.ok());

        total_output_rows += projected_batch->num_rows();

        // Estimate memory usage
        for (int i = 0; i < projected_batch->num_columns(); ++i) {
            auto array = projected_batch->column(i);
            total_output_bytes += array->nbytes();
        }
    }

    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);

    EXPECT_EQ(total_output_rows, total_rows_);

    std::cout << "Projection Performance:" << std::endl;
    std::cout << "  Input: " << total_rows_ << " rows × 10 columns" << std::endl;
    std::cout << "  Output: " << total_output_rows << " rows × 3 columns" << std::endl;
    std::cout << "  Time: " << duration.count() << "ms" << std::endl;
    std::cout << "  Throughput: " << (total_rows_ / duration.count()) << " rows/ms" << std::endl;
    std::cout << "  Reduction: 70% fewer columns processed" << std::endl;
}

TEST_F(PushdownPerformanceTest, PredicatePushdownPerformance) {
    // Benchmark: SELECT * FROM large_table WHERE price > 500 AND category = 'Electronics'

    std::vector<ColumnPredicate> predicates = {
        ColumnPredicate("price", PredicateType::kGreaterThan, arrow::MakeScalar<double>(500.0)),
        ColumnPredicate("category", PredicateType::kEqual, arrow::MakeScalar<std::string>("Electronics"))
    };

    auto evaluator = CreateArrowPredicateEvaluator(predicates);

    auto start_time = std::chrono::high_resolution_clock::now();

    int64_t total_matching_rows = 0;

    for (const auto& batch : batches_) {
        std::shared_ptr<arrow::RecordBatch> filtered_batch;
        auto status = evaluator->FilterBatch(batch, &filtered_batch);
        ASSERT_TRUE(status.ok());

        total_matching_rows += filtered_batch->num_rows();
    }

    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);

    double selectivity = static_cast<double>(total_matching_rows) / total_rows_;

    std::cout << "Predicate Pushdown Performance:" << std::endl;
    std::cout << "  Query: price > 500 AND category = 'Electronics'" << std::endl;
    std::cout << "  Input: " << total_rows_ << " rows" << std::endl;
    std::cout << "  Output: " << total_matching_rows << " matching rows" << std::endl;
    std::cout << "  Selectivity: " << (selectivity * 100) << "%" << std::endl;
    std::cout << "  Time: " << duration.count() << "ms" << std::endl;
    std::cout << "  Throughput: " << (total_rows_ / duration.count()) << " rows/ms" << std::endl;
    std::cout << "  Reduction: " << ((1.0 - selectivity) * 100) << "% fewer rows processed" << std::endl;
}

TEST_F(PushdownPerformanceTest, CombinedPushdownPerformance) {
    // Benchmark: SELECT id, user_id, price FROM large_table
    // WHERE price > 100 AND quantity > 10 AND region = 'North'

    std::vector<std::string> projection_columns = {"id", "user_id", "price"};
    std::vector<ColumnPredicate> predicates = {
        ColumnPredicate("price", PredicateType::kGreaterThan, arrow::MakeScalar<double>(100.0)),
        ColumnPredicate("quantity", PredicateType::kGreaterThan, arrow::MakeScalar<int32_t>(10)),
        ColumnPredicate("region", PredicateType::kEqual, arrow::MakeScalar<std::string>("North"))
    };

    auto evaluator = CreateArrowPredicateEvaluator(predicates);
    auto projector = CreateArrowColumnProjector(projection_columns);

    auto start_time = std::chrono::high_resolution_clock::now();

    int64_t total_output_rows = 0;
    size_t total_output_bytes = 0;

    for (const auto& batch : batches_) {
        // Apply filtering first
        std::shared_ptr<arrow::RecordBatch> filtered_batch;
        auto eval_status = evaluator->FilterBatch(batch, &filtered_batch);
        ASSERT_TRUE(eval_status.ok());

        // Then apply projection
        std::shared_ptr<arrow::RecordBatch> final_batch;
        auto proj_status = projector->ProjectBatch(filtered_batch, &final_batch);
        ASSERT_TRUE(proj_status.ok());

        total_output_rows += final_batch->num_rows();

        // Estimate memory usage
        for (int i = 0; i < final_batch->num_columns(); ++i) {
            auto array = final_batch->column(i);
            total_output_bytes += array->nbytes();
        }
    }

    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);

    double row_reduction = 1.0 - (static_cast<double>(total_output_rows) / total_rows_);
    double col_reduction = 1.0 - (3.0 / 10.0); // 3 out of 10 columns

    std::cout << "Combined Pushdown Performance:" << std::endl;
    std::cout << "  Query: SELECT id, user_id, price WHERE price > 100 AND quantity > 10 AND region = 'North'" << std::endl;
    std::cout << "  Input: " << total_rows_ << " rows × 10 columns" << std::endl;
    std::cout << "  Output: " << total_output_rows << " rows × 3 columns" << std::endl;
    std::cout << "  Row reduction: " << (row_reduction * 100) << "%" << std::endl;
    std::cout << "  Column reduction: " << (col_reduction * 100) << "%" << std::endl;
    std::cout << "  Time: " << duration.count() << "ms" << std::endl;
    std::cout << "  Throughput: " << (total_rows_ / duration.count()) << " rows/ms" << std::endl;
}

TEST_F(PushdownPerformanceTest, ColumnStatisticsBuildPerformance) {
    // Benchmark building column statistics for query optimization

    auto start_time = std::chrono::high_resolution_clock::now();

    // Build statistics for all columns
    std::vector<std::string> column_names = {"price", "quantity", "discount", "tax_rate"};

    for (const auto& col_name : column_names) {
        ColumnStatistics stats(col_name);

        std::vector<std::shared_ptr<arrow::Array>> chunks;
        for (const auto& batch : batches_) {
            chunks.push_back(batch->GetColumnByName(col_name));
        }

        auto chunked_array = std::make_shared<arrow::ChunkedArray>(chunks);
        auto status = BuildColumnStatistics(chunked_array, col_name, &stats);
        ASSERT_TRUE(status.ok());

        // Verify statistics are reasonable
        if (col_name == "price") {
            EXPECT_GE(stats.min_value->ViewAs<arrow::DoubleScalar>()->value, 10.0);
            EXPECT_LE(stats.max_value->ViewAs<arrow::DoubleScalar>()->value, 1000.0);
        }
    }

    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);

    std::cout << "Column Statistics Build Performance:" << std::endl;
    std::cout << "  Columns analyzed: " << column_names.size() << std::endl;
    std::cout << "  Rows analyzed: " << total_rows_ << std::endl;
    std::cout << "  Time: " << duration.count() << "ms" << std::endl;
    std::cout << "  Throughput: " << (total_rows_ * column_names.size() / duration.count()) << " cells/ms" << std::endl;
}

TEST_F(PushdownPerformanceTest, ScalabilityTest) {
    // Test how performance scales with different data sizes

    std::vector<int64_t> test_sizes = {1000, 10000, 50000, 100000};

    for (int64_t test_size : test_sizes) {
        // Create subset of data
        int64_t batches_needed = (test_size + 9999) / 10000; // Round up
        std::vector<std::shared_ptr<arrow::RecordBatch>> test_batches(
            batches_.begin(), batches_.begin() + std::min(batches_needed, (int64_t)batches_.size()));

        // Run a simple query
        std::vector<ColumnPredicate> predicates = {
            ColumnPredicate("price", PredicateType::kGreaterThan, arrow::MakeScalar<double>(200.0))
        };

        auto evaluator = CreateArrowPredicateEvaluator(predicates);

        auto start_time = std::chrono::high_resolution_clock::now();

        int64_t matching_rows = 0;
        for (const auto& batch : test_batches) {
            std::shared_ptr<arrow::RecordBatch> filtered_batch;
            evaluator->FilterBatch(batch, &filtered_batch);
            matching_rows += filtered_batch->num_rows();
        }

        auto end_time = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);

        double throughput = static_cast<double>(test_size) / duration.count() * 1000; // rows/second

        std::cout << "Scalability Test (" << test_size << " rows):" << std::endl;
        std::cout << "  Matching rows: " << matching_rows << std::endl;
        std::cout << "  Time: " << duration.count() << " microseconds" << std::endl;
        std::cout << "  Throughput: " << throughput << " rows/second" << std::endl;
    }
}

//==============================================================================
// Memory Usage Tests
//==============================================================================

TEST_F(PushdownPerformanceTest, MemoryEfficiencyTest) {
    // Test that pushdown operations don't create excessive memory overhead

    std::vector<std::string> projection_columns = {"id", "price"}; // Only 2 columns
    std::vector<ColumnPredicate> predicates = {
        ColumnPredicate("price", PredicateType::kGreaterThan, arrow::MakeScalar<double>(100.0))
    };

    auto evaluator = CreateArrowPredicateEvaluator(predicates);
    auto projector = CreateArrowColumnProjector(projection_columns);

    size_t max_memory_per_batch = 0;
    int64_t total_output_rows = 0;

    for (const auto& batch : batches_) {
        // Measure memory before processing
        size_t mem_before = 0; // In real test, would use memory profiler

        std::shared_ptr<arrow::RecordBatch> filtered_batch;
        evaluator->FilterBatch(batch, &filtered_batch);

        std::shared_ptr<arrow::RecordBatch> final_batch;
        projector->ProjectBatch(filtered_batch, &final_batch);

        // Estimate memory used by final batch
        size_t batch_memory = 0;
        for (int i = 0; i < final_batch->num_columns(); ++i) {
            batch_memory += final_batch->column(i)->nbytes();
        }

        max_memory_per_batch = std::max(max_memory_per_batch, batch_memory);
        total_output_rows += final_batch->num_rows();
    }

    // With pushdown, we should only keep 2 columns of filtered data in memory
    size_t expected_max_memory = 10000 * 2 * 8; // Rough estimate: 10k rows × 2 cols × 8 bytes

    std::cout << "Memory Efficiency Test:" << std::endl;
    std::cout << "  Output rows: " << total_output_rows << std::endl;
    std::cout << "  Max memory per batch: " << max_memory_per_batch << " bytes" << std::endl;
    std::cout << "  Expected max: ~" << expected_max_memory << " bytes" << std::endl;
    std::cout << "  Memory reduction: ~80% (from original 10 columns)" << std::endl;

    EXPECT_LT(max_memory_per_batch, expected_max_memory * 2); // Allow some overhead
}

//==============================================================================
// Comparative Performance Tests
//==============================================================================

TEST_F(PushdownPerformanceTest, PushdownVsNoPushdownComparison) {
    // Compare performance with and without pushdown

    std::vector<ColumnPredicate> predicates = {
        ColumnPredicate("price", PredicateType::kGreaterThan, arrow::MakeScalar<double>(300.0)),
        ColumnPredicate("quantity", PredicateType::kLessThan, arrow::MakeScalar<int32_t>(25))
    };

    std::vector<std::string> projection_columns = {"id", "price", "quantity"};

    // Method 1: With pushdown
    auto evaluator = CreateArrowPredicateEvaluator(predicates);
    auto projector = CreateArrowColumnProjector(projection_columns);

    auto pushdown_start = std::chrono::high_resolution_clock::now();

    int64_t pushdown_rows = 0;
    for (const auto& batch : batches_) {
        std::shared_ptr<arrow::RecordBatch> filtered_batch;
        evaluator->FilterBatch(batch, &filtered_batch);

        std::shared_ptr<arrow::RecordBatch> final_batch;
        projector->ProjectBatch(filtered_batch, &final_batch);

        pushdown_rows += final_batch->num_rows();
    }

    auto pushdown_end = std::chrono::high_resolution_clock::now();
    auto pushdown_duration = std::chrono::duration_cast<std::chrono::milliseconds>(pushdown_end - pushdown_start);

    // Method 2: Without pushdown (simulate traditional approach)
    auto traditional_start = std::chrono::high_resolution_clock::now();

    int64_t traditional_rows = 0;
    for (const auto& batch : batches_) {
        // Simulate manual filtering and projection
        auto price_array = std::static_pointer_cast<arrow::DoubleArray>(batch->GetColumnByName("price"));
        auto quantity_array = std::static_pointer_cast<arrow::Int32Array>(batch->GetColumnByName("quantity"));

        int64_t matching_in_batch = 0;
        for (int64_t i = 0; i < batch->num_rows(); ++i) {
            bool matches = price_array->Value(i) > 300.0 && quantity_array->Value(i) < 25;
            if (matches) matching_in_batch++;
        }

        traditional_rows += matching_in_batch;
    }

    auto traditional_end = std::chrono::high_resolution_clock::now();
    auto traditional_duration = std::chrono::duration_cast<std::chrono::milliseconds>(traditional_end - traditional_start);

    // Results should be the same
    EXPECT_EQ(pushdown_rows, traditional_rows);

    double speedup = static_cast<double>(traditional_duration.count()) / pushdown_duration.count();

    std::cout << "Pushdown vs Traditional Performance Comparison:" << std::endl;
    std::cout << "  Matching rows: " << pushdown_rows << std::endl;
    std::cout << "  With pushdown: " << pushdown_duration.count() << "ms" << std::endl;
    std::cout << "  Traditional: " << traditional_duration.count() << "ms" << std::endl;
    std::cout << "  Speedup: " << speedup << "x" << std::endl;
    std::cout << "  Efficiency gain: " << ((speedup - 1.0) * 100) << "%" << std::endl;
}

} // namespace marble
