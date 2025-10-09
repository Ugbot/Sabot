/**
 * Test Utilities for MarbleDB
 *
 * Common utilities, fixtures, and helpers for testing
 * MarbleDB components and functionality.
 */

#pragma once

#include <gtest/gtest.h>
#include <arrow/api.h>
#include <marble/status.h>
#include <memory>
#include <vector>
#include <string>
#include <filesystem>
#include <random>

namespace fs = std::filesystem;

namespace marble {
namespace test {

//==============================================================================
// Test Fixtures and Setup
//==============================================================================

/**
 * @brief Base test fixture with common setup/teardown
 */
class MarbleTestBase : public ::testing::Test {
protected:
    void SetUp() override {
        test_path_ = "/tmp/marble_test_" + std::to_string(std::time(nullptr)) +
                    "_" + std::to_string(rand());
        fs::create_directories(test_path_);
    }

    void TearDown() override {
        if (!test_path_.empty() && fs::exists(test_path_)) {
            fs::remove_all(test_path_);
        }
    }

    std::string test_path_;
};

/**
 * @brief Test fixture for Arrow-based tests
 */
class ArrowTestBase : public MarbleTestBase {
protected:
    void SetUp() override {
        MarbleTestBase::SetUp();
        CreateTestSchema();
    }

    void CreateTestSchema() {
        // Default test schema
        schema_ = arrow::schema({
            arrow::field("id", arrow::int64()),
            arrow::field("name", arrow::utf8()),
            arrow::field("value", arrow::float64()),
            arrow::field("category", arrow::utf8()),
            arrow::field("timestamp", arrow::timestamp(arrow::TimeUnit::MICRO))
        });
    }

    std::shared_ptr<arrow::Schema> schema_;
    std::vector<std::shared_ptr<arrow::RecordBatch>> test_batches_;
};

//==============================================================================
// Test Data Generators
//==============================================================================

/**
 * @brief Generate random test data for Arrow RecordBatches
 */
class TestDataGenerator {
public:
    explicit TestDataGenerator(std::shared_ptr<arrow::Schema> schema)
        : schema_(std::move(schema)), gen_(rd_()) {}

    /**
     * @brief Generate a RecordBatch with random data
     */
    std::shared_ptr<arrow::RecordBatch> GenerateBatch(int64_t num_rows);

    /**
     * @brief Generate multiple RecordBatches
     */
    std::vector<std::shared_ptr<arrow::RecordBatch>> GenerateBatches(
        int64_t num_batches, int64_t rows_per_batch);

private:
    std::shared_ptr<arrow::Schema> schema_;
    std::random_device rd_;
    std::mt19937 gen_;

    // Distribution generators for different types
    std::uniform_int_distribution<int64_t> int64_dist_{-1000, 1000};
    std::uniform_real_distribution<double> double_dist_{-1000.0, 1000.0};
    std::uniform_int_distribution<int> string_len_dist_{1, 20};
    std::uniform_int_distribution<int> choice_dist_{0, 9};

    std::string GenerateRandomString(int length);
    int64_t GenerateTimestamp();
};

/**
 * @brief Generate specific patterns of test data
 */
class PatternedDataGenerator : public TestDataGenerator {
public:
    explicit PatternedDataGenerator(std::shared_ptr<arrow::Schema> schema)
        : TestDataGenerator(schema) {}

    /**
     * @brief Generate data with specific patterns for testing predicates
     */
    std::shared_ptr<arrow::RecordBatch> GeneratePatternedBatch(
        int64_t num_rows,
        const std::string& pattern = "uniform"); // "uniform", "clustered", "skewed"

    /**
     * @brief Generate data that will match specific predicates
     */
    std::shared_ptr<arrow::RecordBatch> GenerateMatchingBatch(
        int64_t num_rows,
        const std::vector<std::pair<std::string, std::string>>& match_patterns);
};

//==============================================================================
// Performance Measurement Utilities
//==============================================================================

/**
 * @brief Simple performance timer for tests
 */
class PerformanceTimer {
public:
    void Start() { start_time_ = std::chrono::high_resolution_clock::now(); }
    void Stop() { end_time_ = std::chrono::high_resolution_clock::now(); }

    template<typename Duration = std::chrono::milliseconds>
    typename Duration::rep Elapsed() const {
        return std::chrono::duration_cast<Duration>(end_time_ - start_time_).count();
    }

    double ElapsedSeconds() const {
        return std::chrono::duration_cast<std::chrono::duration<double>>(end_time_ - start_time_).count();
    }

    void Reset() {
        start_time_ = std::chrono::high_resolution_clock::time_point{};
        end_time_ = std::chrono::high_resolution_clock::time_point{};
    }

private:
    std::chrono::high_resolution_clock::time_point start_time_;
    std::chrono::high_resolution_clock::time_point end_time_;
};

/**
 * @brief Performance benchmark runner
 */
class BenchmarkRunner {
public:
    struct BenchmarkResult {
        std::string name;
        double elapsed_seconds;
        int64_t operations;
        double throughput; // operations per second

        void Print() const;
    };

    template<typename Func>
    BenchmarkResult Run(const std::string& name, int64_t operations, Func&& func) {
        PerformanceTimer timer;
        timer.Start();

        func(); // Execute the benchmark function

        timer.Stop();

        BenchmarkResult result;
        result.name = name;
        result.elapsed_seconds = timer.ElapsedSeconds();
        result.operations = operations;
        result.throughput = operations / result.elapsed_seconds;

        return result;
    }

    template<typename Func>
    std::vector<BenchmarkResult> RunMultiple(
        const std::vector<std::string>& names,
        const std::vector<int64_t>& operation_counts,
        const std::vector<Func>& funcs) {

        std::vector<BenchmarkResult> results;
        for (size_t i = 0; i < names.size(); ++i) {
            results.push_back(Run(names[i], operation_counts[i], funcs[i]));
        }
        return results;
    }
};

//==============================================================================
// Memory Usage Measurement
//==============================================================================

/**
 * @brief Simple memory usage tracker
 */
class MemoryTracker {
public:
    void Start() {
        initial_memory_ = GetCurrentMemoryUsage();
    }

    void Stop() {
        final_memory_ = GetCurrentMemoryUsage();
    }

    size_t PeakMemoryUsage() const {
        return final_memory_ - initial_memory_;
    }

    void Reset() {
        initial_memory_ = 0;
        final_memory_ = 0;
    }

private:
    size_t initial_memory_ = 0;
    size_t final_memory_ = 0;

    // Platform-specific memory usage (simplified)
    size_t GetCurrentMemoryUsage() {
        // This is a simplified implementation
        // In production, would use platform-specific APIs like getrusage, /proc/self/statm, etc.
        return 0; // Placeholder
    }
};

//==============================================================================
// Test Data Validators
//==============================================================================

/**
 * @brief Validate RecordBatch contents
 */
class RecordBatchValidator {
public:
    static bool ValidateSchema(const std::shared_ptr<arrow::RecordBatch>& batch,
                              const std::shared_ptr<arrow::Schema>& expected_schema);

    static bool ValidateRowCount(const std::shared_ptr<arrow::RecordBatch>& batch,
                                int64_t expected_rows);

    static bool ValidateColumnValues(const std::shared_ptr<arrow::RecordBatch>& batch,
                                    const std::string& column_name,
                                    const std::function<bool(const std::shared_ptr<arrow::Scalar>&)>& validator);

    static bool ValidateNoNulls(const std::shared_ptr<arrow::RecordBatch>& batch,
                               const std::string& column_name);

    static bool ValidateUniqueValues(const std::shared_ptr<arrow::RecordBatch>& batch,
                                    const std::string& column_name);
};

/**
 * @brief Validate query results
 */
class QueryResultValidator {
public:
    static bool ValidateResultCount(int64_t actual_count, int64_t expected_count,
                                   double tolerance = 0.0);

    static bool ValidateColumnProjection(const std::shared_ptr<arrow::RecordBatch>& batch,
                                       const std::vector<std::string>& expected_columns);

    static bool ValidatePredicateApplication(const std::shared_ptr<arrow::RecordBatch>& batch,
                                           const std::string& column_name,
                                           const std::function<bool(const std::shared_ptr<arrow::Scalar>&)>& predicate);
};

//==============================================================================
// Test Macros and Helpers
//==============================================================================

/**
 * @brief Assert that a Status is OK, with detailed error message
 */
#define ASSERT_STATUS_OK(status) \
    do { \
        auto s = (status); \
        ASSERT_TRUE(s.ok()) << "Status not OK: " << s.ToString(); \
    } while (0)

/**
 * @brief Expect that a Status is OK
 */
#define EXPECT_STATUS_OK(status) \
    do { \
        auto s = (status); \
        EXPECT_TRUE(s.ok()) << "Status not OK: " << s.ToString(); \
    } while (0)

/**
 * @brief Measure execution time of a block
 */
#define MEASURE_TIME(timer, block) \
    do { \
        (timer).Start(); \
        block \
        (timer).Stop(); \
    } while (0)

/**
 * @brief Benchmark a function and print results
 */
#define BENCHMARK_FUNCTION(runner, name, operations, func) \
    do { \
        auto result = (runner).Run(name, operations, func); \
        result.Print(); \
    } while (0)

//==============================================================================
// Common Test Constants
//==============================================================================

namespace constants {
    const int64_t SMALL_BATCH_SIZE = 100;
    const int64_t MEDIUM_BATCH_SIZE = 1000;
    const int64_t LARGE_BATCH_SIZE = 10000;

    const int64_t SMALL_DATASET_ROWS = 1000;
    const int64_t MEDIUM_DATASET_ROWS = 10000;
    const int64_t LARGE_DATASET_ROWS = 100000;

    const std::vector<std::string> COMMON_COLUMN_NAMES = {
        "id", "name", "value", "category", "timestamp", "price", "quantity"
    };

    const std::vector<std::string> TEST_CATEGORIES = {
        "Electronics", "Clothing", "Books", "Home", "Sports"
    };
} // namespace constants

//==============================================================================
// Factory Functions for Test Objects
//==============================================================================

/**
 * @brief Create common test schemas
 */
class TestSchemaFactory {
public:
    static std::shared_ptr<arrow::Schema> CreateEmployeeSchema();
    static std::shared_ptr<arrow::Schema> CreateEcommerceSchema();
    static std::shared_ptr<arrow::Schema> CreateTimeSeriesSchema();
    static std::shared_ptr<arrow::Schema> CreateSimpleSchema();
};

/**
 * @brief Create common test predicates
 */
class TestPredicateFactory {
public:
    static std::vector<ColumnPredicate> CreateNumericPredicates(
        const std::string& column_name, double min_val, double max_val);

    static std::vector<ColumnPredicate> CreateStringPredicates(
        const std::string& column_name, const std::vector<std::string>& values);

    static std::vector<ColumnPredicate> CreateComplexPredicates();
};

//==============================================================================
// Test Data Persistence
//==============================================================================

/**
 * @brief Save/load test data for debugging and reproducibility
 */
class TestDataPersistence {
public:
    static Status SaveBatchToFile(const std::shared_ptr<arrow::RecordBatch>& batch,
                                 const std::string& filepath);

    static Status LoadBatchFromFile(const std::string& filepath,
                                   std::shared_ptr<arrow::RecordBatch>* batch);

    static Status SaveTestData(const std::vector<std::shared_ptr<arrow::RecordBatch>>& batches,
                              const std::string& base_path);

    static Status LoadTestData(const std::string& base_path,
                              std::vector<std::shared_ptr<arrow::RecordBatch>>* batches);
};

} // namespace test
} // namespace marble
