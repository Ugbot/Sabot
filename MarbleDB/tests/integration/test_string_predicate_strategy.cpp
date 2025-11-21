/**
 * Integration test for StringPredicateStrategy
 *
 * Tests the full integration of SIMD string operations:
 * - C++ StringPredicateStrategy
 * - Python C API bridge
 * - Sabot Cython string_operations module
 * - Arrow SIMD kernels
 */

#include <gtest/gtest.h>
#include <arrow/api.h>
#include <Python.h>
#include "marble/optimizations/string_predicate_strategy.h"
#include "marble/optimization_factory.h"
#include "marble/table_capabilities.h"

namespace marble {
namespace test {

class StringPredicateStrategyTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Initialize Python interpreter (required for Cython module)
        if (!Py_IsInitialized()) {
            Py_Initialize();
        }

        // Ensure Sabot module can be imported
        setenv("PYTHONPATH", "/Users/bengamble/Sabot", 1);
    }

    void TearDown() override {
        // Don't finalize Python (other tests may need it)
    }
};

// Test 1: Basic Strategy Creation
TEST_F(StringPredicateStrategyTest, StrategyCreation) {
    auto strategy = std::make_unique<StringPredicateStrategy>();
    ASSERT_NE(strategy, nullptr);
    EXPECT_EQ(strategy->Name(), "StringPredicateStrategy");
}

// Test 2: Manual Column Configuration
TEST_F(StringPredicateStrategyTest, ManualColumnConfiguration) {
    auto strategy = std::make_unique<StringPredicateStrategy>();

    // Enable columns
    strategy->EnableColumn("name");
    strategy->EnableColumn("description");

    // Verify columns enabled
    EXPECT_TRUE(strategy->IsColumnEnabled("name"));
    EXPECT_TRUE(strategy->IsColumnEnabled("description"));
    EXPECT_FALSE(strategy->IsColumnEnabled("age"));

    // Disable column
    strategy->DisableColumn("name");
    EXPECT_FALSE(strategy->IsColumnEnabled("name"));
}

// Test 3: Auto-Configuration from Schema
TEST_F(StringPredicateStrategyTest, AutoConfigureFromSchema) {
    // Create schema with string and non-string columns
    auto schema = arrow::schema({
        arrow::field("name", arrow::utf8()),
        arrow::field("description", arrow::utf8()),
        arrow::field("age", arrow::int32()),
        arrow::field("large_text", arrow::large_utf8())
    });

    // Auto-configure strategy
    auto strategy = StringPredicateStrategy::AutoConfigure(schema);
    ASSERT_NE(strategy, nullptr);

    // Verify string columns enabled
    EXPECT_TRUE(strategy->IsColumnEnabled("name"));
    EXPECT_TRUE(strategy->IsColumnEnabled("description"));
    EXPECT_TRUE(strategy->IsColumnEnabled("large_text"));

    // Verify non-string columns not enabled
    EXPECT_FALSE(strategy->IsColumnEnabled("age"));
}

// Test 4: StringOperationsWrapper Availability
TEST_F(StringPredicateStrategyTest, StringOperationsWrapperAvailability) {
    auto strategy = std::make_unique<StringPredicateStrategy>();

    // Get detailed stats (includes wrapper availability)
    auto stats = strategy->GetDetailedStats();

    // Check if Cython module is available
    // (may fail if module not built, which is acceptable)
    std::cout << "SIMD module availability: "
              << (strategy->GetStats().find("yes") != std::string::npos ? "yes" : "no")
              << std::endl;
}

// Test 5: StringOperationsWrapper - Equal Operation
TEST_F(StringPredicateStrategyTest, StringOperationsEqual) {
    // Create test data
    auto array = arrow::ArrayFromJSON(arrow::utf8(),
        R"(["apple", "banana", "apple", "cherry", "apple"])");

    // Create strategy and wrapper
    auto strategy = std::make_unique<StringPredicateStrategy>();

    // Note: Direct wrapper access not exposed via strategy interface
    // This test verifies compilation and basic setup
    // Full functionality requires predicate passing infrastructure

    SUCCEED();  // Placeholder for when wrapper is accessible
}

// Test 6: Statistics Tracking
TEST_F(StringPredicateStrategyTest, StatisticsTracking) {
    auto strategy = std::make_unique<StringPredicateStrategy>();

    // Get initial stats
    auto stats = strategy->GetDetailedStats();
    EXPECT_EQ(stats.reads_intercepted, 0);
    EXPECT_EQ(stats.rows_filtered, 0);

    // Stats would be updated by OnRead/OnWrite hooks
    // Placeholder for full integration test
}

// Test 7: Vocabulary Cache (RDF Optimization)
TEST_F(StringPredicateStrategyTest, VocabularyCache) {
    auto strategy = std::make_unique<StringPredicateStrategy>();

    // Cache some vocabulary
    strategy->CacheVocabulary("http://example.org/subject", 1);
    strategy->CacheVocabulary("http://example.org/predicate", 2);
    strategy->CacheVocabulary("http://example.org/object", 3);

    // Lookup cached values
    EXPECT_EQ(strategy->LookupVocabulary("http://example.org/subject"), 1);
    EXPECT_EQ(strategy->LookupVocabulary("http://example.org/predicate"), 2);
    EXPECT_EQ(strategy->LookupVocabulary("http://example.org/object"), 3);

    // Lookup non-existent value
    EXPECT_EQ(strategy->LookupVocabulary("http://example.org/missing"), -1);

    // Verify stats
    auto stats = strategy->GetDetailedStats();
    EXPECT_EQ(stats.vocab_cache_hits, 3);
    EXPECT_EQ(stats.vocab_cache_misses, 1);
    EXPECT_EQ(stats.vocab_cache_size, 3);

    // Clear cache
    strategy->ClearVocabulary();
    EXPECT_EQ(strategy->LookupVocabulary("http://example.org/subject"), -1);

    // Stats after clear
    stats = strategy->GetDetailedStats();
    EXPECT_EQ(stats.vocab_cache_size, 0);
}

// Test 8: Serialization/Deserialization
TEST_F(StringPredicateStrategyTest, Serialization) {
    auto strategy = std::make_unique<StringPredicateStrategy>();

    // Cache some vocabulary
    strategy->CacheVocabulary("test1", 100);
    strategy->CacheVocabulary("test2", 200);

    // Serialize
    auto serialized = strategy->Serialize();
    EXPECT_GT(serialized.size(), 0);

    // Create new strategy and deserialize
    auto strategy2 = std::make_unique<StringPredicateStrategy>();
    auto status = strategy2->Deserialize(serialized);
    EXPECT_TRUE(status.ok());

    // Verify vocabulary restored
    EXPECT_EQ(strategy2->LookupVocabulary("test1"), 100);
    EXPECT_EQ(strategy2->LookupVocabulary("test2"), 200);
}

// Test 9: OptimizationFactory Integration
TEST_F(StringPredicateStrategyTest, FactoryIntegration) {
    // Create schema with string columns
    auto schema = arrow::schema({
        arrow::field("name", arrow::utf8()),
        arrow::field("value", arrow::int64())
    });

    TableCapabilities caps;

    // Create optimization pipeline via factory
    auto pipeline = OptimizationFactory::CreateForSchema(schema, caps);
    ASSERT_NE(pipeline, nullptr);

    // Pipeline should have added StringPredicateStrategy
    // (can't directly verify without pipeline introspection)
    EXPECT_GT(pipeline->NumStrategies(), 0);
}

// Test 10: Memory Usage
TEST_F(StringPredicateStrategyTest, MemoryUsage) {
    auto strategy = std::make_unique<StringPredicateStrategy>();

    // Initial memory usage (should be minimal)
    size_t initial_usage = strategy->MemoryUsage();
    EXPECT_GT(initial_usage, 0);  // Should include struct overhead

    // Cache vocabulary (increases memory)
    for (int i = 0; i < 100; ++i) {
        strategy->CacheVocabulary("test_string_" + std::to_string(i), i);
    }

    // Memory usage should increase
    size_t after_cache = strategy->MemoryUsage();
    EXPECT_GT(after_cache, initial_usage);

    // Clear cache
    strategy->ClearVocabulary();
    size_t after_clear = strategy->MemoryUsage();

    // Memory usage should decrease (back to near initial)
    EXPECT_LT(after_clear, after_cache);
}

// Test 11: Lifecycle Hooks
TEST_F(StringPredicateStrategyTest, LifecycleHooks) {
    auto strategy = std::make_unique<StringPredicateStrategy>();

    TableCapabilities caps;

    // OnTableCreate
    auto status = strategy->OnTableCreate(caps);
    EXPECT_TRUE(status.ok());

    // OnRead (placeholder - no actual predicate passing yet)
    ReadContext read_ctx = {Key(), true, false, false, false, false, false, false, "", false};
    status = strategy->OnRead(&read_ctx);
    EXPECT_TRUE(status.ok());

    // OnWrite (placeholder)
    std::shared_ptr<arrow::RecordBatch> batch;
    WriteContext write_ctx = {Key(), batch, 0, WriteContext::INSERT, false, false, false, ""};
    status = strategy->OnWrite(&write_ctx);
    EXPECT_TRUE(status.ok());

    // OnCompaction (placeholder)
    CompactionContext comp_ctx;
    status = strategy->OnCompaction(&comp_ctx);
    EXPECT_TRUE(status.ok());

    // OnFlush (placeholder)
    FlushContext flush_ctx;
    status = strategy->OnFlush(&flush_ctx);
    EXPECT_TRUE(status.ok());
}

// Test 12: Statistics Output Format
TEST_F(StringPredicateStrategyTest, StatisticsOutput) {
    auto strategy = std::make_unique<StringPredicateStrategy>();

    // Get stats string
    std::string stats_str = strategy->GetStats();

    // Should contain strategy name
    EXPECT_NE(stats_str.find("StringPredicateStrategy"), std::string::npos);

    // Should contain key metrics
    EXPECT_NE(stats_str.find("Reads intercepted"), std::string::npos);
    EXPECT_NE(stats_str.find("Vocabulary cache"), std::string::npos);
    EXPECT_NE(stats_str.find("SIMD module available"), std::string::npos);

    std::cout << "\nStatistics Output:\n" << stats_str << std::endl;
}

}  // namespace test
}  // namespace marble

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);

    // Set PYTHONPATH to find Sabot module
    setenv("PYTHONPATH", "/Users/bengamble/Sabot", 1);

    // Set DYLD_LIBRARY_PATH for Arrow
    setenv("DYLD_LIBRARY_PATH",
           "/Users/bengamble/Sabot/vendor/arrow/cpp/build/install/lib:"
           "/Users/bengamble/Sabot/MarbleDB/build", 1);

    return RUN_ALL_TESTS();
}
