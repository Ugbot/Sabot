/**
 * Simple integration test for StringPredicateStrategy
 *
 * Tests basic functionality without requiring GTest.
 */

#include <iostream>
#include <arrow/api.h>
#include <Python.h>
#include "marble/optimizations/string_predicate_strategy.h"
#include "marble/optimization_factory.h"
#include "marble/table_capabilities.h"

using namespace marble;

#define ASSERT(condition, message) \
    do { \
        if (!(condition)) { \
            std::cerr << "FAIL: " << message << std::endl; \
            return 1; \
        } else { \
            std::cout << "PASS: " << message << std::endl; \
        } \
    } while (0)

int main() {
    std::cout << "=== StringPredicateStrategy Integration Test ===\n" << std::endl;

    // Initialize Python
    if (!Py_IsInitialized()) {
        Py_Initialize();
    }
    setenv("PYTHONPATH", "/Users/bengamble/Sabot", 1);

    // Test 1: Basic Strategy Creation
    std::cout << "\nTest 1: Basic Strategy Creation" << std::endl;
    {
        auto strategy = std::make_unique<StringPredicateStrategy>();
        ASSERT(strategy != nullptr, "Strategy created");
        ASSERT(strategy->Name() == "StringPredicateStrategy", "Strategy name correct");
    }

    // Test 2: Manual Column Configuration
    std::cout << "\nTest 2: Manual Column Configuration" << std::endl;
    {
        auto strategy = std::make_unique<StringPredicateStrategy>();

        strategy->EnableColumn("name");
        strategy->EnableColumn("description");

        ASSERT(strategy->IsColumnEnabled("name"), "Column 'name' enabled");
        ASSERT(strategy->IsColumnEnabled("description"), "Column 'description' enabled");
        ASSERT(!strategy->IsColumnEnabled("age"), "Column 'age' not enabled");

        strategy->DisableColumn("name");
        ASSERT(!strategy->IsColumnEnabled("name"), "Column 'name' disabled");
    }

    // Test 3: Auto-Configuration from Schema
    std::cout << "\nTest 3: Auto-Configuration from Schema" << std::endl;
    {
        auto schema = arrow::schema({
            arrow::field("name", arrow::utf8()),
            arrow::field("description", arrow::utf8()),
            arrow::field("age", arrow::int32()),
            arrow::field("large_text", arrow::large_utf8())
        });

        auto strategy = StringPredicateStrategy::AutoConfigure(schema);
        ASSERT(strategy != nullptr, "Auto-configured strategy created");
        ASSERT(strategy->IsColumnEnabled("name"), "String column 'name' enabled");
        ASSERT(strategy->IsColumnEnabled("description"), "String column 'description' enabled");
        ASSERT(strategy->IsColumnEnabled("large_text"), "Large string column enabled");
        ASSERT(!strategy->IsColumnEnabled("age"), "Non-string column not enabled");
    }

    // Test 4: Vocabulary Cache
    std::cout << "\nTest 4: Vocabulary Cache (RDF Optimization)" << std::endl;
    {
        auto strategy = std::make_unique<StringPredicateStrategy>();

        strategy->CacheVocabulary("http://example.org/subject", 1);
        strategy->CacheVocabulary("http://example.org/predicate", 2);
        strategy->CacheVocabulary("http://example.org/object", 3);

        ASSERT(strategy->LookupVocabulary("http://example.org/subject") == 1,
               "Vocabulary lookup 1");
        ASSERT(strategy->LookupVocabulary("http://example.org/predicate") == 2,
               "Vocabulary lookup 2");
        ASSERT(strategy->LookupVocabulary("http://example.org/object") == 3,
               "Vocabulary lookup 3");
        ASSERT(strategy->LookupVocabulary("http://example.org/missing") == -1,
               "Missing vocabulary returns -1");

        auto stats = strategy->GetDetailedStats();
        ASSERT(stats.vocab_cache_size == 3, "Vocabulary cache size correct");
        ASSERT(stats.vocab_cache_hits == 3, "Vocabulary cache hits tracked");
        ASSERT(stats.vocab_cache_misses == 1, "Vocabulary cache misses tracked");

        strategy->ClearVocabulary();
        ASSERT(strategy->LookupVocabulary("http://example.org/subject") == -1,
               "Vocabulary cleared");
    }

    // Test 5: Serialization/Deserialization
    std::cout << "\nTest 5: Serialization/Deserialization" << std::endl;
    {
        auto strategy1 = std::make_unique<StringPredicateStrategy>();
        strategy1->CacheVocabulary("test1", 100);
        strategy1->CacheVocabulary("test2", 200);

        auto serialized = strategy1->Serialize();
        ASSERT(serialized.size() > 0, "Serialization produces data");

        auto strategy2 = std::make_unique<StringPredicateStrategy>();
        auto status = strategy2->Deserialize(serialized);
        ASSERT(status.ok(), "Deserialization succeeds");
        ASSERT(strategy2->LookupVocabulary("test1") == 100, "Deserialized vocab 1");
        ASSERT(strategy2->LookupVocabulary("test2") == 200, "Deserialized vocab 2");
    }

    // Test 6: Memory Usage
    std::cout << "\nTest 6: Memory Usage Tracking" << std::endl;
    {
        auto strategy = std::make_unique<StringPredicateStrategy>();

        size_t initial = strategy->MemoryUsage();
        ASSERT(initial > 0, "Initial memory usage > 0");

        for (int i = 0; i < 100; ++i) {
            strategy->CacheVocabulary("string_" + std::to_string(i), i);
        }

        size_t after_cache = strategy->MemoryUsage();
        ASSERT(after_cache > initial, "Memory increases with cache");

        strategy->ClearVocabulary();
        size_t after_clear = strategy->MemoryUsage();
        ASSERT(after_clear < after_cache, "Memory decreases after clear");
    }

    // Test 7: TableCapabilities Hook (Simplified)
    std::cout << "\nTest 7: TableCapabilities Hook" << std::endl;
    {
        auto strategy = std::make_unique<StringPredicateStrategy>();

        TableCapabilities caps;
        auto status = strategy->OnTableCreate(caps);
        ASSERT(status.ok(), "OnTableCreate succeeds");

        // Note: Full lifecycle hook testing requires MarbleDB integration
        // (ReadContext, WriteContext need Key objects which require full DB setup)
        // This test verifies the strategy can be created and basic hooks work
    }

    // Test 8: Statistics Output
    std::cout << "\nTest 8: Statistics Output" << std::endl;
    {
        auto strategy = std::make_unique<StringPredicateStrategy>();

        std::string stats = strategy->GetStats();
        ASSERT(stats.find("StringPredicateStrategy") != std::string::npos,
               "Stats contains strategy name");
        ASSERT(stats.find("Reads intercepted") != std::string::npos,
               "Stats contains read metrics");
        ASSERT(stats.find("Vocabulary cache") != std::string::npos,
               "Stats contains vocabulary metrics");
        ASSERT(stats.find("SIMD module available") != std::string::npos,
               "Stats contains module availability");

        std::cout << "\nStatistics output:\n" << stats << std::endl;
    }

    // Test 9: OptimizationFactory Integration
    std::cout << "\nTest 9: OptimizationFactory Integration" << std::endl;
    {
        auto schema = arrow::schema({
            arrow::field("name", arrow::utf8()),
            arrow::field("value", arrow::int64())
        });

        TableCapabilities caps;
        auto pipeline = OptimizationFactory::CreateForSchema(schema, caps);

        ASSERT(pipeline != nullptr, "Pipeline created");
        ASSERT(pipeline->NumStrategies() > 0, "Pipeline has strategies");
    }

    // Test 10: StringOperationsWrapper Availability
    std::cout << "\nTest 10: StringOperationsWrapper Availability" << std::endl;
    {
        auto strategy = std::make_unique<StringPredicateStrategy>();
        std::string stats = strategy->GetStats();

        bool module_available = (stats.find("yes") != std::string::npos);
        std::cout << "SIMD module available: " << (module_available ? "YES" : "NO") << std::endl;

        if (module_available) {
            std::cout << "✅ Sabot string operations module successfully imported!" << std::endl;
        } else {
            std::cout << "⚠️  Sabot string operations module not available (may need build)" << std::endl;
        }
    }

    std::cout << "\n=== All Tests Passed! ===\n" << std::endl;

    return 0;
}
