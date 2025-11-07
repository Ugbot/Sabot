/**
 * Integration tests for optimization strategy auto-configuration
 *
 * Tests schema detection, auto-configuration, and end-to-end optimization behavior
 */

#include <gtest/gtest.h>
#include "marble/optimization_factory.h"
#include "marble/bloom_filter_strategy.h"
#include "marble/cache_strategy.h"
#include "marble/table_capabilities.h"
#include <arrow/api.h>

namespace marble {

//==============================================================================
// Schema Detection Tests
//==============================================================================

class SchemaDetectionTest : public ::testing::Test {
protected:
    void SetUp() override {}
};

TEST_F(SchemaDetectionTest, DetectRDFTripleSchema) {
    // Create RDF triple schema: 3 int64 columns named (s, p, o)
    auto schema = arrow::schema({
        arrow::field("s", arrow::int64()),
        arrow::field("p", arrow::int64()),
        arrow::field("o", arrow::int64())
    });

    SchemaType type = OptimizationFactory::DetectSchemaType(schema);
    EXPECT_EQ(type, SchemaType::RDF_TRIPLE);
}

TEST_F(SchemaDetectionTest, DetectRDFTripleSchemaLongNames) {
    // Create RDF triple schema with full names
    auto schema = arrow::schema({
        arrow::field("subject", arrow::int64()),
        arrow::field("predicate", arrow::int64()),
        arrow::field("object", arrow::int64())
    });

    SchemaType type = OptimizationFactory::DetectSchemaType(schema);
    EXPECT_EQ(type, SchemaType::RDF_TRIPLE);
}

TEST_F(SchemaDetectionTest, DetectKeyValueSchema) {
    // Create key-value schema: 2 columns
    auto schema = arrow::schema({
        arrow::field("key", arrow::utf8()),
        arrow::field("value", arrow::utf8())
    });

    SchemaType type = OptimizationFactory::DetectSchemaType(schema);
    EXPECT_EQ(type, SchemaType::KEY_VALUE);
}

TEST_F(SchemaDetectionTest, DetectTimeSeriesSchema) {
    // Create time-series schema with timestamp column
    auto schema = arrow::schema({
        arrow::field("timestamp", arrow::timestamp(arrow::TimeUnit::MILLI)),
        arrow::field("value", arrow::float64()),
        arrow::field("sensor_id", arrow::int32())
    });

    SchemaType type = OptimizationFactory::DetectSchemaType(schema);
    EXPECT_EQ(type, SchemaType::TIME_SERIES);
}

TEST_F(SchemaDetectionTest, DetectTimeSeriesSchemaByName) {
    // Schema with "time" in column name
    auto schema = arrow::schema({
        arrow::field("event_time", arrow::int64()),
        arrow::field("data", arrow::utf8())
    });

    SchemaType type = OptimizationFactory::DetectSchemaType(schema);
    EXPECT_EQ(type, SchemaType::TIME_SERIES);
}

TEST_F(SchemaDetectionTest, DetectPropertyGraphSchema) {
    // Schema with nested structures
    auto vertex_struct = arrow::struct_({
        arrow::field("id", arrow::int64()),
        arrow::field("label", arrow::utf8()),
        arrow::field("properties", arrow::map(arrow::utf8(), arrow::utf8()))
    });

    auto schema = arrow::schema({
        arrow::field("vertices", arrow::list(vertex_struct))
    });

    SchemaType type = OptimizationFactory::DetectSchemaType(schema);
    EXPECT_EQ(type, SchemaType::PROPERTY_GRAPH);
}

TEST_F(SchemaDetectionTest, DetectDocumentSchema) {
    // Schema with mostly string/binary columns
    auto schema = arrow::schema({
        arrow::field("title", arrow::utf8()),
        arrow::field("body", arrow::utf8()),
        arrow::field("author", arrow::utf8()),
        arrow::field("tags", arrow::list(arrow::utf8())),
        arrow::field("created_at", arrow::int64())
    });

    SchemaType type = OptimizationFactory::DetectSchemaType(schema);
    EXPECT_EQ(type, SchemaType::DOCUMENT);
}

TEST_F(SchemaDetectionTest, DetectUnknownSchema) {
    // Generic schema that doesn't match any pattern
    auto schema = arrow::schema({
        arrow::field("col1", arrow::int32()),
        arrow::field("col2", arrow::float64()),
        arrow::field("col3", arrow::boolean())
    });

    SchemaType type = OptimizationFactory::DetectSchemaType(schema);
    EXPECT_EQ(type, SchemaType::UNKNOWN);
}

//==============================================================================
// Auto-Configuration Tests
//==============================================================================

class AutoConfigurationTest : public ::testing::Test {
protected:
    void SetUp() override {
        caps_.enable_hot_key_cache = false;  // Default: cache disabled
    }

    TableCapabilities caps_;
};

TEST_F(AutoConfigurationTest, AutoConfigureForRDF) {
    // RDF triple schema
    auto schema = arrow::schema({
        arrow::field("subject", arrow::int64()),
        arrow::field("predicate", arrow::int64()),
        arrow::field("object", arrow::int64())
    });

    // Auto-configure
    WorkloadHints hints;
    auto pipeline = OptimizationFactory::CreateForSchema(schema, caps_, hints);

    ASSERT_NE(pipeline, nullptr);

    // Should have bloom filter for RDF
    auto* bloom = pipeline->GetStrategy("HashBloomFilter");
    EXPECT_NE(bloom, nullptr) << "RDF schema should get HashBloomFilter strategy";
}

TEST_F(AutoConfigurationTest, AutoConfigureForKeyValue) {
    // Key-value schema
    auto schema = arrow::schema({
        arrow::field("key", arrow::int64()),
        arrow::field("value", arrow::binary())
    });

    // Auto-configure with default hints
    WorkloadHints hints;
    auto pipeline = OptimizationFactory::CreateForSchema(schema, caps_, hints);

    ASSERT_NE(pipeline, nullptr);

    // Should have bloom filter for point lookups
    auto* bloom = pipeline->GetStrategy("HashBloomFilter");
    EXPECT_NE(bloom, nullptr);
}

TEST_F(AutoConfigurationTest, AutoConfigureKeyValueWithHotKeys) {
    // Key-value schema with hot key cache enabled
    auto schema = arrow::schema({
        arrow::field("key", arrow::utf8()),
        arrow::field("data", arrow::binary())
    });

    caps_.enable_hot_key_cache = true;

    WorkloadHints hints;
    hints.has_hot_keys = true;

    auto pipeline = OptimizationFactory::CreateForSchema(schema, caps_, hints);

    ASSERT_NE(pipeline, nullptr);

    // Should have both bloom filter AND cache
    auto* bloom = pipeline->GetStrategy("HashBloomFilter");
    auto* cache = pipeline->GetStrategy("Cache");

    EXPECT_NE(bloom, nullptr);
    EXPECT_NE(cache, nullptr) << "Hot key workload should get Cache strategy";
}

TEST_F(AutoConfigurationTest, WorkloadHintsPointLookup) {
    // Generic schema
    auto schema = arrow::schema({
        arrow::field("id", arrow::int64()),
        arrow::field("data", arrow::utf8())
    });

    // Hint: Point lookup workload
    WorkloadHints hints;
    hints.access_pattern = WorkloadHints::POINT_LOOKUP;

    auto pipeline = OptimizationFactory::CreateForSchema(schema, caps_, hints);

    ASSERT_NE(pipeline, nullptr);

    // Should have bloom filter for point lookups
    auto* bloom = pipeline->GetStrategy("HashBloomFilter");
    EXPECT_NE(bloom, nullptr);
}

TEST_F(AutoConfigurationTest, WorkloadHintsRangeScan) {
    // Time-series schema
    auto schema = arrow::schema({
        arrow::field("timestamp", arrow::timestamp(arrow::TimeUnit::SECOND)),
        arrow::field("value", arrow::float64())
    });

    // Hint: Range scan workload
    WorkloadHints hints;
    hints.access_pattern = WorkloadHints::RANGE_SCAN;

    auto pipeline = OptimizationFactory::CreateForSchema(schema, caps_, hints);

    ASSERT_NE(pipeline, nullptr);

    // Currently, SkippingIndex is not implemented, so pipeline may be empty
    // or contain default strategies
    // TODO: Add check for SkippingIndex when implemented
}

TEST_F(AutoConfigurationTest, ManualStrategySelection) {
    // Use manual configuration
    auto bloom = OptimizationFactory::CreateHashBloomFilter(100000, 0.001);
    auto cache = OptimizationFactory::CreateCache(5000, 500);

    ASSERT_NE(bloom, nullptr);
    ASSERT_NE(cache, nullptr);

    EXPECT_EQ(bloom->Name(), "HashBloomFilter");
    EXPECT_EQ(cache->Name(), "Cache");
}

//==============================================================================
// End-to-End Integration Tests
//==============================================================================

class OptimizationIntegrationTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Create RDF triple schema
        schema_ = arrow::schema({
            arrow::field("s", arrow::int64()),
            arrow::field("p", arrow::int64()),
            arrow::field("o", arrow::int64())
        });

        // Auto-configure optimization pipeline
        TableCapabilities caps;
        WorkloadHints hints;
        hints.access_pattern = WorkloadHints::POINT_LOOKUP;

        pipeline_ = OptimizationFactory::CreateForSchema(schema_, caps, hints);
        ASSERT_NE(pipeline_, nullptr);

        // Initialize pipeline
        pipeline_->OnTableCreate(caps);
    }

    std::shared_ptr<arrow::Schema> schema_;
    std::unique_ptr<OptimizationPipeline> pipeline_;
};

TEST_F(OptimizationIntegrationTest, WriteAndReadWorkflow) {
    // Simulate writing 100 triples
    for (uint64_t i = 0; i < 100; ++i) {
        MockKey key(i);
        WriteContext wctx{key, nullptr, 0};
        Status s = pipeline_->OnWrite(&wctx);
        EXPECT_TRUE(s.ok());
    }

    // Read existing keys - should pass bloom filter
    for (uint64_t i = 0; i < 100; ++i) {
        MockKey key(i);
        ReadContext rctx{key};
        Status s = pipeline_->OnRead(&rctx);
        EXPECT_TRUE(s.ok()) << "Existing key should pass bloom filter";
        EXPECT_FALSE(rctx.definitely_not_found);
    }

    // Read non-existent keys - should be filtered out
    int filtered_count = 0;
    for (uint64_t i = 1000; i < 1100; ++i) {
        MockKey key(i);
        ReadContext rctx{key};
        Status s = pipeline_->OnRead(&rctx);
        if (s.IsNotFound()) {
            filtered_count++;
        }
    }

    // Bloom filter should catch most non-existent keys
    EXPECT_GT(filtered_count, 90) << "Should filter out most non-existent keys";
}

TEST_F(OptimizationIntegrationTest, CompactionRebuildsStructures) {
    // Add some keys
    for (uint64_t i = 0; i < 50; ++i) {
        MockKey key(i);
        WriteContext wctx{key, nullptr, 0};
        pipeline_->OnWrite(&wctx);
    }

    // Simulate compaction
    CompactionContext cctx;
    cctx.level = 0;
    cctx.num_input_files = 2;
    cctx.estimated_output_size = 1024 * 1024;

    Status s = pipeline_->OnCompaction(&cctx);
    EXPECT_TRUE(s.ok());

    // Check that compaction flags were set
    EXPECT_TRUE(cctx.rebuild_bloom_filters);
}

TEST_F(OptimizationIntegrationTest, FlushSerializesMetadata) {
    // Add keys
    for (uint64_t i = 0; i < 20; ++i) {
        MockKey key(i);
        WriteContext wctx{key, nullptr, 0};
        pipeline_->OnWrite(&wctx);
    }

    // Simulate flush
    FlushContext fctx;
    fctx.memtable_batch = nullptr;
    fctx.num_records = 20;

    Status s = pipeline_->OnFlush(&fctx);
    EXPECT_TRUE(s.ok());

    // Check that metadata was generated
    EXPECT_TRUE(fctx.include_bloom_filter);
    EXPECT_FALSE(fctx.metadata.empty());

    // Verify bloom filter metadata exists
    auto it = fctx.metadata.find("bloom_filter");
    EXPECT_NE(it, fctx.metadata.end());
    EXPECT_GT(it->second.size(), 0);
}

TEST_F(OptimizationIntegrationTest, StatisticsTracking) {
    // Perform various operations
    for (uint64_t i = 0; i < 10; ++i) {
        MockKey key(i);
        WriteContext wctx{key, nullptr, 0};
        pipeline_->OnWrite(&wctx);
    }

    for (uint64_t i = 0; i < 20; ++i) {
        MockKey key(i);
        ReadContext rctx{key};
        pipeline_->OnRead(&rctx);
    }

    // Get statistics JSON
    std::string stats = pipeline_->GetStats();
    EXPECT_FALSE(stats.empty());

    // Verify JSON structure (basic check)
    EXPECT_NE(stats.find("{"), std::string::npos);
    EXPECT_NE(stats.find("}"), std::string::npos);
    EXPECT_NE(stats.find("HashBloomFilter"), std::string::npos);
}

TEST_F(OptimizationIntegrationTest, MemoryUsageReporting) {
    // Add many keys to increase memory usage
    for (uint64_t i = 0; i < 1000; ++i) {
        MockKey key(i);
        WriteContext wctx{key, nullptr, 0};
        pipeline_->OnWrite(&wctx);
    }

    size_t memory = pipeline_->MemoryUsage();
    EXPECT_GT(memory, 0);

    // Memory usage should scale with number of keys
    // (bloom filter uses ~10 bits per key)
    EXPECT_GT(memory, 1000);  // At least 1KB for 1000 keys
}

//==============================================================================
// Configuration Parameter Tests
//==============================================================================

class ConfigurationParametersTest : public ::testing::Test {};

TEST_F(ConfigurationParametersTest, HashBloomFilterParameters) {
    size_t expected_keys = 1000000;
    size_t memory_budget = 125000;  // 125KB = 1MB bits

    auto [num_bits, num_hash, fpr] = OptimizationFactory::EstimateHashBloomFilterParams(
        expected_keys, memory_budget);

    EXPECT_GT(num_bits, 0);
    EXPECT_GT(num_hash, 0);
    EXPECT_LT(num_hash, 20);  // Reasonable number of hash functions
    EXPECT_LT(fpr, 0.1);  // FPR should be reasonable
}

TEST_F(ConfigurationParametersTest, CacheCapacity) {
    size_t total_keys = 100000;
    double skewness = 0.8;  // Highly skewed (80/20 rule)
    size_t memory_budget = 10 * 1024 * 1024;  // 10MB
    size_t avg_value_size = 1024;  // 1KB per value

    size_t capacity = OptimizationFactory::EstimateCacheCapacity(
        total_keys, skewness, memory_budget, avg_value_size);

    EXPECT_GT(capacity, 0);
    EXPECT_LE(capacity, memory_budget / avg_value_size);  // Within budget
    EXPECT_LE(capacity, total_keys);  // Not more than total keys
}

TEST_F(ConfigurationParametersTest, BlockSize) {
    size_t avg_record_size = 100;  // bytes
    double selectivity = 0.1;  // 10% selectivity

    size_t block_size = OptimizationFactory::EstimateBlockSize(
        avg_record_size, selectivity);

    EXPECT_GT(block_size, 0);
    EXPECT_GT(block_size, 100);  // At least 100 rows
    EXPECT_LT(block_size, 100000);  // Not unreasonably large
}

}  // namespace marble

// Mock implementations for compilation
namespace marble {
class MockKey : public Key {
public:
    explicit MockKey(uint64_t value) : value_(value) {}

    int Compare(const Key& other) const override {
        const MockKey* other_key = dynamic_cast<const MockKey*>(&other);
        if (!other_key) return -1;
        if (value_ < other_key->value_) return -1;
        if (value_ > other_key->value_) return 1;
        return 0;
    }

    arrow::Result<std::shared_ptr<arrow::Scalar>> ToArrowScalar() const override {
        return arrow::MakeScalar(arrow::uint64(), value_);
    }

    std::shared_ptr<Key> Clone() const override {
        return std::make_shared<MockKey>(value_);
    }

    std::string ToString() const override {
        return std::to_string(value_);
    }

    size_t Hash() const override {
        return std::hash<uint64_t>{}(value_);
    }

    uint64_t GetValue() const { return value_; }

private:
    uint64_t value_;
};

class MockRecord : public Record {
public:
    MockRecord() : begin_ts_(0), commit_ts_(0) {}

    std::shared_ptr<Key> GetKey() const override {
        return std::make_shared<MockKey>(0);
    }

    arrow::Result<std::shared_ptr<arrow::RecordBatch>> ToRecordBatch() const override {
        auto schema = arrow::schema({arrow::field("id", arrow::uint64())});
        auto id_array = arrow::UInt64Builder().Finish().ValueOrDie();
        return arrow::RecordBatch::Make(schema, 0, {id_array});
    }

    std::shared_ptr<arrow::Schema> GetArrowSchema() const override {
        return arrow::schema({arrow::field("id", arrow::uint64())});
    }

    std::unique_ptr<RecordRef> AsRecordRef() const override {
        return nullptr;
    }

    void SetMVCCInfo(uint64_t begin_ts, uint64_t commit_ts) override {
        begin_ts_ = begin_ts;
        commit_ts_ = commit_ts;
    }

    uint64_t GetBeginTimestamp() const override {
        return begin_ts_;
    }

    uint64_t GetCommitTimestamp() const override {
        return commit_ts_;
    }

    bool IsVisible(uint64_t snapshot_ts) const override {
        return snapshot_ts >= begin_ts_ && (commit_ts_ == 0 || snapshot_ts < commit_ts_);
    }

private:
    uint64_t begin_ts_;
    uint64_t commit_ts_;
};
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
