/**
 * Unit tests for optimization strategies
 *
 * Tests BloomFilterStrategy, CacheStrategy, and OptimizationPipeline
 */

#include <gtest/gtest.h>
#include "marble/bloom_filter_strategy.h"
#include "marble/cache_strategy.h"
#include "marble/optimization_factory.h"
#include "marble/table_capabilities.h"
#include "marble/record.h"
#include "marble/api.h"  // For ColumnPredicate definition
#include <thread>
#include <random>

namespace marble {

//==============================================================================
// Mock implementations for testing
//==============================================================================

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

//==============================================================================
// HashBloomFilter Tests
//==============================================================================

class HashBloomFilterTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Create bloom filter for 1000 keys with 1% FPR
        bloom_ = std::make_unique<HashBloomFilter>(1000, 0.01);
    }

    std::unique_ptr<HashBloomFilter> bloom_;
};

TEST_F(HashBloomFilterTest, BasicAddAndQuery) {
    // Add some keys
    bloom_->Add(123);
    bloom_->Add(456);
    bloom_->Add(789);

    // Query existing keys (should always return true)
    EXPECT_TRUE(bloom_->MightContain(123));
    EXPECT_TRUE(bloom_->MightContain(456));
    EXPECT_TRUE(bloom_->MightContain(789));

    // Query non-existent keys (should mostly return false)
    // Note: False positives are possible but rare
    int false_positives = 0;
    for (uint64_t i = 1000; i < 2000; ++i) {
        if (bloom_->MightContain(i)) {
            false_positives++;
        }
    }

    // With 1% FPR, we expect ~10 false positives out of 1000 queries
    // Allow some tolerance (0-50 false positives is acceptable)
    EXPECT_LT(false_positives, 50) << "False positive rate too high";
}

TEST_F(HashBloomFilterTest, FalsePositiveRateAccuracy) {
    // Add 1000 keys
    for (uint64_t i = 0; i < 1000; ++i) {
        bloom_->Add(i);
    }

    // Query 10000 non-existent keys
    int false_positives = 0;
    for (uint64_t i = 10000; i < 20000; ++i) {
        if (bloom_->MightContain(i)) {
            false_positives++;
        }
    }

    double actual_fpr = static_cast<double>(false_positives) / 10000;

    // Theoretical FPR should be close to requested 1%
    // For small datasets (1000 keys), allow up to 10% due to variance
    EXPECT_LT(actual_fpr, 0.10) << "Actual FPR: " << actual_fpr;
    EXPECT_GT(actual_fpr, 0.001) << "Actual FPR: " << actual_fpr;

    // Check internal FPR calculation
    double reported_fpr = bloom_->FalsePositiveRate();
    EXPECT_LT(reported_fpr, 0.10);
}

TEST_F(HashBloomFilterTest, NoFalseNegatives) {
    // Add 100 keys
    std::vector<uint64_t> keys;
    for (uint64_t i = 0; i < 100; ++i) {
        uint64_t key = i * 1000;
        keys.push_back(key);
        bloom_->Add(key);
    }

    // Query all added keys - should NEVER return false
    for (uint64_t key : keys) {
        EXPECT_TRUE(bloom_->MightContain(key))
            << "False negative for key " << key << " (this should never happen!)";
    }
}

TEST_F(HashBloomFilterTest, Serialization) {
    // Add some keys
    bloom_->Add(111);
    bloom_->Add(222);
    bloom_->Add(333);

    // Serialize
    std::vector<uint8_t> data = bloom_->Serialize();
    EXPECT_GT(data.size(), 0);

    // Deserialize
    auto bloom2 = HashBloomFilter::Deserialize(data);
    ASSERT_NE(bloom2, nullptr);

    // Verify same behavior
    EXPECT_TRUE(bloom2->MightContain(111));
    EXPECT_TRUE(bloom2->MightContain(222));
    EXPECT_TRUE(bloom2->MightContain(333));
    EXPECT_EQ(bloom2->NumKeys(), bloom_->NumKeys());
}

TEST_F(HashBloomFilterTest, Clear) {
    // Add keys
    bloom_->Add(100);
    bloom_->Add(200);
    EXPECT_TRUE(bloom_->MightContain(100));

    // Clear
    bloom_->Clear();
    EXPECT_EQ(bloom_->NumKeys(), 0);

    // After clear, queries should mostly return false
    int false_positives = 0;
    for (uint64_t i = 0; i < 1000; ++i) {
        if (bloom_->MightContain(i)) {
            false_positives++;
        }
    }
    // Should be very few false positives after clear
    EXPECT_LT(false_positives, 10);
}

TEST_F(HashBloomFilterTest, ThreadSafety) {
    // Add keys from multiple threads
    std::vector<std::thread> threads;
    for (int t = 0; t < 4; ++t) {
        threads.emplace_back([this, t]() {
            for (uint64_t i = t * 1000; i < (t + 1) * 1000; ++i) {
                bloom_->Add(i);
            }
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }

    // Verify all keys present
    for (uint64_t i = 0; i < 4000; ++i) {
        EXPECT_TRUE(bloom_->MightContain(i));
    }
}

//==============================================================================
// BloomFilterStrategy Tests
//==============================================================================

class BloomFilterStrategyTest : public ::testing::Test {
protected:
    void SetUp() override {
        strategy_ = std::make_unique<BloomFilterStrategy>(10000, 0.01);

        // Initialize with table capabilities
        TableCapabilities caps;
        strategy_->OnTableCreate(caps);
    }

    std::unique_ptr<BloomFilterStrategy> strategy_;
};

TEST_F(BloomFilterStrategyTest, InitialState) {
    EXPECT_EQ(strategy_->Name(), "BloomFilter");
    EXPECT_GT(strategy_->MemoryUsage(), 0);

    const auto& stats = strategy_->GetStatsStruct();
    EXPECT_EQ(stats.num_lookups.load(), 0);
    EXPECT_EQ(stats.num_bloom_hits.load(), 0);
    EXPECT_EQ(stats.num_bloom_misses.load(), 0);
}

TEST_F(BloomFilterStrategyTest, WriteAndRead) {
    MockKey key1(1000);
    MockKey key2(2000);
    MockKey key3(3000);

    // Write some keys
    WriteContext wctx1{key1, nullptr, 0};
    WriteContext wctx2{key2, nullptr, 0};
    EXPECT_TRUE(strategy_->OnWrite(&wctx1).ok());
    EXPECT_TRUE(strategy_->OnWrite(&wctx2).ok());

    // Read existing key (should hit bloom filter)
    ReadContext rctx1{key1};
    Status s1 = strategy_->OnRead(&rctx1);
    EXPECT_TRUE(s1.ok());  // Bloom says "might exist"
    EXPECT_FALSE(rctx1.definitely_not_found);

    // Read non-existent key (should miss bloom filter)
    ReadContext rctx3{key3};
    Status s3 = strategy_->OnRead(&rctx3);
    EXPECT_TRUE(s3.IsNotFound());  // Bloom says "definitely not"
    EXPECT_TRUE(rctx3.definitely_not_found);

    // Check statistics
    const auto& stats = strategy_->GetStatsStruct();
    EXPECT_EQ(stats.num_lookups.load(), 2);
    EXPECT_GE(stats.num_bloom_misses.load(), 1);  // At least key3 missed
    EXPECT_EQ(stats.num_keys_added.load(), 2);
}

TEST_F(BloomFilterStrategyTest, OnlySupportPointLookups) {
    MockKey key(1000);

    // Add key
    WriteContext wctx{key, nullptr, 0};
    strategy_->OnWrite(&wctx);

    // Point lookup - should use bloom filter
    ReadContext rctx1{key};
    rctx1.is_point_lookup = true;
    Status s1 = strategy_->OnRead(&rctx1);
    EXPECT_TRUE(s1.ok());

    // Range scan - should skip bloom filter
    ReadContext rctx2{key};
    rctx2.is_point_lookup = false;
    rctx2.is_range_scan = true;
    Status s2 = strategy_->OnRead(&rctx2);
    EXPECT_TRUE(s2.ok());
    EXPECT_FALSE(rctx2.definitely_not_found);  // Bloom not consulted
}

TEST_F(BloomFilterStrategyTest, Clear) {
    // Add keys
    for (uint64_t i = 0; i < 100; ++i) {
        MockKey key(i);
        WriteContext wctx{key, nullptr, 0};
        strategy_->OnWrite(&wctx);
    }

    const auto& stats_before = strategy_->GetStatsStruct();
    EXPECT_EQ(stats_before.num_keys_added.load(), 100);

    // Clear
    strategy_->Clear();

    const auto& stats_after = strategy_->GetStatsStruct();
    EXPECT_EQ(stats_after.num_keys_added.load(), 0);
    EXPECT_EQ(stats_after.num_lookups.load(), 0);
}

//==============================================================================
// CacheStrategy Tests
//==============================================================================

class CacheStrategyTest : public ::testing::Test {
protected:
    void SetUp() override {
        strategy_ = std::make_unique<CacheStrategy>(100, 50);

        TableCapabilities caps;
        caps.enable_hot_key_cache = true;
        strategy_->OnTableCreate(caps);
    }

    std::unique_ptr<CacheStrategy> strategy_;
};

TEST_F(CacheStrategyTest, InitialState) {
    EXPECT_EQ(strategy_->Name(), "Cache");
    EXPECT_GT(strategy_->MemoryUsage(), 0);

    const auto& stats = strategy_->GetStatsStruct();
    EXPECT_EQ(stats.num_lookups.load(), 0);
    EXPECT_EQ(stats.num_hot_hits.load(), 0);
    EXPECT_EQ(stats.num_negative_hits.load(), 0);
}

TEST_F(CacheStrategyTest, HotKeyTracking) {
    MockKey key1(1000);

    // First read - miss
    ReadContext rctx1{key1};
    Status s1 = strategy_->OnRead(&rctx1);
    EXPECT_TRUE(s1.ok());
    EXPECT_FALSE(rctx1.definitely_found);

    // Simulate successful read
    MockRecord record;  // Assuming MockRecord exists or use test record
    strategy_->OnReadComplete(key1, record);

    // Second read - should hit hot tracker
    ReadContext rctx2{key1};
    Status s2 = strategy_->OnRead(&rctx2);
    EXPECT_TRUE(s2.ok());

    // Check statistics
    const auto& stats = strategy_->GetStatsStruct();
    EXPECT_EQ(stats.num_lookups.load(), 2);
    EXPECT_GE(stats.num_hot_hits.load(), 1);
}

TEST_F(CacheStrategyTest, NegativeCache) {
    MockKey missing_key(9999);

    // First miss - not in negative cache
    ReadContext rctx1{missing_key};
    Status s1 = strategy_->OnRead(&rctx1);
    EXPECT_TRUE(s1.ok());
    EXPECT_FALSE(rctx1.definitely_not_found);

    // TODO: Need to add API to mark failed lookup in negative cache
    // For now, negative cache only works if OnRead itself determines NotFound
}

TEST_F(CacheStrategyTest, Clear) {
    // Track some keys
    for (uint64_t i = 0; i < 10; ++i) {
        MockKey key(i);
        MockRecord record;
        strategy_->OnReadComplete(key, record);
    }

    const auto& stats_before = strategy_->GetStatsStruct();
    EXPECT_EQ(stats_before.num_inserts.load(), 10);

    // Clear
    strategy_->Clear();

    const auto& stats_after = strategy_->GetStatsStruct();
    EXPECT_EQ(stats_after.num_inserts.load(), 0);
    EXPECT_EQ(stats_after.num_lookups.load(), 0);
}

//==============================================================================
// OptimizationPipeline Tests
//==============================================================================

class OptimizationPipelineTest : public ::testing::Test {
protected:
    void SetUp() override {
        pipeline_ = std::make_unique<OptimizationPipeline>();
    }

    std::unique_ptr<OptimizationPipeline> pipeline_;
};

TEST_F(OptimizationPipelineTest, AddAndRemoveStrategies) {
    EXPECT_EQ(pipeline_->NumStrategies(), 0);

    // Add strategies
    pipeline_->AddStrategy(std::make_unique<BloomFilterStrategy>(1000, 0.01));
    EXPECT_EQ(pipeline_->NumStrategies(), 1);

    pipeline_->AddStrategy(std::make_unique<CacheStrategy>(100, 50));
    EXPECT_EQ(pipeline_->NumStrategies(), 2);

    // Get strategy by name
    auto* bloom = pipeline_->GetStrategy("BloomFilter");
    ASSERT_NE(bloom, nullptr);
    EXPECT_EQ(bloom->Name(), "BloomFilter");

    auto* cache = pipeline_->GetStrategy("Cache");
    ASSERT_NE(cache, nullptr);
    EXPECT_EQ(cache->Name(), "Cache");

    // Remove strategy
    pipeline_->RemoveStrategy("BloomFilter");
    EXPECT_EQ(pipeline_->NumStrategies(), 1);
    EXPECT_EQ(pipeline_->GetStrategy("BloomFilter"), nullptr);
}

TEST_F(OptimizationPipelineTest, ShortCircuitOnNotFound) {
    // Add bloom filter
    pipeline_->AddStrategy(std::make_unique<BloomFilterStrategy>(1000, 0.01));

    MockKey missing_key(9999);

    // Read non-existent key - bloom filter should short-circuit
    ReadContext rctx{missing_key};
    Status s = pipeline_->OnRead(&rctx);
    EXPECT_TRUE(s.IsNotFound());
    EXPECT_TRUE(rctx.definitely_not_found);
}

TEST_F(OptimizationPipelineTest, MultipleStrategiesCompose) {
    // Add both bloom filter and cache
    auto bloom = std::make_unique<BloomFilterStrategy>(1000, 0.01);
    auto cache = std::make_unique<CacheStrategy>(100, 50);

    // Initialize
    TableCapabilities caps;
    bloom->OnTableCreate(caps);
    cache->OnTableCreate(caps);

    pipeline_->AddStrategy(std::move(bloom));
    pipeline_->AddStrategy(std::move(cache));

    MockKey key(1000);

    // Write key (both strategies should see it)
    WriteContext wctx{key, nullptr, 0};
    Status s1 = pipeline_->OnWrite(&wctx);
    EXPECT_TRUE(s1.ok());

    // Read key (both strategies should be consulted)
    ReadContext rctx{key};
    Status s2 = pipeline_->OnRead(&rctx);
    EXPECT_TRUE(s2.ok());
}

TEST_F(OptimizationPipelineTest, MemoryUsage) {
    pipeline_->AddStrategy(std::make_unique<BloomFilterStrategy>(10000, 0.01));
    pipeline_->AddStrategy(std::make_unique<CacheStrategy>(1000, 100));

    size_t memory = pipeline_->MemoryUsage();
    EXPECT_GT(memory, 0);
}

TEST_F(OptimizationPipelineTest, GetStats) {
    pipeline_->AddStrategy(std::make_unique<BloomFilterStrategy>(1000, 0.01));
    pipeline_->AddStrategy(std::make_unique<CacheStrategy>(100, 50));

    std::string stats = pipeline_->GetStats();
    EXPECT_FALSE(stats.empty());
    EXPECT_NE(stats.find("BloomFilter"), std::string::npos);
    EXPECT_NE(stats.find("Cache"), std::string::npos);
}

}  // namespace marble

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
