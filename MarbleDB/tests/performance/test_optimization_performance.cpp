/**
 * Performance benchmarks for optimization strategies
 *
 * Measures actual speedups from bloom filters, caching, and other optimizations
 */

#include <gtest/gtest.h>
#include <benchmark/benchmark.h>
#include "marble/optimization_factory.h"
#include "marble/bloom_filter_strategy.h"
#include "marble/cache_strategy.h"
#include "marble/table_capabilities.h"
#include <random>
#include <chrono>

namespace marble {

//==============================================================================
// Mock implementations for benchmarking
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
// Benchmark Utilities
//==============================================================================

// Generate random keys for testing
std::vector<uint64_t> GenerateRandomKeys(size_t count, uint64_t max_value) {
    std::vector<uint64_t> keys;
    keys.reserve(count);

    std::mt19937_64 rng(12345);  // Fixed seed for reproducibility
    std::uniform_int_distribution<uint64_t> dist(0, max_value);

    for (size_t i = 0; i < count; ++i) {
        keys.push_back(dist(rng));
    }

    return keys;
}

// Generate skewed keys (80/20 distribution)
std::vector<uint64_t> GenerateSkewedKeys(size_t count, size_t hot_key_count) {
    std::vector<uint64_t> keys;
    keys.reserve(count);

    std::mt19937_64 rng(12345);
    std::uniform_int_distribution<uint64_t> hot_dist(0, hot_key_count - 1);
    std::uniform_int_distribution<uint64_t> cold_dist(hot_key_count, hot_key_count * 10);

    // 80% hot keys, 20% cold keys
    for (size_t i = 0; i < count; ++i) {
        if (i % 5 < 4) {  // 80%
            keys.push_back(hot_dist(rng));
        } else {  // 20%
            keys.push_back(cold_dist(rng));
        }
    }

    return keys;
}

//==============================================================================
// Bloom Filter Benchmarks
//==============================================================================

class HashBloomFilterBenchmark : public ::testing::Test {
protected:
    void SetUp() override {}

    // Measure lookup time WITHOUT bloom filter
    double BenchmarkWithoutBloom(const std::vector<uint64_t>& existing_keys,
                                  const std::vector<uint64_t>& query_keys) {
        auto start = std::chrono::high_resolution_clock::now();

        // Simulate disk lookups for all queries
        size_t found_count = 0;
        for (uint64_t query : query_keys) {
            // Linear search to simulate disk lookup
            for (uint64_t key : existing_keys) {
                if (key == query) {
                    found_count++;
                    break;
                }
            }
        }

        auto end = std::chrono::high_resolution_clock::now();
        double elapsed = std::chrono::duration<double>(end - start).count();

        return elapsed;
    }

    // Measure lookup time WITH bloom filter
    double BenchmarkWithBloom(const std::vector<uint64_t>& existing_keys,
                               const std::vector<uint64_t>& query_keys) {
        // Create bloom filter
        HashBloomFilter bloom(existing_keys.size(), 0.01);

        // Populate bloom filter
        for (uint64_t key : existing_keys) {
            bloom.Add(key);
        }

        auto start = std::chrono::high_resolution_clock::now();

        size_t found_count = 0;
        for (uint64_t query : query_keys) {
            // Check bloom filter first
            if (!bloom.MightContain(query)) {
                // Bloom says "definitely not" - skip disk lookup
                continue;
            }

            // Bloom says "might exist" - do disk lookup
            for (uint64_t key : existing_keys) {
                if (key == query) {
                    found_count++;
                    break;
                }
            }
        }

        auto end = std::chrono::high_resolution_clock::now();
        double elapsed = std::chrono::duration<double>(end - start).count();

        return elapsed;
    }
};

TEST_F(HashBloomFilterBenchmark, LookupPerformance_1M_Keys) {
    // Setup: 1M existing keys
    std::vector<uint64_t> existing_keys;
    for (uint64_t i = 0; i < 1000000; ++i) {
        existing_keys.push_back(i);
    }

    // Query: 100K mostly non-existent keys
    auto query_keys = GenerateRandomKeys(100000, 10000000);

    // Benchmark
    double time_without_bloom = BenchmarkWithoutBloom(existing_keys, query_keys);
    double time_with_bloom = BenchmarkWithBloom(existing_keys, query_keys);

    double speedup = time_without_bloom / time_with_bloom;

    std::cout << "Bloom Filter Performance (1M keys, 100K queries):" << std::endl;
    std::cout << "  Without bloom: " << time_without_bloom << " seconds" << std::endl;
    std::cout << "  With bloom:    " << time_with_bloom << " seconds" << std::endl;
    std::cout << "  Speedup:       " << speedup << "x" << std::endl;

    // Bloom filter should provide at least 2x speedup
    EXPECT_GT(speedup, 2.0) << "Bloom filter should speed up non-existent key lookups";
}

TEST_F(HashBloomFilterBenchmark, LookupPerformance_10M_Keys) {
    // Setup: 10M existing keys
    std::vector<uint64_t> existing_keys;
    for (uint64_t i = 0; i < 10000000; ++i) {
        existing_keys.push_back(i);
    }

    // Query: 1M mostly non-existent keys
    auto query_keys = GenerateRandomKeys(1000000, 100000000);

    // Benchmark
    double time_without_bloom = BenchmarkWithoutBloom(existing_keys, query_keys);
    double time_with_bloom = BenchmarkWithBloom(existing_keys, query_keys);

    double speedup = time_without_bloom / time_with_bloom;

    std::cout << "Bloom Filter Performance (10M keys, 1M queries):" << std::endl;
    std::cout << "  Without bloom: " << time_without_bloom << " seconds" << std::endl;
    std::cout << "  With bloom:    " << time_with_bloom << " seconds" << std::endl;
    std::cout << "  Speedup:       " << speedup << "x" << std::endl;

    // Larger dataset should show even better speedup
    EXPECT_GT(speedup, 3.0) << "Bloom filter should scale well with dataset size";
}

TEST_F(HashBloomFilterBenchmark, FalsePositiveRate) {
    // Create bloom filter for 1M keys with 1% FPR
    HashBloomFilter bloom(1000000, 0.01);

    // Add 1M keys
    for (uint64_t i = 0; i < 1000000; ++i) {
        bloom.Add(i);
    }

    // Query 1M non-existent keys
    int false_positives = 0;
    for (uint64_t i = 10000000; i < 11000000; ++i) {
        if (bloom.MightContain(i)) {
            false_positives++;
        }
    }

    double actual_fpr = static_cast<double>(false_positives) / 1000000;

    std::cout << "Bloom Filter False Positive Rate:" << std::endl;
    std::cout << "  Target FPR:  0.01 (1%)" << std::endl;
    std::cout << "  Actual FPR:  " << actual_fpr << std::endl;
    std::cout << "  False positives: " << false_positives << " / 1M queries" << std::endl;

    // FPR should be close to target (within 5%)
    EXPECT_LT(actual_fpr, 0.05);
}

//==============================================================================
// Cache Strategy Benchmarks
//==============================================================================

class CacheBenchmark : public ::testing::Test {
protected:
    void SetUp() override {
        strategy_ = std::make_unique<CacheStrategy>(10000, 1000);

        TableCapabilities caps;
        caps.enable_hot_key_cache = true;
        strategy_->OnTableCreate(caps);
    }

    std::unique_ptr<CacheStrategy> strategy_;
};

TEST_F(CacheBenchmark, HotKeyAccessPerformance) {
    // Setup: 10K hot keys (80/20 distribution)
    auto access_keys = GenerateSkewedKeys(1000000, 10000);

    // Warm up cache by "reading" keys
    for (uint64_t key_val : access_keys) {
        MockKey key(key_val);
        ReadContext rctx{key};
        strategy_->OnRead(&rctx);

        // Simulate successful read
        MockRecord record;
        strategy_->OnReadComplete(key, record);
    }

    // Now benchmark hot key lookups
    auto start = std::chrono::high_resolution_clock::now();

    size_t hot_hits = 0;
    for (int i = 0; i < 100000; ++i) {
        uint64_t key_val = i % 10000;  // Access hot keys
        MockKey key(key_val);
        ReadContext rctx{key};
        Status s = strategy_->OnRead(&rctx);
        if (s.ok() && !rctx.definitely_not_found) {
            hot_hits++;
        }
    }

    auto end = std::chrono::high_resolution_clock::now();
    double elapsed = std::chrono::duration<double>(end - start).count();

    const auto& stats = strategy_->GetStatsStruct();
    double hit_rate = static_cast<double>(stats.num_hot_hits.load()) / stats.num_lookups.load();

    std::cout << "Hot Key Cache Performance:" << std::endl;
    std::cout << "  100K lookups in " << elapsed << " seconds" << std::endl;
    std::cout << "  Throughput: " << (100000 / elapsed) << " lookups/sec" << std::endl;
    std::cout << "  Hit rate: " << (hit_rate * 100) << "%" << std::endl;
    std::cout << "  Hot hits: " << stats.num_hot_hits.load() << std::endl;

    // Cache should track hot keys effectively (>50% hit rate)
    EXPECT_GT(hit_rate, 0.5) << "Cache should achieve >50% hit rate on skewed workload";
}

TEST_F(CacheBenchmark, NegativeCacheEffectiveness) {
    // Benchmark negative cache (remember failed lookups)
    std::vector<uint64_t> missing_keys = GenerateRandomKeys(10000, 1000000);

    // First pass - populate negative cache
    for (uint64_t key_val : missing_keys) {
        MockKey key(key_val);
        ReadContext rctx{key};
        strategy_->OnRead(&rctx);
        // Don't call OnReadComplete (simulate lookup failure)
    }

    // Second pass - should hit negative cache
    auto start = std::chrono::high_resolution_clock::now();

    size_t negative_hits = 0;
    for (uint64_t key_val : missing_keys) {
        MockKey key(key_val);
        ReadContext rctx{key};
        Status s = strategy_->OnRead(&rctx);
        if (s.IsNotFound()) {
            negative_hits++;
        }
    }

    auto end = std::chrono::high_resolution_clock::now();
    double elapsed = std::chrono::duration<double>(end - start).count();

    const auto& stats = strategy_->GetStatsStruct();

    std::cout << "Negative Cache Performance:" << std::endl;
    std::cout << "  10K lookups in " << elapsed << " seconds" << std::endl;
    std::cout << "  Throughput: " << (10000 / elapsed) << " lookups/sec" << std::endl;
    std::cout << "  Negative hits: " << stats.num_negative_hits.load() << std::endl;

    // Note: Negative cache might not catch all due to TTL expiration
    // Just verify it's working to some degree
    EXPECT_GT(stats.num_negative_hits.load(), 0) << "Negative cache should catch some failed lookups";
}

//==============================================================================
// Pipeline Composition Benchmarks
//==============================================================================

class PipelineBenchmark : public ::testing::Test {
protected:
    void SetUp() override {
        // Create pipeline with bloom + cache
        pipeline_ = std::make_unique<OptimizationPipeline>();

        auto bloom = std::make_unique<BloomFilterStrategy>(1000000, 0.01);
        auto cache = std::make_unique<CacheStrategy>(10000, 1000);

        TableCapabilities caps;
        caps.enable_hot_key_cache = true;
        bloom->OnTableCreate(caps);
        cache->OnTableCreate(caps);

        pipeline_->AddStrategy(std::move(bloom));
        pipeline_->AddStrategy(std::move(cache));

        pipeline_->OnTableCreate(caps);
    }

    std::unique_ptr<OptimizationPipeline> pipeline_;
};

TEST_F(PipelineBenchmark, ComposedOptimizationPerformance) {
    // Populate pipeline with 100K keys
    for (uint64_t i = 0; i < 100000; ++i) {
        MockKey key(i);
        WriteContext wctx{key, nullptr, 0};
        pipeline_->OnWrite(&wctx);
    }

    // Benchmark mixed workload
    auto start = std::chrono::high_resolution_clock::now();

    // 50% existing keys, 50% non-existent keys
    size_t found = 0;
    size_t not_found = 0;

    for (int i = 0; i < 100000; ++i) {
        uint64_t key_val = (i % 2 == 0) ? (i / 2) : (i + 1000000);
        MockKey key(key_val);
        ReadContext rctx{key};
        Status s = pipeline_->OnRead(&rctx);

        if (s.IsNotFound()) {
            not_found++;
        } else {
            found++;
        }
    }

    auto end = std::chrono::high_resolution_clock::now();
    double elapsed = std::chrono::duration<double>(end - start).count();

    std::cout << "Composed Pipeline Performance:" << std::endl;
    std::cout << "  100K mixed lookups in " << elapsed << " seconds" << std::endl;
    std::cout << "  Throughput: " << (100000 / elapsed) << " lookups/sec" << std::endl;
    std::cout << "  Found: " << found << ", Not found: " << not_found << std::endl;
    std::cout << "  Memory usage: " << pipeline_->MemoryUsage() << " bytes" << std::endl;

    // Pipeline should filter out most non-existent keys
    EXPECT_GT(not_found, 40000) << "Bloom filter should catch most non-existent keys";
}

//==============================================================================
// Schema-Specific Optimizations Benchmark
//==============================================================================

class SchemaOptimizationBenchmark : public ::testing::Test {};

TEST_F(SchemaOptimizationBenchmark, RDFTripleAutoConfiguration) {
    // RDF triple schema
    auto schema = arrow::schema({
        arrow::field("subject", arrow::int64()),
        arrow::field("predicate", arrow::int64()),
        arrow::field("object", arrow::int64())
    });

    TableCapabilities caps;
    WorkloadHints hints;
    hints.access_pattern = WorkloadHints::POINT_LOOKUP;

    // Auto-configure
    auto pipeline = OptimizationFactory::CreateForSchema(schema, caps, hints);
    ASSERT_NE(pipeline, nullptr);

    pipeline->OnTableCreate(caps);

    // Populate with 1M triples
    auto start_write = std::chrono::high_resolution_clock::now();
    for (uint64_t i = 0; i < 1000000; ++i) {
        MockKey key(i);
        WriteContext wctx{key, nullptr, 0};
        pipeline->OnWrite(&wctx);
    }
    auto end_write = std::chrono::high_resolution_clock::now();
    double write_time = std::chrono::duration<double>(end_write - start_write).count();

    // Benchmark lookups
    auto start_read = std::chrono::high_resolution_clock::now();
    size_t filtered = 0;
    for (uint64_t i = 10000000; i < 10100000; ++i) {  // Non-existent keys
        MockKey key(i);
        ReadContext rctx{key};
        if (pipeline->OnRead(&rctx).IsNotFound()) {
            filtered++;
        }
    }
    auto end_read = std::chrono::high_resolution_clock::now();
    double read_time = std::chrono::duration<double>(end_read - start_read).count();

    std::cout << "RDF Triple Auto-Configuration Performance:" << std::endl;
    std::cout << "  Write 1M triples: " << write_time << " seconds" << std::endl;
    std::cout << "  Write throughput: " << (1000000 / write_time) << " triples/sec" << std::endl;
    std::cout << "  Read 100K queries: " << read_time << " seconds" << std::endl;
    std::cout << "  Read throughput: " << (100000 / read_time) << " queries/sec" << std::endl;
    std::cout << "  Filtered: " << filtered << " / 100K queries" << std::endl;

    // Should filter most non-existent keys
    EXPECT_GT(filtered, 95000) << "Auto-configured bloom should filter >95% of non-existent keys";
}

//==============================================================================
// Memory Usage Benchmarks
//==============================================================================

class MemoryBenchmark : public ::testing::Test {};

TEST_F(MemoryBenchmark, HashBloomFilterMemoryUsage) {
    // Theoretical: 10 bits per key = 1.25 bytes per key
    // 1M keys = 1.25 MB

    HashBloomFilter bloom(1000000, 0.01);

    for (uint64_t i = 0; i < 1000000; ++i) {
        bloom.Add(i);
    }

    size_t memory = bloom.MemoryUsage();
    double mb = memory / (1024.0 * 1024.0);
    double bytes_per_key = static_cast<double>(memory) / 1000000;

    std::cout << "Bloom Filter Memory Usage (1M keys):" << std::endl;
    std::cout << "  Total memory: " << mb << " MB" << std::endl;
    std::cout << "  Bytes per key: " << bytes_per_key << std::endl;

    // Should be close to theoretical 1.25 bytes per key
    EXPECT_LT(bytes_per_key, 2.0) << "Bloom filter should use <2 bytes per key";
}

}  // namespace marble

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
