/**
 * @file storage_techniques_bench.cpp
 * @brief Benchmark suite for storage optimization techniques
 *
 * This benchmark suite measures the impact of each storage technique:
 * - Baseline: Current implementation
 * - Step 1: Searchable MemTable with in-memory indexes
 * - Step 2: Large block write batching
 * - Step 3: Write buffer back-pressure
 *
 * Run with: ./storage_techniques_bench --benchmark_format=json > results.json
 */

#include <benchmark/benchmark.h>
#include <marble/memtable.h>
#include <marble/sstable.h>
#include <marble/lsm_storage.h>
#include <random>
#include <vector>
#include <string>
#include <chrono>

using namespace marble;

// ============================================================================
// Test Data Generation
// ============================================================================

class BenchmarkData {
public:
    BenchmarkData(size_t num_records = 100000) {
        GenerateRecords(num_records);
    }

    struct Record {
        uint64_t key;
        std::string user_id;
        std::string event_type;
        uint64_t timestamp;
        std::string payload;
    };

    void GenerateRecords(size_t count) {
        records_.clear();
        records_.reserve(count);

        std::random_device rd;
        std::mt19937_64 gen(rd());
        std::uniform_int_distribution<uint64_t> key_dist(1, 1000000);
        std::uniform_int_distribution<size_t> user_dist(1, 10000);
        std::uniform_int_distribution<size_t> event_dist(0, 9);

        const std::vector<std::string> event_types = {
            "click", "view", "purchase", "search", "login",
            "logout", "add_to_cart", "checkout", "review", "share"
        };

        auto now = std::chrono::system_clock::now().time_since_epoch().count();

        for (size_t i = 0; i < count; ++i) {
            Record rec;
            rec.key = key_dist(gen);
            rec.user_id = "user_" + std::to_string(user_dist(gen));
            rec.event_type = event_types[event_dist(gen)];
            rec.timestamp = now + i * 1000;  // 1ms apart
            rec.payload = GeneratePayload(512);  // 512 bytes
            records_.push_back(rec);
        }
    }

    std::string GeneratePayload(size_t size) {
        static const char charset[] =
            "0123456789"
            "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
            "abcdefghijklmnopqrstuvwxyz";

        std::string result;
        result.reserve(size);

        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_int_distribution<> dist(0, sizeof(charset) - 2);

        for (size_t i = 0; i < size; ++i) {
            result += charset[dist(gen)];
        }

        return result;
    }

    const std::vector<Record>& GetRecords() const { return records_; }

    std::vector<Record> GetRandomSample(size_t count) {
        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_int_distribution<size_t> dist(0, records_.size() - 1);

        std::vector<Record> sample;
        sample.reserve(count);

        for (size_t i = 0; i < count; ++i) {
            sample.push_back(records_[dist(gen)]);
        }

        return sample;
    }

private:
    std::vector<Record> records_;
};

// Global test data
static BenchmarkData g_test_data(100000);

// ============================================================================
// BASELINE: Current MemTable Performance
// ============================================================================

static void BM_Baseline_MemTable_SequentialWrite(benchmark::State& state) {
    const auto& records = g_test_data.GetRecords();

    for (auto _ : state) {
        auto memtable = CreateMemTable();

        for (const auto& rec : records) {
            memtable->Put(rec.key, rec.payload);
        }

        benchmark::DoNotOptimize(memtable);
    }

    state.SetItemsProcessed(state.iterations() * records.size());
    state.SetBytesProcessed(state.iterations() * records.size() * 512);
}
BENCHMARK(BM_Baseline_MemTable_SequentialWrite);

static void BM_Baseline_MemTable_PointLookup(benchmark::State& state) {
    auto memtable = CreateMemTable();
    const auto& records = g_test_data.GetRecords();

    // Populate
    for (const auto& rec : records) {
        memtable->Put(rec.key, rec.payload);
    }

    // Lookup random keys
    auto sample = g_test_data.GetRandomSample(1000);

    for (auto _ : state) {
        for (const auto& rec : sample) {
            std::string value;
            auto status = memtable->Get(rec.key, &value);
            benchmark::DoNotOptimize(status);
            benchmark::DoNotOptimize(value);
        }
    }

    state.SetItemsProcessed(state.iterations() * sample.size());
}
BENCHMARK(BM_Baseline_MemTable_PointLookup);

static void BM_Baseline_MemTable_SequentialScan(benchmark::State& state) {
    auto memtable = CreateMemTable();
    const auto& records = g_test_data.GetRecords();

    // Populate
    for (const auto& rec : records) {
        memtable->Put(rec.key, rec.payload);
    }

    for (auto _ : state) {
        auto iter = memtable->CreateIterator();
        size_t count = 0;

        for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
            benchmark::DoNotOptimize(iter->Key());
            benchmark::DoNotOptimize(iter->Value());
            count++;
        }

        benchmark::DoNotOptimize(count);
    }

    state.SetItemsProcessed(state.iterations() * records.size());
}
BENCHMARK(BM_Baseline_MemTable_SequentialScan);

// Simulated equality search (no index)
static void BM_Baseline_MemTable_EqualitySearch_NoIndex(benchmark::State& state) {
    auto memtable = CreateMemTable();
    const auto& records = g_test_data.GetRecords();

    // Populate
    for (const auto& rec : records) {
        memtable->Put(rec.key, rec.payload);
    }

    std::string target_user = "user_5000";

    for (auto _ : state) {
        // Sequential scan to find matches (no index)
        auto iter = memtable->CreateIterator();
        size_t matches = 0;

        for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
            // Simulate parsing payload to check user_id
            // In reality, would deserialize and check
            matches++;
        }

        benchmark::DoNotOptimize(matches);
    }

    state.SetItemsProcessed(state.iterations() * records.size());
}
BENCHMARK(BM_Baseline_MemTable_EqualitySearch_NoIndex);

// Simulated range search (no index)
static void BM_Baseline_MemTable_RangeSearch_NoIndex(benchmark::State& state) {
    auto memtable = CreateMemTable();
    const auto& records = g_test_data.GetRecords();

    // Populate
    for (const auto& rec : records) {
        memtable->Put(rec.key, rec.payload);
    }

    auto now = std::chrono::system_clock::now().time_since_epoch().count();
    uint64_t start_ts = now;
    uint64_t end_ts = now + 10000000;  // 10 second range

    for (auto _ : state) {
        // Sequential scan for timestamp range (no index)
        auto iter = memtable->CreateIterator();
        size_t matches = 0;

        for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
            // Simulate parsing payload to check timestamp
            matches++;
        }

        benchmark::DoNotOptimize(matches);
    }

    state.SetItemsProcessed(state.iterations() * records.size());
}
BENCHMARK(BM_Baseline_MemTable_RangeSearch_NoIndex);

// ============================================================================
// BASELINE: SSTable Write Performance
// ============================================================================

static void BM_Baseline_SSTable_Write_4KB_Blocks(benchmark::State& state) {
    const auto& records = g_test_data.GetRecords();

    LSMTreeConfig config;
    config.sstable_block_size = 4096;  // Current: 4KB blocks

    for (auto _ : state) {
        auto sstable = CreateSSTable("/tmp/bench_sstable_4kb.sst", config);

        for (const auto& rec : records) {
            sstable->Put(rec.key, rec.payload);
        }

        sstable->Finalize();
        benchmark::DoNotOptimize(sstable);
    }

    state.SetItemsProcessed(state.iterations() * records.size());
    state.SetBytesProcessed(state.iterations() * records.size() * 512);
}
BENCHMARK(BM_Baseline_SSTable_Write_4KB_Blocks);

// ============================================================================
// BASELINE: Memory Usage
// ============================================================================

static void BM_Baseline_MemTable_MemoryUsage(benchmark::State& state) {
    const auto& records = g_test_data.GetRecords();
    size_t record_count = state.range(0);

    for (auto _ : state) {
        auto memtable = CreateMemTable();

        for (size_t i = 0; i < record_count && i < records.size(); ++i) {
            memtable->Put(records[i].key, records[i].payload);
        }

        size_t memory_usage = memtable->MemoryUsage();
        state.counters["memory_bytes"] = memory_usage;
        state.counters["memory_per_record"] = memory_usage / record_count;

        benchmark::DoNotOptimize(memtable);
    }
}
BENCHMARK(BM_Baseline_MemTable_MemoryUsage)->Arg(10000)->Arg(50000)->Arg(100000);

// ============================================================================
// PLACEHOLDER: Searchable MemTable Benchmarks (Step 1)
// ============================================================================
// These will be enabled after implementing searchable MemTable

/*
static void BM_Step1_SearchableMemTable_EqualityQuery_Hash(benchmark::State& state) {
    auto memtable = CreateSearchableMemTable();
    const auto& records = g_test_data.GetRecords();

    // Configure with hash index
    MemTableConfig config;
    config.indexes = {{"user_id_idx", {"user_id"}, IndexConfig::kEager}};
    config.enable_hash_index = true;
    memtable->Configure(config);

    // Populate
    for (const auto& rec : records) {
        memtable->Put(rec.key, rec.payload);
    }

    IndexQuery query;
    query.type = IndexQuery::kEquality;
    query.index_name = "user_id_idx";
    query.value = "user_5000";

    for (auto _ : state) {
        std::vector<Record> results;
        auto status = memtable->SearchByIndex("user_id_idx", query, &results);
        benchmark::DoNotOptimize(status);
        benchmark::DoNotOptimize(results);
    }
}
// BENCHMARK(BM_Step1_SearchableMemTable_EqualityQuery_Hash);

static void BM_Step1_SearchableMemTable_RangeQuery_BTree(benchmark::State& state) {
    auto memtable = CreateSearchableMemTable();
    const auto& records = g_test_data.GetRecords();

    // Configure with B-tree index
    MemTableConfig config;
    config.indexes = {{"timestamp_idx", {"timestamp"}, IndexConfig::kEager}};
    config.enable_btree_index = true;
    memtable->Configure(config);

    // Populate
    for (const auto& rec : records) {
        memtable->Put(rec.key, rec.payload);
    }

    auto now = std::chrono::system_clock::now().time_since_epoch().count();
    IndexQuery query;
    query.type = IndexQuery::kRange;
    query.index_name = "timestamp_idx";
    query.start_value = std::to_string(now);
    query.end_value = std::to_string(now + 10000000);

    for (auto _ : state) {
        std::vector<Record> results;
        auto status = memtable->SearchByIndex("timestamp_idx", query, &results);
        benchmark::DoNotOptimize(status);
        benchmark::DoNotOptimize(results);
    }
}
// BENCHMARK(BM_Step1_SearchableMemTable_RangeQuery_BTree);
*/

// ============================================================================
// PLACEHOLDER: Large Block Write Benchmarks (Step 2)
// ============================================================================

/*
static void BM_Step2_SSTable_Write_8MB_Blocks(benchmark::State& state) {
    const auto& records = g_test_data.GetRecords();

    LSMTreeConfig config;
    config.write_block_size_mb = 8;   // NEW: 8MB blocks
    config.flush_size_kb = 128;       // NEW: 128KB flushes

    for (auto _ : state) {
        auto sstable = CreateSSTable("/tmp/bench_sstable_8mb.sst", config);

        for (const auto& rec : records) {
            sstable->Put(rec.key, rec.payload);
        }

        sstable->Finalize();
        benchmark::DoNotOptimize(sstable);
    }

    state.SetItemsProcessed(state.iterations() * records.size());
    state.SetBytesProcessed(state.iterations() * records.size() * 512);
}
// BENCHMARK(BM_Step2_SSTable_Write_8MB_Blocks);
*/

// ============================================================================
// PLACEHOLDER: Write Buffer Back-Pressure Benchmarks (Step 3)
// ============================================================================

/*
static void BM_Step3_WriteBuffer_WithBackPressure(benchmark::State& state) {
    const auto& records = g_test_data.GetRecords();

    LSMTreeConfig config;
    config.write_buffer_config.max_write_buffer_mb = 256;
    config.write_buffer_config.strategy = WriteBufferConfig::kSlowWrites;

    auto lsm = CreateLSMTree();
    lsm->Init(config);

    for (auto _ : state) {
        for (const auto& rec : records) {
            auto status = lsm->Put(rec.key, rec.payload);
            if (!status.ok()) {
                state.counters["backpressure_events"]++;
            }
        }
    }

    state.SetItemsProcessed(state.iterations() * records.size());
}
// BENCHMARK(BM_Step3_WriteBuffer_WithBackPressure);
*/

// ============================================================================
// Main
// ============================================================================

BENCHMARK_MAIN();
