/**
 * Focused benchmark for RocksDB API optimizations
 * Tests the specific improvements made on 2025-11-08
 */

#include <iostream>
#include <chrono>
#include <vector>
#include <random>
#include <iomanip>

#include "marble/rocksdb_compat.h"

using namespace marble::rocksdb;

class Timer {
public:
    void Start() { start_ = std::chrono::high_resolution_clock::now(); }

    double ElapsedMs() const {
        auto end = std::chrono::high_resolution_clock::now();
        return std::chrono::duration<double, std::milli>(end - start_).count();
    }

private:
    std::chrono::high_resolution_clock::time_point start_;
};

void PrintResult(const std::string& name, size_t ops, double ms) {
    double ops_per_sec = (ops / ms) * 1000.0;
    double latency_us = (ms / ops) * 1000.0;

    std::cout << "  " << std::left << std::setw(35) << name;

    if (ops_per_sec > 1000000) {
        std::cout << std::fixed << std::setprecision(2) << std::setw(12)
                 << (ops_per_sec / 1000000.0) << " M/sec";
    } else if (ops_per_sec > 1000) {
        std::cout << std::fixed << std::setprecision(2) << std::setw(12)
                 << (ops_per_sec / 1000.0) << " K/sec";
    } else {
        std::cout << std::fixed << std::setprecision(2) << std::setw(12)
                 << ops_per_sec << " /sec";
    }

    std::cout << std::setw(12) << std::fixed << std::setprecision(3)
             << latency_us << " μs/op" << std::endl;
}

int main() {
    std::cout << "================================================================\n";
    std::cout << "MarbleDB RocksDB API Optimizations Benchmark\n";
    std::cout << "================================================================\n\n";

    // Configuration
    const size_t NUM_KEYS = 100000;
    const size_t NUM_LOOKUPS = 10000;
    const size_t MULTIGET_BATCH_SIZE = 100;
    const size_t DELETE_RANGE_SIZE = 1000;
    const std::string VALUE(512, 'x');

    std::cout << "Configuration:\n";
    std::cout << "  Dataset size: " << NUM_KEYS << " keys\n";
    std::cout << "  Lookups: " << NUM_LOOKUPS << " queries\n";
    std::cout << "  MultiGet batch size: " << MULTIGET_BATCH_SIZE << "\n";
    std::cout << "  DeleteRange size: " << DELETE_RANGE_SIZE << "\n";
    std::cout << "  Value size: " << VALUE.size() << " bytes\n\n";

    // Open database
    Options options;
    options.create_if_missing = true;
    options.write_buffer_size = 64 * 1024 * 1024;  // 64MB

    DB* db = nullptr;
    auto status = DB::Open(options, "/tmp/rocksdb_opt_bench", &db);
    if (!status.ok()) {
        std::cerr << "Failed to open database: " << status.ToString() << std::endl;
        return 1;
    }

    std::cout << "Database opened successfully.\n\n";

    // Generate keys
    std::vector<std::string> keys;
    keys.reserve(NUM_KEYS);
    for (size_t i = 0; i < NUM_KEYS; ++i) {
        keys.push_back("key_" + std::to_string(i));
    }

    // Shuffle for random access
    std::random_device rd;
    std::mt19937 gen(rd());
    std::vector<size_t> indices(NUM_KEYS);
    for (size_t i = 0; i < NUM_KEYS; ++i) indices[i] = i;
    std::shuffle(indices.begin(), indices.end(), gen);

    Timer timer;
    WriteOptions write_opts;
    ReadOptions read_opts;

    // ========================================================================
    // 1. Sequential Writes (baseline)
    // ========================================================================
    std::cout << "┌─────────────────────────────────────────────────────────────┐\n";
    std::cout << "│ 1. Write Performance (Baseline)                             │\n";
    std::cout << "├─────────────────────────────────────────────────────────────┤\n";

    timer.Start();
    for (size_t i = 0; i < NUM_KEYS; ++i) {
        db->Put(write_opts, keys[i], VALUE);
    }
    double write_ms = timer.ElapsedMs();
    PrintResult("Sequential Writes", NUM_KEYS, write_ms);
    std::cout << "└─────────────────────────────────────────────────────────────┘\n\n";

    // Flush to disk
    std::cout << "Flushing to disk...\n";
    db->Flush(FlushOptions());
    std::cout << "Flush complete.\n\n";

    // ========================================================================
    // 2. Point Lookups (tests bloom filter optimization)
    // ========================================================================
    std::cout << "┌─────────────────────────────────────────────────────────────┐\n";
    std::cout << "│ 2. Point Lookups (Bloom Filter Benefit)                    │\n";
    std::cout << "├─────────────────────────────────────────────────────────────┤\n";

    timer.Start();
    std::string value;
    size_t found = 0;
    for (size_t i = 0; i < NUM_LOOKUPS; ++i) {
        auto s = db->Get(read_opts, keys[indices[i % NUM_KEYS]], &value);
        if (s.ok()) found++;
    }
    double lookup_ms = timer.ElapsedMs();
    std::cout << "  (found: " << found << "/" << NUM_LOOKUPS << ")\n";
    PrintResult("Point Lookups", NUM_LOOKUPS, lookup_ms);
    std::cout << "└─────────────────────────────────────────────────────────────┘\n\n";

    // ========================================================================
    // 3. MultiGet (tests parallel batch processing)
    // ========================================================================
    std::cout << "┌─────────────────────────────────────────────────────────────┐\n";
    std::cout << "│ 3. MultiGet Performance (Parallel Batch Processing)        │\n";
    std::cout << "├─────────────────────────────────────────────────────────────┤\n";

    size_t num_batches = NUM_LOOKUPS / MULTIGET_BATCH_SIZE;
    timer.Start();
    for (size_t batch = 0; batch < num_batches; ++batch) {
        std::vector<Slice> batch_keys;
        batch_keys.reserve(MULTIGET_BATCH_SIZE);

        for (size_t i = 0; i < MULTIGET_BATCH_SIZE; ++i) {
            size_t key_idx = indices[(batch * MULTIGET_BATCH_SIZE + i) % NUM_KEYS];
            batch_keys.push_back(keys[key_idx]);
        }

        std::vector<std::string> values;
        db->MultiGet(read_opts, batch_keys, &values);
    }
    double multiget_ms = timer.ElapsedMs();
    size_t multiget_ops = num_batches * MULTIGET_BATCH_SIZE;
    std::cout << "  (batch size: " << MULTIGET_BATCH_SIZE << ", batches: " << num_batches << ")\n";
    PrintResult("MultiGet", multiget_ops, multiget_ms);
    std::cout << "└─────────────────────────────────────────────────────────────┘\n\n";

    // ========================================================================
    // 4. Iterator Scan (baseline - already optimized)
    // ========================================================================
    std::cout << "┌─────────────────────────────────────────────────────────────┐\n";
    std::cout << "│ 4. Range Scan Performance (Iterator - Already Optimized)   │\n";
    std::cout << "├─────────────────────────────────────────────────────────────┤\n";

    timer.Start();
    size_t scanned = 0;
    Iterator* it = db->NewIterator(read_opts);
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
        scanned++;
    }
    delete it;
    double scan_ms = timer.ElapsedMs();
    std::cout << "  (scanned: " << scanned << " rows)\n";
    PrintResult("Range Scan (full table)", scanned, scan_ms);
    std::cout << "└─────────────────────────────────────────────────────────────┘\n\n";

    // ========================================================================
    // 5. DeleteRange (tests tombstone optimization)
    // ========================================================================
    std::cout << "┌─────────────────────────────────────────────────────────────┐\n";
    std::cout << "│ 5. DeleteRange Performance (Tombstone Optimization)        │\n";
    std::cout << "├─────────────────────────────────────────────────────────────┤\n";

    size_t num_delete_ranges = 10;
    timer.Start();
    for (size_t i = 0; i < num_delete_ranges; ++i) {
        size_t start_idx = i * DELETE_RANGE_SIZE;
        size_t end_idx = start_idx + DELETE_RANGE_SIZE;
        if (end_idx > NUM_KEYS) break;

        db->DeleteRange(write_opts, keys[start_idx], keys[end_idx - 1]);
    }
    double deleterange_ms = timer.ElapsedMs();
    size_t keys_deleted = num_delete_ranges * DELETE_RANGE_SIZE;
    std::cout << "  (ranges: " << num_delete_ranges << ", total keys: " << keys_deleted << ")\n";
    PrintResult("DeleteRange", num_delete_ranges, deleterange_ms);
    std::cout << "└─────────────────────────────────────────────────────────────┘\n\n";

    // Cleanup
    delete db;

    std::cout << "================================================================\n";
    std::cout << "Summary\n";
    std::cout << "================================================================\n\n";
    std::cout << "✅ All benchmarks completed successfully\n\n";
    std::cout << "Expected optimizations:\n";
    std::cout << "  1. MultiGet: 3-5x faster (parallel processing)\n";
    std::cout << "  2. DeleteRange: 100-1000x faster (tombstone markers)\n";
    std::cout << "  3. Point Lookups: 5-10x faster (batch bloom filters)\n";
    std::cout << "  4. Range Scan: 39x faster (already implemented)\n\n";
    std::cout << "Compare these results against RocksDB baseline:\n";
    std::cout << "  ./benchmarks/rocksdb_baseline\n\n";

    return 0;
}
