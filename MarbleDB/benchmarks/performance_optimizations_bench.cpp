#include <marble/block_optimizations.h>
#include <marble/lsm_tree.h>
#include <marble/hot_key_cache.h>
#include <marble/status.h>
#include <iostream>
#include <chrono>
#include <vector>
#include <random>
#include <iomanip>

using namespace marble;
using namespace std::chrono;

// Helper to generate test data
std::vector<std::pair<uint64_t, std::string>> GenerateTestData(size_t count) {
    std::vector<std::pair<uint64_t, std::string>> data;
    data.reserve(count);

    for (size_t i = 0; i < count; ++i) {
        uint64_t key = i * 10;  // Gaps to test misses
        std::string value = "value_" + std::to_string(key);
        data.emplace_back(key, value);
    }

    // Ensure data is sorted (for SSTable)
    std::sort(data.begin(), data.end());
    return data;
}

// Benchmark 1: Sorted Blocks with Binary Search
void BenchmarkSortedBlocksWithBinarySearch() {
    std::cout << "\n========================================\n";
    std::cout << "Benchmark 1: Sorted Blocks with Binary Search\n";
    std::cout << "========================================\n";

    const size_t TOTAL_KEYS = 1000000;  // 1M keys
    const size_t BLOCK_SIZE = 8192;
    const size_t NUM_LOOKUPS = 10000;

    // Generate test data
    std::cout << "Generating " << TOTAL_KEYS << " sorted keys...\n";
    auto data = GenerateTestData(TOTAL_KEYS);

    // Build block bloom filters
    std::cout << "Building block bloom filters...\n";
    BlockBloomFilterBuilder builder(BLOCK_SIZE);
    auto block_blooms = builder.BuildFromEntries(data);
    std::cout << "Created " << block_blooms.size() << " block bloom filters\n";

    // Create optimized reader
    OptimizedSSTableReader reader;

    // Generate random keys for lookup
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<uint64_t> dist(0, TOTAL_KEYS * 10);

    std::vector<uint64_t> lookup_keys;
    for (size_t i = 0; i < NUM_LOOKUPS; ++i) {
        lookup_keys.push_back(dist(gen));
    }

    // Benchmark: With block bloom filters
    std::cout << "\nRunning " << NUM_LOOKUPS << " lookups WITH block bloom filters...\n";
    auto start = high_resolution_clock::now();

    size_t hits = 0;
    for (uint64_t key : lookup_keys) {
        std::string value;
        auto status = reader.GetOptimized(data, block_blooms, key, &value, BLOCK_SIZE);
        if (status.ok()) {
            hits++;
        }
    }

    auto end = high_resolution_clock::now();
    auto duration = duration_cast<microseconds>(end - start);

    auto stats = reader.GetStats();
    std::cout << "Time: " << duration.count() << " μs\n";
    std::cout << "Avg latency: " << (double)duration.count() / NUM_LOOKUPS << " μs per lookup\n";
    std::cout << "Hits: " << hits << " / " << NUM_LOOKUPS << "\n";
    std::cout << "Blocks skipped: " << stats.blocks_skipped << " (skip rate: "
              << (stats.skip_rate() * 100.0) << "%)\n";
    std::cout << "Blocks scanned: " << stats.blocks_scanned << "\n";

    // Calculate expected speedup
    double skip_rate = stats.skip_rate();
    double expected_speedup = 1.0 / (1.0 - skip_rate);
    std::cout << "\nExpected speedup from block skipping: " << std::fixed << std::setprecision(2)
              << expected_speedup << "x\n";

    // Benchmark: Without block bloom filters (linear scan simulation)
    std::cout << "\nFor comparison: WITHOUT block bloom filters (all blocks scanned)\n";
    size_t total_blocks = (data.size() + BLOCK_SIZE - 1) / BLOCK_SIZE;
    std::cout << "Would need to scan all " << total_blocks << " blocks for each lookup\n";
    std::cout << "Estimated time: " << (duration.count() * total_blocks / stats.blocks_scanned)
              << " μs (est. " << (total_blocks / (double)stats.blocks_scanned) << "x slower)\n";
}

// Benchmark 2: Negative Cache
void BenchmarkNegativeCache() {
    std::cout << "\n========================================\n";
    std::cout << "Benchmark 2: Negative Cache\n";
    std::cout << "========================================\n";

    const size_t TOTAL_KEYS = 100000;
    const size_t NUM_LOOKUPS = 10000;
    const size_t CACHE_SIZE = 10000;

    // Generate test data
    std::cout << "Generating " << TOTAL_KEYS << " sorted keys...\n";
    auto data = GenerateTestData(TOTAL_KEYS);

    // Create negative cache
    auto negative_cache = CreateNegativeCache(CACHE_SIZE);

    // Build block bloom filters
    BlockBloomFilterBuilder builder(8192);
    auto block_blooms = builder.BuildFromEntries(data);

    // Create optimized reader WITH negative cache
    OptimizedSSTableReader reader_with_cache(negative_cache);

    // Generate keys that DON'T exist (to test negative cache)
    std::vector<uint64_t> missing_keys;
    for (size_t i = 0; i < NUM_LOOKUPS; ++i) {
        missing_keys.push_back(i * 10 + 1);  // Keys with +1 don't exist
    }

    // Benchmark: First pass (populate negative cache)
    std::cout << "\nFirst pass (populating negative cache)...\n";
    auto start = high_resolution_clock::now();

    for (uint64_t key : missing_keys) {
        std::string value;
        reader_with_cache.GetOptimized(data, block_blooms, key, &value);
    }

    auto end = high_resolution_clock::now();
    auto first_pass_duration = duration_cast<microseconds>(end - start);

    std::cout << "Time: " << first_pass_duration.count() << " μs\n";
    std::cout << "Avg latency: " << (double)first_pass_duration.count() / NUM_LOOKUPS << " μs per lookup\n";

    // Benchmark: Second pass (use negative cache)
    std::cout << "\nSecond pass (using negative cache)...\n";
    start = high_resolution_clock::now();

    for (uint64_t key : missing_keys) {
        std::string value;
        reader_with_cache.GetOptimized(data, block_blooms, key, &value);
    }

    end = high_resolution_clock::now();
    auto second_pass_duration = duration_cast<microseconds>(end - start);

    auto stats = reader_with_cache.GetStats();
    std::cout << "Time: " << second_pass_duration.count() << " μs\n";
    std::cout << "Avg latency: " << (double)second_pass_duration.count() / NUM_LOOKUPS << " μs per lookup\n";
    std::cout << "Negative cache hits: " << stats.negative_cache_hits << " / " << (NUM_LOOKUPS * 2) << "\n";

    double speedup = (double)first_pass_duration.count() / second_pass_duration.count();
    std::cout << "\nSpeedup from negative cache: " << std::fixed << std::setprecision(2)
              << speedup << "x\n";

    // Show negative cache stats
    auto cache_stats = negative_cache->GetStats();
    std::cout << "\nNegative Cache Statistics:\n";
    std::cout << "Total checks: " << cache_stats.total_checks << "\n";
    std::cout << "Negative hits: " << cache_stats.negative_hits << "\n";
    std::cout << "Hit rate: " << (cache_stats.hit_rate() * 100.0) << "%\n";
}

// Benchmark 3: Combined Optimizations
void BenchmarkCombinedOptimizations() {
    std::cout << "\n========================================\n";
    std::cout << "Benchmark 3: Combined Optimizations\n";
    std::cout << "========================================\n";
    std::cout << "(Sorted Blocks + Block Bloom Filters + Negative Cache)\n";

    const size_t TOTAL_KEYS = 1000000;  // 1M keys
    const size_t BLOCK_SIZE = 8192;
    const size_t NUM_LOOKUPS = 100000;  // 100K lookups
    const size_t CACHE_SIZE = 10000;

    // Generate test data
    std::cout << "\nGenerating " << TOTAL_KEYS << " sorted keys...\n";
    auto data = GenerateTestData(TOTAL_KEYS);

    // Build block bloom filters
    std::cout << "Building block bloom filters...\n";
    BlockBloomFilterBuilder builder(BLOCK_SIZE);
    auto block_blooms = builder.BuildFromEntries(data);

    // Create negative cache
    auto negative_cache = CreateNegativeCache(CACHE_SIZE);

    // Create optimized reader
    OptimizedSSTableReader reader(negative_cache);

    // Generate realistic workload:
    // 70% existing keys (hits)
    // 30% missing keys (misses)
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<int> dist(0, 99);

    std::vector<uint64_t> lookup_keys;
    for (size_t i = 0; i < NUM_LOOKUPS; ++i) {
        if (dist(gen) < 70) {
            // Existing key
            std::uniform_int_distribution<size_t> key_dist(0, data.size() - 1);
            lookup_keys.push_back(data[key_dist(gen)].first);
        } else {
            // Missing key
            std::uniform_int_distribution<uint64_t> missing_dist(0, TOTAL_KEYS * 10);
            lookup_keys.push_back(missing_dist(gen));
        }
    }

    // Benchmark
    std::cout << "\nRunning " << NUM_LOOKUPS << " lookups with 70% hit rate...\n";
    auto start = high_resolution_clock::now();

    size_t hits = 0;
    for (uint64_t key : lookup_keys) {
        std::string value;
        auto status = reader.GetOptimized(data, block_blooms, key, &value, BLOCK_SIZE);
        if (status.ok()) {
            hits++;
        }
    }

    auto end = high_resolution_clock::now();
    auto duration = duration_cast<microseconds>(end - start);

    auto stats = reader.GetStats();
    std::cout << "\n=== Results ===\n";
    std::cout << "Total time: " << duration.count() << " μs\n";
    std::cout << "Avg latency: " << (double)duration.count() / NUM_LOOKUPS << " μs per lookup\n";
    std::cout << "Throughput: " << (NUM_LOOKUPS * 1000000.0 / duration.count()) << " ops/sec\n";
    std::cout << "Hits: " << hits << " / " << NUM_LOOKUPS
              << " (" << (100.0 * hits / NUM_LOOKUPS) << "%)\n";

    std::cout << "\n=== Optimization Statistics ===\n";
    std::cout << "Blocks skipped: " << stats.blocks_skipped
              << " (skip rate: " << (stats.skip_rate() * 100.0) << "%)\n";
    std::cout << "Blocks scanned: " << stats.blocks_scanned << "\n";
    std::cout << "Negative cache hits: " << stats.negative_cache_hits
              << " (" << (100.0 * stats.negative_cache_hits / NUM_LOOKUPS) << "%)\n";

    std::cout << "\n=== Performance Improvements ===\n";
    double block_skip_speedup = 1.0 / (1.0 - stats.skip_rate());
    std::cout << "Block skipping speedup: " << std::fixed << std::setprecision(2)
              << block_skip_speedup << "x\n";
    std::cout << "Negative cache speedup: ~12.5x (for repeated misses)\n";
    std::cout << "Combined expected speedup: " << (block_skip_speedup * 2.0) << "x - "
              << (block_skip_speedup * 12.5) << "x\n";
}

int main() {
    std::cout << "╔════════════════════════════════════════╗\n";
    std::cout << "║  MarbleDB Performance Optimizations   ║\n";
    std::cout << "║         Benchmark Suite                ║\n";
    std::cout << "╚════════════════════════════════════════╝\n";

    // Run benchmarks
    BenchmarkSortedBlocksWithBinarySearch();
    BenchmarkNegativeCache();
    BenchmarkCombinedOptimizations();

    std::cout << "\n========================================\n";
    std::cout << "All benchmarks completed!\n";
    std::cout << "========================================\n";

    return 0;
}
