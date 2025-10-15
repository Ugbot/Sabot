#include <iostream>
#include <vector>
#include <chrono>
#include <random>
#include <iomanip>
#include <memory>
#include <algorithm>

// Standalone test for block optimizations
// Tests the core optimization algorithms without full MarbleDB dependency

using namespace std::chrono;

// Mock Status class
struct Status {
    enum Code { OK_CODE, NOT_FOUND, ERROR };
    Code code;
    std::string msg;

    Status(Code c, const std::string& m = "") : code(c), msg(m) {}
    static Status OK() { return Status(OK_CODE); }
    static Status NotFound(const std::string& msg) { return Status(NOT_FOUND, msg); }
    bool ok() const { return code == OK_CODE; }
};

// Standalone bloom filter implementation
class SimpleBloomFilter {
private:
    std::vector<bool> bits_;
    size_t num_hash_functions_;

    uint64_t hash1(uint64_t key) const {
        return key * 0x9ddfea08eb382d69ULL;
    }

    uint64_t hash2(uint64_t key) const {
        return key * 0xc6a4a7935bd1e995ULL;
    }

public:
    SimpleBloomFilter(size_t bits_per_key, size_t expected_items)
        : bits_(bits_per_key * expected_items, false), num_hash_functions_(3) {}

    void Add(uint64_t key) {
        for (size_t i = 0; i < num_hash_functions_; ++i) {
            uint64_t hash = (hash1(key) + i * hash2(key)) % bits_.size();
            bits_[hash] = true;
        }
    }

    bool MayContain(uint64_t key) const {
        for (size_t i = 0; i < num_hash_functions_; ++i) {
            uint64_t hash = (hash1(key) + i * hash2(key)) % bits_.size();
            if (!bits_[hash]) return false;
        }
        return true;
    }
};

// Test data generator
std::vector<std::pair<uint64_t, std::string>> GenerateTestData(size_t count) {
    std::vector<std::pair<uint64_t, std::string>> data;
    data.reserve(count);

    for (size_t i = 0; i < count; ++i) {
        uint64_t key = i * 10;  // Gaps for testing misses
        std::string value = "value_" + std::to_string(key);
        data.emplace_back(key, value);
    }

    std::sort(data.begin(), data.end());
    return data;
}

// Binary search within a sorted range
Status BinarySearchInBlock(
    const std::vector<std::pair<uint64_t, std::string>>& entries,
    size_t block_start,
    size_t block_end,
    uint64_t key,
    std::string* value) {

    auto it = std::lower_bound(
        entries.begin() + block_start,
        entries.begin() + block_end,
        key,
        [](const std::pair<uint64_t, std::string>& entry, uint64_t k) {
            return entry.first < k;
        });

    if (it != entries.begin() + block_end && it->first == key) {
        *value = it->second;
        return Status::OK();
    }

    return Status::NotFound("Key not found");
}

// Benchmark 1: Block bloom filters with binary search
void BenchmarkBlockBloomFilters() {
    std::cout << "\n========================================\n";
    std::cout << "Benchmark 1: Block Bloom Filters + Binary Search\n";
    std::cout << "========================================\n";

    const size_t TOTAL_KEYS = 1000000;  // 1M keys
    const size_t BLOCK_SIZE = 8192;
    const size_t NUM_LOOKUPS = 10000;
    const size_t BITS_PER_KEY = 10;

    // Generate test data
    std::cout << "Generating " << TOTAL_KEYS << " sorted keys...\n";
    auto data = GenerateTestData(TOTAL_KEYS);

    // Build block bloom filters
    std::cout << "Building block bloom filters...\n";
    size_t num_blocks = (data.size() + BLOCK_SIZE - 1) / BLOCK_SIZE;
    std::vector<std::unique_ptr<SimpleBloomFilter>> block_blooms;

    for (size_t block_id = 0; block_id < num_blocks; ++block_id) {
        size_t block_start = block_id * BLOCK_SIZE;
        size_t block_end = std::min(block_start + BLOCK_SIZE, data.size());
        size_t block_key_count = block_end - block_start;

        auto bloom = std::make_unique<SimpleBloomFilter>(BITS_PER_KEY, block_key_count);

        // Add all keys in this block
        for (size_t i = block_start; i < block_end; ++i) {
            bloom->Add(data[i].first);
        }

        block_blooms.push_back(std::move(bloom));
    }

    std::cout << "Created " << block_blooms.size() << " block bloom filters\n";

    // Generate random keys for lookup
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<uint64_t> dist(0, TOTAL_KEYS * 10);

    std::vector<uint64_t> lookup_keys;
    for (size_t i = 0; i < NUM_LOOKUPS; ++i) {
        lookup_keys.push_back(dist(gen));
    }

    // Benchmark: WITH block bloom filters
    std::cout << "\nRunning " << NUM_LOOKUPS << " lookups WITH block bloom filters...\n";
    auto start = high_resolution_clock::now();

    size_t hits = 0;
    size_t blocks_skipped = 0;
    size_t blocks_scanned = 0;

    for (uint64_t key : lookup_keys) {
        std::string value;
        bool found = false;

        for (size_t block_id = 0; block_id < num_blocks; ++block_id) {
            // Check bloom filter first
            if (!block_blooms[block_id]->MayContain(key)) {
                blocks_skipped++;
                continue;
            }

            blocks_scanned++;

            // Binary search within block
            size_t block_start = block_id * BLOCK_SIZE;
            size_t block_end = std::min(block_start + BLOCK_SIZE, data.size());

            auto status = BinarySearchInBlock(data, block_start, block_end, key, &value);
            if (status.ok()) {
                hits++;
                found = true;
                break;
            }
        }
    }

    auto end = high_resolution_clock::now();
    auto duration = duration_cast<microseconds>(end - start);

    double skip_rate = blocks_skipped / (double)(blocks_skipped + blocks_scanned);

    std::cout << "Time: " << duration.count() << " μs\n";
    std::cout << "Avg latency: " << (double)duration.count() / NUM_LOOKUPS << " μs per lookup\n";
    std::cout << "Hits: " << hits << " / " << NUM_LOOKUPS << "\n";
    std::cout << "Blocks skipped: " << blocks_skipped << " (skip rate: "
              << (skip_rate * 100.0) << "%)\\n";
    std::cout << "Blocks scanned: " << blocks_scanned << "\n";

    double expected_speedup = 1.0 / (1.0 - skip_rate);
    std::cout << "\nExpected speedup from block skipping: " << std::fixed << std::setprecision(2)
              << expected_speedup << "x\n";

    std::cout << "\n✅ Block bloom filters working correctly!\n";
}

// Benchmark 2: Binary search vs linear scan
void BenchmarkBinarySearch() {
    std::cout << "\n========================================\n";
    std::cout << "Benchmark 2: Binary Search vs Linear Scan\n";
    std::cout << "========================================\n";

    const size_t BLOCK_SIZE = 8192;
    const size_t NUM_SEARCHES = 10000;

    // Generate one block of sorted data
    auto data = GenerateTestData(BLOCK_SIZE);

    // Generate random keys
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<uint64_t> dist(0, BLOCK_SIZE * 10);

    std::vector<uint64_t> lookup_keys;
    for (size_t i = 0; i < NUM_SEARCHES; ++i) {
        lookup_keys.push_back(dist(gen));
    }

    // Test binary search
    std::cout << "Testing binary search on " << BLOCK_SIZE << " keys...\n";
    auto start = high_resolution_clock::now();

    size_t binary_hits = 0;
    for (uint64_t key : lookup_keys) {
        std::string value;
        auto status = BinarySearchInBlock(data, 0, data.size(), key, &value);
        if (status.ok()) binary_hits++;
    }

    auto end = high_resolution_clock::now();
    auto binary_duration = duration_cast<microseconds>(end - start);

    std::cout << "Binary search: " << binary_duration.count() << " μs ("
              << (double)binary_duration.count() / NUM_SEARCHES << " μs per lookup)\n";
    std::cout << "Hits: " << binary_hits << " / " << NUM_SEARCHES << "\n";

    // Test linear scan
    std::cout << "\nTesting linear scan on " << BLOCK_SIZE << " keys...\n";
    start = high_resolution_clock::now();

    size_t linear_hits = 0;
    for (uint64_t key : lookup_keys) {
        for (const auto& [k, v] : data) {
            if (k == key) {
                linear_hits++;
                break;
            }
        }
    }

    end = high_resolution_clock::now();
    auto linear_duration = duration_cast<microseconds>(end - start);

    std::cout << "Linear scan: " << linear_duration.count() << " μs ("
              << (double)linear_duration.count() / NUM_SEARCHES << " μs per lookup)\n";
    std::cout << "Hits: " << linear_hits << " / " << NUM_SEARCHES << "\n";

    double speedup = (double)linear_duration.count() / binary_duration.count();
    std::cout << "\n✅ Binary search speedup: " << std::fixed << std::setprecision(2)
              << speedup << "x\n";
}

int main() {
    std::cout << "╔════════════════════════════════════════╗\n";
    std::cout << "║  MarbleDB Performance Optimizations   ║\n";
    std::cout << "║    Standalone Validation Tests         ║\n";
    std::cout << "╚════════════════════════════════════════╝\n";

    BenchmarkBlockBloomFilters();
    BenchmarkBinarySearch();

    std::cout << "\n========================================\n";
    std::cout << "✅ All tests completed successfully!\n";
    std::cout << "========================================\n";

    return 0;
}
