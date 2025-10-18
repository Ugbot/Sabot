/**
 * @file simple_storage_bench.cpp
 * @brief Simple standalone benchmark for storage techniques (no external deps)
 *
 * Compile: g++ -std=c++20 -O3 -I../include -o simple_storage_bench simple_storage_bench.cpp ../src/core/memtable.cpp -lpthread
 * Run: ./simple_storage_bench
 */

#include <iostream>
#include <chrono>
#include <vector>
#include <random>
#include <iomanip>
#include <string>
#include <map>
#include <algorithm>

// Simple timing utilities
class Timer {
public:
    Timer() : start_(std::chrono::high_resolution_clock::now()) {}

    double ElapsedMs() const {
        auto end = std::chrono::high_resolution_clock::now();
        return std::chrono::duration<double, std::milli>(end - start_).count();
    }

    double ElapsedUs() const {
        auto end = std::chrono::high_resolution_clock::now();
        return std::chrono::duration<double, std::micro>(end - start_).count();
    }

private:
    std::chrono::high_resolution_clock::time_point start_;
};

// Simple test data generator
class TestDataGenerator {
public:
    struct Record {
        uint64_t key;
        std::string value;
    };

    static std::vector<Record> GenerateRecords(size_t count, size_t value_size = 512) {
        std::vector<Record> records;
        records.reserve(count);

        std::random_device rd;
        std::mt19937_64 gen(rd());
        std::uniform_int_distribution<uint64_t> dist(1, 1000000);

        for (size_t i = 0; i < count; ++i) {
            Record rec;
            rec.key = dist(gen);
            rec.value = GenerateValue(value_size);
            records.push_back(rec);
        }

        return records;
    }

private:
    static std::string GenerateValue(size_t size) {
        static const char charset[] = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
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
};

// Simple in-memory map (baseline)
class SimpleMemTable {
public:
    void Put(uint64_t key, const std::string& value) {
        data_[key] = value;
    }

    bool Get(uint64_t key, std::string* value) const {
        auto it = data_.find(key);
        if (it != data_.end()) {
            *value = it->second;
            return true;
        }
        return false;
    }

    size_t Size() const {
        return data_.size();
    }

private:
    std::map<uint64_t, std::string> data_;
};

// Print benchmark results
void PrintResults(const std::string& name, size_t operations, double elapsed_ms) {
    double ops_per_sec = (operations / elapsed_ms) * 1000.0;

    std::cout << std::left << std::setw(50) << name
              << std::right << std::setw(15);

    if (ops_per_sec > 1000000) {
        std::cout << std::fixed << std::setprecision(2) << (ops_per_sec / 1000000.0) << " M/sec";
    } else if (ops_per_sec > 1000) {
        std::cout << std::fixed << std::setprecision(2) << (ops_per_sec / 1000.0) << " K/sec";
    } else {
        std::cout << std::fixed << std::setprecision(2) << ops_per_sec << " /sec";
    }

    std::cout << std::setw(15) << std::fixed << std::setprecision(2) << elapsed_ms << " ms"
              << std::endl;
}

int main() {
    std::cout << "====================================================================" << std::endl;
    std::cout << "MarbleDB Storage Techniques - Simple Benchmark" << std::endl;
    std::cout << "====================================================================" << std::endl;
    std::cout << std::endl;

    const size_t NUM_RECORDS = 100000;
    const size_t VALUE_SIZE = 512;

    std::cout << "Generating " << NUM_RECORDS << " test records..." << std::endl;
    auto test_data = TestDataGenerator::GenerateRecords(NUM_RECORDS, VALUE_SIZE);
    std::cout << "Test data generated: " << test_data.size() << " records" << std::endl;
    std::cout << std::endl;

    // ========================================================================
    // BASELINE: Sequential Write
    // ========================================================================

    std::cout << "┌────────────────────────────────────────────────────────────────────┐" << std::endl;
    std::cout << "│ Benchmark                                  │ Throughput  │ Time     │" << std::endl;
    std::cout << "├────────────────────────────────────────────────────────────────────┤" << std::endl;

    {
        SimpleMemTable memtable;
        Timer timer;

        for (const auto& rec : test_data) {
            memtable.Put(rec.key, rec.value);
        }

        double elapsed = timer.ElapsedMs();
        PrintResults("Baseline: Sequential Write", test_data.size(), elapsed);
    }

    // ========================================================================
    // BASELINE: Point Lookup
    // ========================================================================

    {
        SimpleMemTable memtable;

        // Populate
        for (const auto& rec : test_data) {
            memtable.Put(rec.key, rec.value);
        }

        // Random lookups
        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_int_distribution<size_t> dist(0, test_data.size() - 1);

        const size_t NUM_LOOKUPS = 10000;
        std::vector<size_t> lookup_indices;
        for (size_t i = 0; i < NUM_LOOKUPS; ++i) {
            lookup_indices.push_back(dist(gen));
        }

        Timer timer;
        size_t found = 0;

        for (size_t idx : lookup_indices) {
            std::string value;
            if (memtable.Get(test_data[idx].key, &value)) {
                found++;
            }
        }

        double elapsed = timer.ElapsedMs();
        PrintResults("Baseline: Point Lookup (10K queries)", NUM_LOOKUPS, elapsed);
        std::cout << "  Found: " << found << " / " << NUM_LOOKUPS << std::endl;
    }

    // ========================================================================
    // BASELINE: Sequential Scan (simulated)
    // ========================================================================

    {
        SimpleMemTable memtable;

        // Populate
        for (const auto& rec : test_data) {
            memtable.Put(rec.key, rec.value);
        }

        Timer timer;

        size_t count = 0;
        for (const auto& rec : test_data) {
            std::string value;
            if (memtable.Get(rec.key, &value)) {
                count++;
            }
        }

        double elapsed = timer.ElapsedMs();
        PrintResults("Baseline: Sequential Scan", count, elapsed);
    }

    // ========================================================================
    // BASELINE: Equality Search (no index - linear scan)
    // ========================================================================

    {
        SimpleMemTable memtable;

        // Populate
        for (const auto& rec : test_data) {
            memtable.Put(rec.key, rec.value);
        }

        // Search for specific key (worst case: not found, scan all)
        uint64_t target_key = 999999999;  // Likely not in dataset

        Timer timer;

        size_t found = 0;
        for (const auto& rec : test_data) {
            std::string value;
            if (memtable.Get(rec.key, &value)) {
                if (rec.key == target_key) {
                    found++;
                    break;
                }
            }
        }

        double elapsed = timer.ElapsedMs();
        PrintResults("Baseline: Equality Search (no index)", test_data.size(), elapsed);
    }

    std::cout << "└────────────────────────────────────────────────────────────────────┘" << std::endl;
    std::cout << std::endl;

    // ========================================================================
    // Memory Usage
    // ========================================================================

    std::cout << "Memory Usage Estimate:" << std::endl;
    size_t total_data_size = NUM_RECORDS * (sizeof(uint64_t) + VALUE_SIZE);
    double total_mb = total_data_size / (1024.0 * 1024.0);

    std::cout << "  Records: " << NUM_RECORDS << std::endl;
    std::cout << "  Avg Value Size: " << VALUE_SIZE << " bytes" << std::endl;
    std::cout << "  Total Data: " << std::fixed << std::setprecision(2) << total_mb << " MB" << std::endl;
    std::cout << "  Estimated MemTable Size: ~" << (total_mb * 1.2) << " MB (with overhead)" << std::endl;
    std::cout << std::endl;

    // ========================================================================
    // Summary
    // ========================================================================

    std::cout << "====================================================================" << std::endl;
    std::cout << "Baseline Benchmark Complete!" << std::endl;
    std::cout << "====================================================================" << std::endl;
    std::cout << std::endl;

    std::cout << "Next Steps:" << std::endl;
    std::cout << "  1. These are BASELINE results (no optimization)" << std::endl;
    std::cout << "  2. Step 1: Implement Searchable MemTable with indexes" << std::endl;
    std::cout << "  3. Expected improvements:" << std::endl;
    std::cout << "     - Equality queries: 10-20x faster (with hash index)" << std::endl;
    std::cout << "     - Range queries: 10-100x faster (with B-tree index)" << std::endl;
    std::cout << "     - Write latency: +10-20% (index maintenance overhead)" << std::endl;
    std::cout << "     - Memory usage: +50% (index overhead)" << std::endl;
    std::cout << std::endl;

    std::cout << "To run full benchmark suite with Google Benchmark:" << std::endl;
    std::cout << "  cd build && cmake .. -DMARBLE_BUILD_BENCHMARKS=ON && make" << std::endl;
    std::cout << "  ./storage_techniques_bench" << std::endl;
    std::cout << std::endl;

    return 0;
}
