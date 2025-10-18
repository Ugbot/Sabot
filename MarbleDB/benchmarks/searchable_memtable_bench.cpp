/**
 * @file searchable_memtable_bench.cpp
 * @brief Benchmark for Searchable MemTable with indexes
 *
 * Compile: g++ -std=c++20 -O3 -I../include -o searchable_memtable_bench searchable_memtable_bench.cpp ../src/core/searchable_memtable.cpp ../src/core/memtable.cpp -lpthread
 * Run: ./searchable_memtable_bench
 */

#include <iostream>
#include <chrono>
#include <vector>
#include <random>
#include <iomanip>
#include <string>
#include <map>
#include <algorithm>
#include <sstream>
#include "marble/searchable_memtable.h"

using namespace marble;

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

// Test data generator with structured payload
class TestDataGenerator {
public:
    struct Record {
        uint64_t key;
        std::string value;
        std::string user_id;
        std::string event_type;
        uint64_t timestamp;
    };

    static std::vector<Record> GenerateRecords(size_t count) {
        std::vector<Record> records;
        records.reserve(count);

        std::random_device rd;
        std::mt19937_64 gen(rd());
        std::uniform_int_distribution<uint64_t> key_dist(1, 1000000);
        std::uniform_int_distribution<uint64_t> user_dist(1, 10000);
        std::uniform_int_distribution<int> event_dist(0, 4);
        std::uniform_int_distribution<uint64_t> time_dist(1697500000, 1697600000);

        const std::vector<std::string> events = {"click", "view", "purchase", "signup", "logout"};

        for (size_t i = 0; i < count; ++i) {
            Record rec;
            rec.key = i + 1;
            rec.user_id = "user_" + std::to_string(user_dist(gen));
            rec.event_type = events[event_dist(gen)];
            rec.timestamp = time_dist(gen);

            // Structured payload: "user_id:X|event_type:Y|timestamp:Z"
            std::ostringstream oss;
            oss << "user_id:" << rec.user_id << "|"
                << "event_type:" << rec.event_type << "|"
                << "timestamp:" << rec.timestamp;
            rec.value = oss.str();

            records.push_back(rec);
        }

        return records;
    }
};

// Print benchmark results
void PrintResults(const std::string& name, size_t operations, double elapsed_ms) {
    double ops_per_sec = (operations / elapsed_ms) * 1000.0;

    std::cout << std::left << std::setw(60) << name
              << std::right << std::setw(20);

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
    std::cout << "MarbleDB Searchable MemTable - Step 1 Benchmark" << std::endl;
    std::cout << "====================================================================" << std::endl;
    std::cout << std::endl;

    const size_t NUM_RECORDS = 100000;

    std::cout << "Generating " << NUM_RECORDS << " test records..." << std::endl;
    auto test_data = TestDataGenerator::GenerateRecords(NUM_RECORDS);
    std::cout << "Test data generated: " << test_data.size() << " records" << std::endl;
    std::cout << std::endl;

    // ========================================================================
    // Step 1: Searchable MemTable with Indexes
    // ========================================================================

    std::cout << "┌────────────────────────────────────────────────────────────────────────────────┐" << std::endl;
    std::cout << "│ Benchmark                                                  │ Throughput  │ Time     │" << std::endl;
    std::cout << "├────────────────────────────────────────────────────────────────────────────────┤" << std::endl;

    // Configure Searchable MemTable with indexes
    MemTableConfig config;
    config.enable_hash_index = true;
    config.enable_btree_index = true;
    config.indexes = {
        IndexConfig("user_id_idx", {"user_id"}, IndexConfig::kEager),
        IndexConfig("event_type_idx", {"event_type"}, IndexConfig::kEager),
        IndexConfig("timestamp_idx", {"timestamp"}, IndexConfig::kEager)
    };

    auto searchable_memtable = CreateSearchableMemTable(config);

    // ========================================================================
    // Write with Index Maintenance
    // ========================================================================

    {
        Timer timer;

        for (const auto& rec : test_data) {
            searchable_memtable->Put(rec.key, rec.value);
        }

        double elapsed = timer.ElapsedMs();
        PrintResults("Step 1: Write with Index Maintenance", test_data.size(), elapsed);
    }

    // ========================================================================
    // Equality Query (Hash Index)
    // ========================================================================

    {
        const size_t NUM_QUERIES = 10000;
        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_int_distribution<size_t> dist(0, test_data.size() - 1);

        // Collect unique user_ids for queries
        std::vector<std::string> query_user_ids;
        for (size_t i = 0; i < NUM_QUERIES; ++i) {
            query_user_ids.push_back(test_data[dist(gen)].user_id);
        }

        Timer timer;
        size_t total_found = 0;

        for (const auto& user_id : query_user_ids) {
            IndexQuery query;
            query.type = IndexQuery::kEquality;
            query.index_name = "user_id_idx";
            query.value = user_id;

            std::vector<MemTableEntry> results;
            searchable_memtable->SearchByIndex("user_id_idx", query, &results);
            total_found += results.size();
        }

        double elapsed = timer.ElapsedMs();
        PrintResults("Step 1: Equality Query (Hash Index, 10K queries)", NUM_QUERIES, elapsed);
        std::cout << "  Total results found: " << total_found << std::endl;
    }

    // ========================================================================
    // Range Query (B-Tree Index)
    // ========================================================================

    {
        const size_t NUM_QUERIES = 1000;

        Timer timer;
        size_t total_found = 0;

        // Range queries on timestamp
        for (size_t i = 0; i < NUM_QUERIES; ++i) {
            IndexQuery query;
            query.type = IndexQuery::kRange;
            query.index_name = "timestamp_idx";
            query.start_value = std::to_string(1697500000 + i * 100);
            query.end_value = std::to_string(1697500000 + i * 100 + 5000);

            std::vector<MemTableEntry> results;
            searchable_memtable->SearchByIndex("timestamp_idx", query, &results);
            total_found += results.size();
        }

        double elapsed = timer.ElapsedMs();
        PrintResults("Step 1: Range Query (B-Tree Index, 1K queries)", NUM_QUERIES, elapsed);
        std::cout << "  Total results found: " << total_found << std::endl;
    }

    // ========================================================================
    // Point Lookup (still fast)
    // ========================================================================

    {
        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_int_distribution<size_t> dist(0, test_data.size() - 1);

        const size_t NUM_LOOKUPS = 10000;
        std::vector<uint64_t> lookup_keys;
        for (size_t i = 0; i < NUM_LOOKUPS; ++i) {
            lookup_keys.push_back(test_data[dist(gen)].key);
        }

        Timer timer;
        size_t found = 0;

        for (uint64_t key : lookup_keys) {
            std::string value;
            if (searchable_memtable->Get(key, &value).ok()) {
                found++;
            }
        }

        double elapsed = timer.ElapsedMs();
        PrintResults("Step 1: Point Lookup (10K queries)", NUM_LOOKUPS, elapsed);
        std::cout << "  Found: " << found << " / " << NUM_LOOKUPS << std::endl;
    }

    std::cout << "└────────────────────────────────────────────────────────────────────────────────┘" << std::endl;
    std::cout << std::endl;

    // ========================================================================
    // Memory Usage
    // ========================================================================

    size_t memory_usage = searchable_memtable->GetMemoryUsage();
    auto index_stats = searchable_memtable->GetIndexStats();

    std::cout << "Memory Usage:" << std::endl;
    std::cout << "  Total: " << std::fixed << std::setprecision(2) << (memory_usage / (1024.0 * 1024.0)) << " MB" << std::endl;
    std::cout << std::endl;

    std::cout << "Index Statistics:" << std::endl;
    for (const auto& stats : index_stats) {
        std::cout << "  " << stats.name << ":" << std::endl;
        std::cout << "    Type: " << (stats.is_hash ? "Hash" : "") << (stats.is_btree ? " + B-Tree" : "") << std::endl;
        std::cout << "    Entries: " << stats.num_entries << std::endl;
        std::cout << "    Memory: " << std::fixed << std::setprecision(2) << (stats.memory_bytes / 1024.0) << " KB" << std::endl;
    }
    std::cout << std::endl;

    // ========================================================================
    // Summary
    // ========================================================================

    std::cout << "====================================================================" << std::endl;
    std::cout << "Step 1 Benchmark Complete!" << std::endl;
    std::cout << "====================================================================" << std::endl;
    std::cout << std::endl;

    std::cout << "Comparison with Baseline:" << std::endl;
    std::cout << "  Baseline equality search (no index): ~628K queries/sec" << std::endl;
    std::cout << "  Step 1 hash index equality search: MEASURED ABOVE" << std::endl;
    std::cout << "  Expected improvement: 10-20x faster" << std::endl;
    std::cout << std::endl;

    std::cout << "Next Steps:" << std::endl;
    std::cout << "  1. Compare equality query performance (should be 6-12M queries/sec)" << std::endl;
    std::cout << "  2. Verify memory overhead (+50% is acceptable)" << std::endl;
    std::cout << "  3. If validated → Proceed to Step 2 (Large Block Writes)" << std::endl;
    std::cout << std::endl;

    return 0;
}
