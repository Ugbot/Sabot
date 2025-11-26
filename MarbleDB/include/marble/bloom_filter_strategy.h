/**
 * BloomFilterStrategy - Probabilistic membership testing
 *
 * Optimizes point lookups by filtering out keys that definitely don't exist.
 * Particularly effective for RDF triple stores and key-value workloads.
 *
 * Performance characteristics:
 * - Space: ~10 bits per key (1% false positive rate)
 * - Time: O(k) where k = number of hash functions (typically 7)
 * - False positive rate: Configurable (default 1%)
 * - False negative rate: 0% (never misses existing keys)
 */

#pragma once

#include "marble/optimization_strategy.h"
#include <vector>
#include <cstdint>
#include <atomic>
#include <memory>

namespace marble {

/**
 * Hash-based bloom filter implementation for optimization strategies.
 *
 * Uses k independent hash functions with bit array storage.
 * Thread-safe for concurrent reads and writes (mutex-based).
 * Optimized for pre-computed uint64_t hashes.
 */
class HashBloomFilter {
public:
    HashBloomFilter(size_t expected_keys, double false_positive_rate);
    ~HashBloomFilter() = default;

    // Add a key to the filter
    void Add(uint64_t hash);

    // Check if key might exist (false positives possible)
    bool MightContain(uint64_t hash) const;

    // Batch bloom filter check - returns bitmask of keys that might exist
    // More efficient than individual checks due to better cache locality
    std::vector<bool> MightContainBatch(const std::vector<uint64_t>& hashes) const;

    // Get current memory usage
    size_t MemoryUsage() const;

    // Get number of keys added (approximate)
    size_t NumKeys() const;

    // Get false positive rate (actual)
    double FalsePositiveRate() const;

    // Clear all bits
    void Clear();

    // Serialize/deserialize
    std::vector<uint8_t> Serialize() const;
    static std::unique_ptr<HashBloomFilter> Deserialize(const std::vector<uint8_t>& data);

private:
    size_t num_bits_;          // Total number of bits in filter
    size_t num_hash_functions_; // Number of hash functions (k)
    std::vector<uint8_t> bits_; // Bit array
    mutable std::mutex mutex_;  // Mutex for thread safety
    size_t num_keys_{0};        // Number of keys added

    // Get bit indices for a hash
    std::vector<size_t> GetBitIndices(uint64_t hash) const;

    // Set a bit (thread-safe)
    void SetBit(size_t index);

    // Check if bit is set (thread-safe)
    bool IsBitSet(size_t index) const;
};

/**
 * BloomFilterStrategy - Optimization strategy using bloom filters
 *
 * Integrates bloom filters into MarbleDB's read path to skip lookups
 * for keys that definitely don't exist.
 *
 * Use cases:
 * - RDF triple stores (check if triple exists before fetching)
 * - Key-value stores (check existence before disk I/O)
 * - Any workload with point lookups and significant misses
 */
class BloomFilterStrategy : public OptimizationStrategy {
public:
    /**
     * Create BloomFilterStrategy.
     *
     * @param expected_keys Expected number of keys (for sizing)
     * @param false_positive_rate Target false positive rate (default: 0.01 = 1%)
     */
    BloomFilterStrategy(size_t expected_keys = 1000000,
                        double false_positive_rate = 0.01);

    ~BloomFilterStrategy() override = default;

    //==========================================================================
    // OptimizationStrategy interface
    //==========================================================================

    Status OnTableCreate(const TableCapabilities& caps) override;

    Status OnRead(ReadContext* ctx) override;

    void OnReadComplete(const Key& key, const Record& record) override;

    Status OnWrite(WriteContext* ctx) override;

    Status OnCompaction(CompactionContext* ctx) override;

    Status OnFlush(FlushContext* ctx) override;

    size_t MemoryUsage() const override;

    void Clear() override;

    std::vector<uint8_t> Serialize() const override;

    Status Deserialize(const std::vector<uint8_t>& data) override;

    std::string Name() const override { return "BloomFilter"; }

    std::string GetStats() const override;

    //==========================================================================
    // Statistics
    //==========================================================================

    struct Stats {
        std::atomic<uint64_t> num_lookups{0};      // Total lookups
        std::atomic<uint64_t> num_bloom_hits{0};   // Bloom filter said "might exist"
        std::atomic<uint64_t> num_bloom_misses{0}; // Bloom filter said "definitely not"
        std::atomic<uint64_t> num_false_positives{0}; // Bloom said yes but key didn't exist
        std::atomic<uint64_t> num_keys_added{0};   // Keys added to filter
    };

    const Stats& GetStatsStruct() const { return stats_; }

private:
    std::unique_ptr<HashBloomFilter> bloom_filter_;
    double false_positive_rate_;
    size_t expected_keys_;
    Stats stats_;

    // Hash a key
    uint64_t HashKey(const Key& key) const;

    // Hash an Arrow Scalar for predicate checking
    uint64_t HashScalar(const std::shared_ptr<arrow::Scalar>& value) const;
};

}  // namespace marble
