#pragma once

#include <memory>
#include <vector>
#include <unordered_map>
#include <marble/lsm_tree.h>
#include <marble/hot_key_cache.h>
#include <marble/status.h>
#include <marble/record.h>

namespace marble {

/**
 * @brief Block-level bloom filter manager
 *
 * Maintains per-block bloom filters for fast negative lookups.
 * Each block contains ~8192 rows and has its own bloom filter.
 */
class BlockBloomFilterManager {
public:
    explicit BlockBloomFilterManager(size_t block_size = 8192, size_t bits_per_key = 10);

    /**
     * @brief Add a key to the bloom filter for a specific block
     */
    void AddKeyToBlock(size_t block_id, const Key& key);

    /**
     * @brief Check if a key might exist in a specific block
     *
     * @return true if key might exist (requires block scan)
     * @return false if key definitely doesn't exist (skip block)
     */
    bool BlockMayContainKey(size_t block_id, const Key& key) const;

    /**
     * @brief Get bloom filter for a specific block
     */
    std::shared_ptr<BloomFilter> GetBlockBloomFilter(size_t block_id) const;

    /**
     * @brief Finalize all bloom filters (call after all keys added)
     */
    void Finalize();

    /**
     * @brief Get total number of blocks
     */
    size_t GetBlockCount() const { return block_blooms_.size(); }

private:
    size_t block_size_;
    size_t bits_per_key_;
    std::unordered_map<size_t, std::shared_ptr<BloomFilter>> block_blooms_;
};

/**
 * @brief Enhanced SSTable metadata with block bloom filters
 */
struct EnhancedSSTableMetadata {
    // Block-level bloom filters
    std::vector<std::shared_ptr<BloomFilter>> block_blooms;

    // Block size (rows per block)
    size_t block_size = 8192;

    // Number of blocks
    size_t block_count = 0;

    // Enable features
    bool sorted_blocks_enabled = true;  // Already implemented
    bool block_bloom_filters_enabled = true;
    bool negative_cache_enabled = true;

    EnhancedSSTableMetadata() = default;
};

/**
 * @brief Optimized SSTable reader with block bloom filters and negative cache
 */
class OptimizedSSTableReader {
public:
    explicit OptimizedSSTableReader(
        std::shared_ptr<NegativeCache> negative_cache = nullptr);

    /**
     * @brief Read with block bloom filter optimization
     *
     * Uses block bloom filters to skip blocks that definitely don't contain the key.
     * Falls back to binary search within candidate blocks.
     */
    Status GetOptimized(
        const std::vector<std::pair<uint64_t, std::string>>& sorted_entries,
        const std::vector<std::shared_ptr<BloomFilter>>& block_blooms,
        uint64_t key,
        std::string* value,
        size_t block_size = 8192);

    /**
     * @brief Get statistics
     */
    struct Stats {
        uint64_t total_lookups = 0;
        uint64_t blocks_skipped = 0;
        uint64_t blocks_scanned = 0;
        uint64_t negative_cache_hits = 0;

        double skip_rate() const {
            return total_lookups > 0 ?
                static_cast<double>(blocks_skipped) / total_lookups : 0.0;
        }
    };

    Stats GetStats() const { return stats_; }

private:
    std::shared_ptr<NegativeCache> negative_cache_;
    mutable Stats stats_;

    /**
     * @brief Binary search within a block
     */
    Status BinarySearchInBlock(
        const std::vector<std::pair<uint64_t, std::string>>& entries,
        size_t block_start,
        size_t block_end,
        uint64_t key,
        std::string* value) const;
};

/**
 * @brief Helper to create block bloom filters during SSTable write
 */
class BlockBloomFilterBuilder {
public:
    explicit BlockBloomFilterBuilder(size_t block_size = 8192, size_t bits_per_key = 10);

    /**
     * @brief Build block bloom filters from sorted entries
     */
    std::vector<std::shared_ptr<BloomFilter>> BuildFromEntries(
        const std::vector<std::pair<uint64_t, std::string>>& sorted_entries);

private:
    size_t block_size_;
    size_t bits_per_key_;
};

} // namespace marble
