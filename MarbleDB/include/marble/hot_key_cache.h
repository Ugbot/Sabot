/************************************************************************
MarbleDB Hot Key Cache - Aerospike-Inspired In-Memory Index

Provides fast point lookups for frequently accessed keys while maintaining
sparse index benefits for analytical queries.

Features:
- LRU eviction for hot key management
- Lock-free reads using RCU-style updates
- Adaptive promotion (access frequency tracking)
- Configurable memory budget
- Integration with sparse index fallback

Design inspired by Aerospike's primary index architecture.
**************************************************************************/

#pragma once

#include <marble/status.h>
#include <marble/record.h>
#include <memory>
#include <string>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <mutex>
#include <atomic>
#include <functional>

namespace marble {

// Forward declarations
class LSMSSTable;
class BloomFilter;

/**
 * @brief Hot key cache entry with access tracking
 */
struct HotKeyEntry {
    std::shared_ptr<Key> key;
    uint64_t sstable_offset = 0;  // Byte offset in SSTable file
    uint64_t row_index = 0;        // Row index for direct access
    uint32_t access_count = 0;     // Access frequency
    uint64_t last_access_time = 0; // For LRU eviction
    
    HotKeyEntry() = default;
    HotKeyEntry(std::shared_ptr<Key> k, uint64_t offset, uint64_t row)
        : key(std::move(k)), sstable_offset(offset), row_index(row), access_count(1) {}
};

/**
 * @brief Access frequency tracker for adaptive promotion
 */
class AccessTracker {
public:
    explicit AccessTracker(size_t window_size = 1000);
    
    // Record key access
    void RecordAccess(const std::string& key_str);
    
    // Check if key should be promoted to hot cache
    bool ShouldPromote(const std::string& key_str, uint32_t threshold = 3) const;
    
    // Get access count for a key
    uint32_t GetAccessCount(const std::string& key_str) const;
    
    // Reset statistics
    void Reset();
    
private:
    mutable std::mutex mutex_;
    std::unordered_map<std::string, uint32_t> access_counts_;
    size_t window_size_;
    size_t total_accesses_ = 0;
};

/**
 * @brief LRU eviction policy for hot key cache
 */
class LRUEvictionPolicy {
public:
    struct EvictionCandidate {
        std::string key_str;
        uint64_t last_access_time;
        uint32_t access_count;
    };
    
    // Find key to evict (LRU with access count weighting)
    std::string SelectVictim(const std::unordered_map<std::string, HotKeyEntry>& cache) const;
    
    // Update access time for a key
    void RecordAccess(HotKeyEntry* entry);
};

/**
 * @brief Hot key cache configuration
 */
struct HotKeyCacheConfig {
    // Memory management
    size_t max_entries = 10000;           // Maximum cached keys
    size_t target_memory_mb = 64;         // Target memory usage
    
    // Promotion thresholds
    uint32_t promotion_threshold = 3;     // Accesses needed to promote
    size_t access_window_size = 10000;    // Access tracking window
    
    // Eviction policy
    bool use_access_frequency = true;     // Weight by access count, not just LRU
    double frequency_weight = 0.3;        // How much to weight frequency vs recency
    
    // Performance tuning
    bool enable_prefetch = false;         // Prefetch adjacent keys
    size_t prefetch_distance = 10;        // Keys to prefetch ahead
    bool use_rcu_reads = true;            // Lock-free reads (RCU-style)
    
    // Integration
    bool fallback_to_sparse_index = true; // Use sparse index on cache miss
};

/**
 * @brief Statistics for hot key cache
 */
struct HotKeyCacheStats {
    uint64_t total_lookups = 0;
    uint64_t cache_hits = 0;
    uint64_t cache_misses = 0;
    uint64_t promotions = 0;
    uint64_t evictions = 0;
    uint64_t current_entries = 0;
    uint64_t memory_used_bytes = 0;
    
    double hit_rate() const {
        return total_lookups > 0 ? static_cast<double>(cache_hits) / total_lookups : 0.0;
    }
    
    double miss_rate() const {
        return 1.0 - hit_rate();
    }
};

/**
 * @brief Hot key cache for fast point lookups
 * 
 * Aerospike-inspired in-memory index that caches frequently accessed keys
 * while falling back to sparse index for analytical queries.
 */
class HotKeyCache {
public:
    explicit HotKeyCache(const HotKeyCacheConfig& config = HotKeyCacheConfig());
    ~HotKeyCache() = default;
    
    /**
     * @brief Lookup a key in the hot cache
     * 
     * @param key Key to lookup
     * @param entry Output parameter for cache entry
     * @return true if found in cache, false otherwise
     */
    bool Get(const Key& key, HotKeyEntry* entry);
    
    /**
     * @brief Insert or update a key in the cache
     * 
     * @param key Key to cache
     * @param sstable_offset Byte offset in SSTable
     * @param row_index Row index for direct access
     */
    void Put(std::shared_ptr<Key> key, uint64_t sstable_offset, uint64_t row_index);
    
    /**
     * @brief Record an access (for adaptive promotion)
     * 
     * @param key Key that was accessed
     */
    void RecordAccess(const Key& key);
    
    /**
     * @brief Check if a key should be promoted to cache
     * 
     * @param key Key to check
     * @return true if key should be cached
     */
    bool ShouldPromote(const Key& key) const;
    
    /**
     * @brief Evict a single entry (LRU)
     * 
     * @return true if eviction succeeded
     */
    bool Evict();
    
    /**
     * @brief Clear all cached entries
     */
    void Clear();
    
    /**
     * @brief Get cache statistics
     */
    HotKeyCacheStats GetStats() const;
    
    /**
     * @brief Prefetch adjacent keys (optional optimization)
     * 
     * @param key Anchor key for prefetching
     * @param distance Number of keys to prefetch ahead/behind
     */
    void Prefetch(const Key& key, size_t distance);
    
    /**
     * @brief Invalidate cache entry (on update/delete)
     * 
     * @param key Key to invalidate
     */
    void Invalidate(const Key& key);
    
    /**
     * @brief Bulk load hot keys (on SSTable open)
     * 
     * @param entries Vector of pre-identified hot keys
     */
    void BulkLoad(const std::vector<HotKeyEntry>& entries);
    
private:
    // Configuration
    HotKeyCacheConfig config_;
    
    // Cache storage (key_str -> entry)
    std::unordered_map<std::string, HotKeyEntry> cache_;
    mutable std::mutex cache_mutex_;  // For writes only if !use_rcu_reads
    
    // Access tracking for adaptive promotion
    AccessTracker access_tracker_;
    
    // Eviction policy
    LRUEvictionPolicy eviction_policy_;
    
    // Statistics
    mutable std::atomic<uint64_t> total_lookups_{0};
    mutable std::atomic<uint64_t> cache_hits_{0};
    mutable std::atomic<uint64_t> cache_misses_{0};
    mutable std::atomic<uint64_t> promotions_{0};
    mutable std::atomic<uint64_t> evictions_{0};
    
    // Helper methods
    bool ShouldEvict() const;
    size_t EstimateMemoryUsage() const;
    void MaybeEvict();
};

/**
 * @brief Hot key cache manager for multiple SSTables
 * 
 * Manages hot key caches across multiple SSTables in an LSM tree,
 * with global memory budget and coordination.
 */
class HotKeyCacheManager {
public:
    explicit HotKeyCacheManager(size_t global_memory_budget_mb = 256);
    
    /**
     * @brief Get or create cache for an SSTable
     * 
     * @param sstable_id Unique identifier for the SSTable
     * @return Pointer to hot key cache
     */
    std::shared_ptr<HotKeyCache> GetCache(const std::string& sstable_id);
    
    /**
     * @brief Remove cache for an SSTable (on compaction)
     * 
     * @param sstable_id SSTable identifier
     */
    void RemoveCache(const std::string& sstable_id);
    
    /**
     * @brief Get global statistics
     */
    HotKeyCacheStats GetGlobalStats() const;
    
    /**
     * @brief Rebalance memory across caches
     * 
     * Redistributes memory budget based on cache hit rates
     */
    void RebalanceMemory();
    
private:
    size_t global_memory_budget_mb_;
    std::unordered_map<std::string, std::shared_ptr<HotKeyCache>> caches_;
    mutable std::mutex manager_mutex_;
};

/**
 * @brief Negative cache for fast "key doesn't exist" checks
 * 
 * Remembers recently failed lookups to avoid redundant work.
 * Inspired by Aerospike's negative response caching.
 */
class NegativeCache {
public:
    explicit NegativeCache(size_t max_entries = 10000);
    
    /**
     * @brief Record a failed lookup
     */
    void RecordMiss(const Key& key);
    
    /**
     * @brief Check if key is known to not exist
     * 
     * @return true if key definitely doesn't exist
     */
    bool DefinitelyNotExists(const Key& key) const;
    
    /**
     * @brief Invalidate entry (on insert)
     */
    void Invalidate(const Key& key);
    
    /**
     * @brief Clear all entries
     */
    void Clear();
    
    /**
     * @brief Get statistics
     */
    struct Stats {
        uint64_t total_checks = 0;
        uint64_t negative_hits = 0;  // Keys found in negative cache
        uint64_t false_negatives = 0; // Keys thought missing but actually exist
        
        double hit_rate() const {
            return total_checks > 0 ? 
                static_cast<double>(negative_hits) / total_checks : 0.0;
        }
    };
    
    Stats GetStats() const;
    
private:
    std::unique_ptr<BloomFilter> negative_bloom_;
    std::unordered_set<std::string> recent_misses_;
    size_t max_entries_;
    mutable std::mutex mutex_;
    
    mutable std::atomic<uint64_t> total_checks_{0};
    mutable std::atomic<uint64_t> negative_hits_{0};
};

// Factory functions
std::unique_ptr<HotKeyCache> CreateHotKeyCache(
    const HotKeyCacheConfig& config = HotKeyCacheConfig());

std::unique_ptr<HotKeyCacheManager> CreateHotKeyCacheManager(
    size_t global_memory_budget_mb = 256);

std::unique_ptr<NegativeCache> CreateNegativeCache(
    size_t max_entries = 10000);

} // namespace marble

