/************************************************************************
MarbleDB Hot Key Cache Implementation

Aerospike-inspired in-memory index for frequently accessed keys.
**************************************************************************/

#include "marble/hot_key_cache.h"
#include "marble/lsm_tree.h"
#include <chrono>
#include <algorithm>
#include <iostream>

namespace marble {

// AccessTracker implementation
AccessTracker::AccessTracker(size_t window_size)
    : window_size_(window_size) {}

void AccessTracker::RecordAccess(const std::string& key_str) {
    std::lock_guard<std::mutex> lock(mutex_);
    access_counts_[key_str]++;
    total_accesses_++;
    
    // Reset window if it gets too large
    if (total_accesses_ > window_size_) {
        // Decay all counts by half
        for (auto& pair : access_counts_) {
            pair.second = pair.second / 2;
        }
        total_accesses_ = total_accesses_ / 2;
    }
}

bool AccessTracker::ShouldPromote(const std::string& key_str, uint32_t threshold) const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = access_counts_.find(key_str);
    return it != access_counts_.end() && it->second >= threshold;
}

uint32_t AccessTracker::GetAccessCount(const std::string& key_str) const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = access_counts_.find(key_str);
    return it != access_counts_.end() ? it->second : 0;
}

void AccessTracker::Reset() {
    std::lock_guard<std::mutex> lock(mutex_);
    access_counts_.clear();
    total_accesses_ = 0;
}

// LRUEvictionPolicy implementation
std::string LRUEvictionPolicy::SelectVictim(
    const std::unordered_map<std::string, HotKeyEntry>& cache) const {
    
    if (cache.empty()) {
        return "";
    }
    
    // Find entry with lowest score (oldest + least accessed)
    std::string victim_key;
    uint64_t lowest_score = UINT64_MAX;
    
    for (const auto& pair : cache) {
        // Score combines recency and frequency
        // Lower score = better eviction candidate
        uint64_t score = pair.second.last_access_time + 
                        (pair.second.access_count * 1000000ULL); // Weight frequency heavily
        
        if (score < lowest_score) {
            lowest_score = score;
            victim_key = pair.first;
        }
    }
    
    return victim_key;
}

void LRUEvictionPolicy::RecordAccess(HotKeyEntry* entry) {
    if (entry) {
        entry->access_count++;
        entry->last_access_time = std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::steady_clock::now().time_since_epoch()).count();
    }
}

// HotKeyCache implementation
HotKeyCache::HotKeyCache(const HotKeyCacheConfig& config)
    : config_(config)
    , access_tracker_(config.access_window_size) {}

bool HotKeyCache::Get(const Key& key, HotKeyEntry* entry) {
    total_lookups_++;
    
    std::string key_str = key.ToString();
    
    // Lock-free read if RCU enabled (for now, use mutex)
    std::lock_guard<std::mutex> lock(cache_mutex_);
    
    auto it = cache_.find(key_str);
    if (it != cache_.end()) {
        // Cache hit!
        cache_hits_++;
        *entry = it->second;
        
        // Update access metadata
        eviction_policy_.RecordAccess(&it->second);
        
        return true;
    }
    
    // Cache miss
    cache_misses_++;
    
    // Record access for potential promotion
    if (config_.fallback_to_sparse_index) {
        access_tracker_.RecordAccess(key_str);
    }
    
    return false;
}

void HotKeyCache::Put(std::shared_ptr<Key> key, uint64_t sstable_offset, uint64_t row_index) {
    std::lock_guard<std::mutex> lock(cache_mutex_);
    
    std::string key_str = key->ToString();
    
    // Check if we need to evict
    MaybeEvict();
    
    // Insert or update entry
    auto it = cache_.find(key_str);
    if (it != cache_.end()) {
        // Update existing entry
        it->second.sstable_offset = sstable_offset;
        it->second.row_index = row_index;
        eviction_policy_.RecordAccess(&it->second);
    } else {
        // Insert new entry
        HotKeyEntry entry(key, sstable_offset, row_index);
        eviction_policy_.RecordAccess(&entry);
        cache_[key_str] = entry;
        promotions_++;
    }
}

void HotKeyCache::RecordAccess(const Key& key) {
    access_tracker_.RecordAccess(key.ToString());
}

bool HotKeyCache::ShouldPromote(const Key& key) const {
    return access_tracker_.ShouldPromote(key.ToString(), config_.promotion_threshold);
}

bool HotKeyCache::Evict() {
    std::lock_guard<std::mutex> lock(cache_mutex_);
    
    if (cache_.empty()) {
        return false;
    }
    
    std::string victim = eviction_policy_.SelectVictim(cache_);
    if (!victim.empty()) {
        cache_.erase(victim);
        evictions_++;
        return true;
    }
    
    return false;
}

void HotKeyCache::Clear() {
    std::lock_guard<std::mutex> lock(cache_mutex_);
    cache_.clear();
    access_tracker_.Reset();
}

HotKeyCacheStats HotKeyCache::GetStats() const {
    std::lock_guard<std::mutex> lock(cache_mutex_);
    
    HotKeyCacheStats stats;
    stats.total_lookups = total_lookups_.load();
    stats.cache_hits = cache_hits_.load();
    stats.cache_misses = cache_misses_.load();
    stats.promotions = promotions_.load();
    stats.evictions = evictions_.load();
    stats.current_entries = cache_.size();
    stats.memory_used_bytes = EstimateMemoryUsage();
    
    return stats;
}

void HotKeyCache::Prefetch(const Key& key, size_t distance) {
    if (!config_.enable_prefetch) {
        return;
    }
    
    // TODO: Implement prefetching logic
    // Would prefetch keys adjacent to this one
    // Useful for sequential access patterns
}

void HotKeyCache::Invalidate(const Key& key) {
    std::lock_guard<std::mutex> lock(cache_mutex_);
    cache_.erase(key.ToString());
}

void HotKeyCache::BulkLoad(const std::vector<HotKeyEntry>& entries) {
    std::lock_guard<std::mutex> lock(cache_mutex_);
    
    for (const auto& entry : entries) {
        if (cache_.size() >= config_.max_entries) {
            break;
        }
        
        std::string key_str = entry.key->ToString();
        cache_[key_str] = entry;
    }
    
    promotions_ += entries.size();
}

bool HotKeyCache::ShouldEvict() const {
    // Check both entry count and memory usage
    if (cache_.size() >= config_.max_entries) {
        return true;
    }
    
    size_t memory_used = EstimateMemoryUsage();
    if (memory_used >= config_.target_memory_mb * 1024 * 1024) {
        return true;
    }
    
    return false;
}

size_t HotKeyCache::EstimateMemoryUsage() const {
    // Rough estimate: 
    // - Key string: ~32 bytes average
    // - Entry struct: ~48 bytes
    // - Map overhead: ~16 bytes per entry
    // Total: ~96 bytes per entry
    return cache_.size() * 96;
}

void HotKeyCache::MaybeEvict() {
    while (ShouldEvict() && !cache_.empty()) {
        Evict();
    }
}

// HotKeyCacheManager implementation
HotKeyCacheManager::HotKeyCacheManager(size_t global_memory_budget_mb)
    : global_memory_budget_mb_(global_memory_budget_mb) {}

std::shared_ptr<HotKeyCache> HotKeyCacheManager::GetCache(const std::string& sstable_id) {
    std::lock_guard<std::mutex> lock(manager_mutex_);
    
    auto it = caches_.find(sstable_id);
    if (it != caches_.end()) {
        return it->second;
    }
    
    // Create new cache with proportional memory budget
    HotKeyCacheConfig config;
    config.target_memory_mb = global_memory_budget_mb_ / std::max(size_t(1), caches_.size() + 1);
    
    auto cache = std::make_shared<HotKeyCache>(config);
    caches_[sstable_id] = cache;
    
    return cache;
}

void HotKeyCacheManager::RemoveCache(const std::string& sstable_id) {
    std::lock_guard<std::mutex> lock(manager_mutex_);
    caches_.erase(sstable_id);
}

HotKeyCacheStats HotKeyCacheManager::GetGlobalStats() const {
    std::lock_guard<std::mutex> lock(manager_mutex_);
    
    HotKeyCacheStats global_stats;
    
    for (const auto& pair : caches_) {
        auto stats = pair.second->GetStats();
        global_stats.total_lookups += stats.total_lookups;
        global_stats.cache_hits += stats.cache_hits;
        global_stats.cache_misses += stats.cache_misses;
        global_stats.promotions += stats.promotions;
        global_stats.evictions += stats.evictions;
        global_stats.current_entries += stats.current_entries;
        global_stats.memory_used_bytes += stats.memory_used_bytes;
    }
    
    return global_stats;
}

void HotKeyCacheManager::RebalanceMemory() {
    std::lock_guard<std::mutex> lock(manager_mutex_);
    
    if (caches_.empty()) {
        return;
    }
    
    // Calculate total hit rate for each cache
    struct CacheMetrics {
        std::string id;
        double hit_rate;
        size_t current_entries;
    };
    
    std::vector<CacheMetrics> metrics;
    for (const auto& pair : caches_) {
        auto stats = pair.second->GetStats();
        metrics.push_back({pair.first, stats.hit_rate(), stats.current_entries});
    }
    
    // Sort by hit rate (descending)
    std::sort(metrics.begin(), metrics.end(), 
             [](const CacheMetrics& a, const CacheMetrics& b) {
                 return a.hit_rate > b.hit_rate;
             });
    
    // Allocate more memory to caches with higher hit rates
    // TODO: Implement dynamic rebalancing
    // For now, equal distribution
}

// HotKeyNegativeCache implementation
HotKeyNegativeCache::HotKeyNegativeCache(size_t max_entries)
    : max_entries_(max_entries) {
    // Create bloom filter for negative lookups
    // Use 8 bits per key for ~3% false positive rate
    // negative_bloom_ = std::make_unique<BloomFilter>(8, max_entries); // TODO: Re-enable
}

void HotKeyNegativeCache::RecordMiss(const Key& key) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    std::string key_str = key.ToString();
    
    // Add to bloom filter
    // negative_bloom_->Add(key); // TODO: Re-enable
    
    // Add to recent misses set
    recent_misses_.insert(key_str);
    
    // Evict oldest if over limit
    if (recent_misses_.size() > max_entries_) {
        // Simple eviction - remove first element
        // In production, would use proper LRU
        recent_misses_.erase(recent_misses_.begin());
    }
}

bool HotKeyNegativeCache::DefinitelyNotExists(const Key& key) const {
    total_checks_++;
    
    // Quick check: bloom filter
    // TODO: Re-enable bloom filter
    /*
    if (!negative_bloom_->MayContain(key)) {
        return false;  // Might exist (not in negative cache)
    }
    */
    
    // Detailed check: recent misses set
    std::lock_guard<std::mutex> lock(mutex_);
    std::string key_str = key.ToString();
    
    if (recent_misses_.find(key_str) != recent_misses_.end()) {
        negative_hits_++;
        return true;  // Definitely doesn't exist (we failed to find it recently)
    }
    
    return false;  // Bloom filter false positive
}

void HotKeyNegativeCache::Invalidate(const Key& key) {
    std::lock_guard<std::mutex> lock(mutex_);
    std::string key_str = key.ToString();
    recent_misses_.erase(key_str);
    
    // Note: Can't remove from bloom filter
    // Bloom filter will have false positives until recreated
}

void HotKeyNegativeCache::Clear() {
    std::lock_guard<std::mutex> lock(mutex_);
    recent_misses_.clear();

    // Recreate bloom filter
    // negative_bloom_ = std::make_unique<BloomFilter>(8, max_entries_); // TODO: Re-enable
}

HotKeyNegativeCache::Stats HotKeyNegativeCache::GetStats() const {
    Stats stats;
    stats.total_checks = total_checks_.load();
    stats.negative_hits = negative_hits_.load();
    return stats;
}

// Factory functions
std::unique_ptr<HotKeyCache> CreateHotKeyCache(const HotKeyCacheConfig& config) {
    return std::make_unique<HotKeyCache>(config);
}

std::unique_ptr<HotKeyCacheManager> CreateHotKeyCacheManager(size_t global_memory_budget_mb) {
    return std::make_unique<HotKeyCacheManager>(global_memory_budget_mb);
}

std::unique_ptr<HotKeyNegativeCache> CreateHotKeyNegativeCache(size_t max_entries) {
    return std::make_unique<HotKeyNegativeCache>(max_entries);
}

} // namespace marble

