/**
 * CacheStrategy implementation
 */

#include "marble/cache_strategy.h"
#include "marble/record.h"
#include "marble/table_capabilities.h"
#include <sstream>

namespace marble {

//==============================================================================
// NegativeCache implementation
//==============================================================================

void NegativeCache::RecordMiss(const std::string& key) {
    std::lock_guard<std::mutex> lock(mutex_);

    // Evict expired entries periodically
    if (misses_.size() >= capacity_) {
        EvictExpired();
    }

    // Evict LRU if still at capacity
    if (misses_.size() >= capacity_ && !lru_list_.empty()) {
        std::string evict_key = lru_list_.back();
        lru_list_.pop_back();
        misses_.erase(evict_key);
    }

    // Record miss with current timestamp
    uint64_t now = GetCurrentTimeMs();
    misses_[key] = {now};
    lru_list_.push_front(key);
}

bool NegativeCache::IsKnownMiss(const std::string& key) {
    std::lock_guard<std::mutex> lock(mutex_);

    auto it = misses_.find(key);
    if (it == misses_.end()) {
        return false;  // Not in negative cache
    }

    // Check if entry has expired
    uint64_t now = GetCurrentTimeMs();
    if (now - it->second.timestamp_ms > ttl_ms_) {
        // Expired, remove it
        misses_.erase(it);
        // Note: We don't bother removing from lru_list_ (will be cleaned up later)
        return false;
    }

    return true;  // Key is known to not exist
}

void NegativeCache::Clear() {
    std::lock_guard<std::mutex> lock(mutex_);
    misses_.clear();
    lru_list_.clear();
}

size_t NegativeCache::Size() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return misses_.size();
}

uint64_t NegativeCache::GetCurrentTimeMs() const {
    using namespace std::chrono;
    return duration_cast<milliseconds>(
        steady_clock::now().time_since_epoch()
    ).count();
}

void NegativeCache::EvictExpired() {
    // Must be called with mutex held
    uint64_t now = GetCurrentTimeMs();

    auto it = misses_.begin();
    while (it != misses_.end()) {
        if (now - it->second.timestamp_ms > ttl_ms_) {
            it = misses_.erase(it);
        } else {
            ++it;
        }
    }
}

//==============================================================================
// CacheStrategy implementation
//==============================================================================

CacheStrategy::CacheStrategy(size_t hot_key_capacity, size_t negative_cache_capacity)
    : hot_key_capacity_(hot_key_capacity),
      negative_cache_capacity_(negative_cache_capacity) {
    hot_tracker_ = std::make_unique<LRUCache<std::string, bool>>(hot_key_capacity);
    negative_cache_ = std::make_unique<NegativeCache>(negative_cache_capacity);
}

Status CacheStrategy::OnTableCreate(const TableCapabilities& caps) {
    // Check if hot key cache is enabled in capabilities
    if (!caps.enable_hot_key_cache) {
        // Cache disabled, but we'll keep the infrastructure in place
        // (just won't be used)
    }

    return Status::OK();
}

Status CacheStrategy::OnRead(ReadContext* ctx) {
    if (!ctx) {
        return Status::InvalidArgument("ReadContext is null");
    }

    // Only apply cache to point lookups
    if (!ctx->is_point_lookup) {
        return Status::OK();
    }

    stats_.num_lookups.fetch_add(1, std::memory_order_relaxed);

    std::string key_str = KeyToString(ctx->key);

    // Check negative cache first (cheaper than hot cache)
    if (negative_cache_->IsKnownMiss(key_str)) {
        stats_.num_negative_hits.fetch_add(1, std::memory_order_relaxed);
        ctx->definitely_not_found = true;
        return Status::NotFound("Negative cache: key not found");
    }

    // Check hot key tracker
    auto tracked = hot_tracker_->Get(key_str);
    if (tracked) {
        stats_.num_hot_hits.fetch_add(1, std::memory_order_relaxed);
        // Key is hot, but we don't cache the actual data
        // This is just a signal that the key is frequently accessed
    }

    // Cache miss
    stats_.num_misses.fetch_add(1, std::memory_order_relaxed);
    return Status::OK();
}

void CacheStrategy::OnReadComplete(const Key& key, const Record& record) {
    // Key was successfully read, track it as hot
    std::string key_str = KeyToString(key);

    auto true_val = std::make_shared<bool>(true);
    hot_tracker_->Put(key_str, true_val);

    stats_.num_inserts.fetch_add(1, std::memory_order_relaxed);
}

Status CacheStrategy::OnWrite(WriteContext* ctx) {
    if (!ctx) {
        return Status::InvalidArgument("WriteContext is null");
    }

    std::string key_str = KeyToString(ctx->key);

    // Remove from negative cache (key now exists)
    negative_cache_->Clear();  // Simple approach: clear entire negative cache on write

    // Invalidate hot key tracking (will be refreshed on next read)
    hot_tracker_->Remove(key_str);

    return Status::OK();
}

Status CacheStrategy::OnCompaction(CompactionContext* ctx) {
    if (!ctx) {
        return Status::InvalidArgument("CompactionContext is null");
    }

    // Clear trackers during compaction (data layout changes)
    hot_tracker_->Clear();
    negative_cache_->Clear();

    ctx->merge_caches = true;

    return Status::OK();
}

Status CacheStrategy::OnFlush(FlushContext* ctx) {
    if (!ctx) {
        return Status::InvalidArgument("FlushContext is null");
    }

    // No need to serialize tracker (transient data)
    // Tracker will be rebuilt from access patterns
    ctx->include_cache_hints = true;

    return Status::OK();
}

size_t CacheStrategy::MemoryUsage() const {
    // Rough estimate: each tracked key is ~64 bytes (key + bool)
    size_t hot_tracker_size = hot_tracker_->Size() * 64;
    size_t negative_cache_size = negative_cache_->Size() * 64;  // Just key + timestamp
    return hot_tracker_size + negative_cache_size + sizeof(*this);
}

void CacheStrategy::Clear() {
    hot_tracker_->Clear();
    negative_cache_->Clear();

    stats_.num_lookups.store(0, std::memory_order_relaxed);
    stats_.num_hot_hits.store(0, std::memory_order_relaxed);
    stats_.num_negative_hits.store(0, std::memory_order_relaxed);
    stats_.num_misses.store(0, std::memory_order_relaxed);
    stats_.num_inserts.store(0, std::memory_order_relaxed);
    stats_.num_evictions.store(0, std::memory_order_relaxed);
}

std::vector<uint8_t> CacheStrategy::Serialize() const {
    // Cache is transient, don't serialize
    return std::vector<uint8_t>();
}

Status CacheStrategy::Deserialize(const std::vector<uint8_t>& data) {
    // Cache is transient, nothing to deserialize
    return Status::OK();
}

std::string CacheStrategy::GetStats() const {
    std::ostringstream ss;

    uint64_t lookups = stats_.num_lookups.load(std::memory_order_relaxed);
    uint64_t hot_hits = stats_.num_hot_hits.load(std::memory_order_relaxed);
    uint64_t neg_hits = stats_.num_negative_hits.load(std::memory_order_relaxed);
    uint64_t misses = stats_.num_misses.load(std::memory_order_relaxed);
    uint64_t inserts = stats_.num_inserts.load(std::memory_order_relaxed);

    double hot_hit_rate = lookups > 0 ? static_cast<double>(hot_hits) / lookups : 0.0;
    double total_hit_rate = lookups > 0 ?
        static_cast<double>(hot_hits + neg_hits) / lookups : 0.0;

    ss << "{\n";
    ss << "  \"lookups\": " << lookups << ",\n";
    ss << "  \"hot_hits\": " << hot_hits << ",\n";
    ss << "  \"negative_hits\": " << neg_hits << ",\n";
    ss << "  \"misses\": " << misses << ",\n";
    ss << "  \"inserts\": " << inserts << ",\n";
    ss << "  \"hot_hit_rate\": " << hot_hit_rate << ",\n";
    ss << "  \"total_hit_rate\": " << total_hit_rate << ",\n";
    ss << "  \"hot_tracker_size\": " << hot_tracker_->Size() << ",\n";
    ss << "  \"negative_cache_size\": " << negative_cache_->Size() << ",\n";
    ss << "  \"memory_usage\": " << MemoryUsage() << "\n";
    ss << "}";

    return ss.str();
}

std::string CacheStrategy::KeyToString(const Key& key) const {
    // Simple conversion - use key address as string
    // In production, would serialize the actual key content
    std::ostringstream ss;
    ss << reinterpret_cast<uintptr_t>(&key);
    return ss.str();
}

}  // namespace marble
