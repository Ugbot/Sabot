#pragma once

// LRU Cache implementation
// High-performance LRU eviction with std::unordered_map

#include <unordered_map>
#include <cstdint>
#include <list>
#include <optional>

namespace sabot_ql {

// Simple LRU cache implementation
// Stores key-value pairs up to a configurable threshold
// Discards least recently used element when threshold exceeded
template <typename K, typename V>
class LRUCache {
private:
    size_t capacity_;
    // Stores keys in order of usage (MRU at front)
    std::list<K> keys_;
    std::unordered_map<K, std::pair<V, typename std::list<K>::iterator>> cache_;

public:
    explicit LRUCache(size_t capacity) : capacity_(capacity) {
        if (capacity == 0) {
            throw std::invalid_argument("Capacity must be greater than 0");
        }
    }

    // Check if key is in the cache and return value if found
    // Otherwise, return std::nullopt
    std::optional<V> Get(const K& key) {
        auto it = cache_.find(key);
        if (it == cache_.end()) {
            return std::nullopt;
        }
        // Move to front (most recently used)
        keys_.splice(keys_.begin(), keys_, it->second.second);
        return it->second.first;
    }

    // Add or update key-value pair
    void Put(const K& key, const V& value) {
        auto it = cache_.find(key);
        if (it != cache_.end()) {
            // Update existing
            it->second.first = value;
            keys_.splice(keys_.begin(), keys_, it->second.second);
            return;
        }

        // Add new entry
        if (cache_.size() >= capacity_) {
            // Evict LRU
            K lru_key = keys_.back();
            keys_.pop_back();
            cache_.erase(lru_key);
        }

        keys_.push_front(key);
        cache_[key] = {value, keys_.begin()};
    }

    void Clear() {
        cache_.clear();
        keys_.clear();
    }

    size_t Size() const {
        return cache_.size();
    }

    size_t Capacity() const {
        return capacity_;
    }
};

} // namespace sabot_ql
