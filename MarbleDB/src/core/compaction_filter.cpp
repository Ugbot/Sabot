/************************************************************************
MarbleDB Compaction Filter Implementation
**************************************************************************/

#include "marble/compaction_filter.h"
#include <chrono>
#include <sstream>

namespace marble {

// TTLCompactionFilter implementation
CompactionFilterDecision TTLCompactionFilter::Filter(
    int level,
    const Key& key,
    const std::string& existing_value,
    std::string* new_value,
    bool* value_changed) const {
    
    if (IsExpired(existing_value)) {
        return CompactionFilterDecision::kRemove;
    }
    
    return CompactionFilterDecision::kKeep;
}

bool TTLCompactionFilter::IsExpired(const std::string& value) const {
    // Parse timestamp from value
    // For now, assume value format: "timestamp:data"
    size_t colon_pos = value.find(':');
    if (colon_pos == std::string::npos) {
        return false;  // No timestamp, keep it
    }
    
    try {
        int64_t timestamp = std::stoll(value.substr(0, colon_pos));
        int64_t now = std::chrono::duration_cast<std::chrono::seconds>(
            std::chrono::system_clock::now().time_since_epoch()
        ).count();
        
        return (now - timestamp) > ttl_seconds_;
    } catch (...) {
        return false;  // Parse error, keep it
    }
}

// TombstoneCleanupFilter implementation
CompactionFilterDecision TombstoneCleanupFilter::Filter(
    int level,
    const Key& key,
    const std::string& existing_value,
    std::string* new_value,
    bool* value_changed) const {
    
    // Only drop tombstones at bottom level
    if (level >= kMaxLevel - 1) {
        // Check if this is a tombstone (empty value or special marker)
        if (existing_value.empty() || existing_value == "__TOMBSTONE__") {
            return CompactionFilterDecision::kRemove;
        }
    }
    
    return CompactionFilterDecision::kKeep;
}

// ValueTransformFilter implementation
CompactionFilterDecision ValueTransformFilter::Filter(
    int level,
    const Key& key,
    const std::string& existing_value,
    std::string* new_value,
    bool* value_changed) const {
    
    if (!transform_func_) {
        return CompactionFilterDecision::kKeep;
    }
    
    std::string transformed = transform_func_(existing_value);
    
    if (transformed != existing_value) {
        *new_value = transformed;
        *value_changed = true;
        return CompactionFilterDecision::kChangeValue;
    }
    
    return CompactionFilterDecision::kKeep;
}

// ConditionalDeleteFilter implementation
CompactionFilterDecision ConditionalDeleteFilter::Filter(
    int level,
    const Key& key,
    const std::string& existing_value,
    std::string* new_value,
    bool* value_changed) const {
    
    if (!predicate_) {
        return CompactionFilterDecision::kKeep;
    }
    
    if (predicate_(key, existing_value)) {
        return CompactionFilterDecision::kRemove;
    }
    
    return CompactionFilterDecision::kKeep;
}

// CompositeCompactionFilter implementation
CompactionFilterDecision CompositeCompactionFilter::Filter(
    int level,
    const Key& key,
    const std::string& existing_value,
    std::string* new_value,
    bool* value_changed) const {
    
    std::string current_value = existing_value;
    bool any_changed = false;
    
    for (const auto& filter : filters_) {
        bool this_changed = false;
        std::string temp_value;
        
        auto decision = filter->Filter(level, key, current_value, &temp_value, &this_changed);
        
        // Stop on first remove decision
        if (decision == CompactionFilterDecision::kRemove) {
            return CompactionFilterDecision::kRemove;
        }
        
        // Apply value change
        if (decision == CompactionFilterDecision::kChangeValue && this_changed) {
            current_value = temp_value;
            any_changed = true;
        }
    }
    
    if (any_changed) {
        *new_value = current_value;
        *value_changed = true;
        return CompactionFilterDecision::kChangeValue;
    }
    
    return CompactionFilterDecision::kKeep;
}

// CompactionFilterFactory implementation
std::shared_ptr<CompactionFilter> CompactionFilterFactory::CreateTTLFilter(
    int64_t ttl_seconds,
    const std::string& timestamp_field) {
    
    return std::make_shared<TTLCompactionFilter>(ttl_seconds, timestamp_field);
}

std::shared_ptr<CompactionFilter> CompactionFilterFactory::CreateTombstoneCleanup() {
    return std::make_shared<TombstoneCleanupFilter>();
}

std::shared_ptr<CompactionFilter> CompactionFilterFactory::CreateConditionalDelete(
    std::function<bool(const Key&, const std::string&)> predicate) {
    
    return std::make_shared<ConditionalDeleteFilter>(predicate);
}

} // namespace marble

