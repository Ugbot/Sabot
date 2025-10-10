/************************************************************************
MarbleDB Compaction Filters
Inspired by RocksDB's compaction filter mechanism

Enables custom logic during compaction:
- TTL expiration
- Tombstone cleanup
- Custom garbage collection
**************************************************************************/

#pragma once

#include <marble/status.h>
#include <marble/record.h>
#include <memory>
#include <string>
#include <chrono>

namespace marble {

/**
 * @brief Decision for compaction filter
 */
enum class CompactionFilterDecision {
    kKeep,          // Keep the key-value pair
    kRemove,        // Remove the key-value pair
    kChangeValue,   // Modify the value
    kRemoveAndSkipUntil  // Remove and skip keys until specified key
};

/**
 * @brief Context passed to compaction filter
 */
struct CompactionFilterContext {
    int level;  // LSM level being compacted
    bool is_full_compaction;
    bool is_manual_compaction;
    uint64_t file_number;
};

/**
 * @brief Base class for compaction filters
 * 
 * Allows custom logic to decide which keys to keep/drop during compaction.
 * Inspired by RocksDB's CompactionFilter.
 */
class CompactionFilter {
public:
    virtual ~CompactionFilter() = default;
    
    /**
     * @brief Filter a key-value pair during compaction
     * 
     * @param level LSM level being compacted
     * @param key The key
     * @param existing_value Current value
     * @param new_value Output: new value if decision is kChangeValue
     * @return Decision on what to do with this key
     * 
     * Example (TTL filter):
     *   if (IsExpired(key, existing_value)) {
     *       return kRemove;  // Drop expired keys
     *   }
     *   return kKeep;
     */
    virtual CompactionFilterDecision Filter(
        int level,
        const Key& key,
        const std::string& existing_value,
        std::string* new_value,
        bool* value_changed) const = 0;
    
    /**
     * @brief Name of this filter
     */
    virtual const char* Name() const = 0;
    
    /**
     * @brief Whether to ignore snapshots (advanced)
     * 
     * If true, filter can drop keys even if they're referenced by snapshots.
     * Use with caution!
     */
    virtual bool IgnoreSnapshots() const { return false; }
};

/**
 * @brief TTL (Time-To-Live) compaction filter
 * 
 * Removes keys older than specified TTL.
 * Requires timestamp in value or key.
 */
class TTLCompactionFilter : public CompactionFilter {
public:
    /**
     * @param ttl_seconds Keys older than this are removed
     * @param timestamp_field Field name containing timestamp (e.g., "created_at")
     */
    explicit TTLCompactionFilter(int64_t ttl_seconds, const std::string& timestamp_field = "ts")
        : ttl_seconds_(ttl_seconds), timestamp_field_(timestamp_field) {}
    
    CompactionFilterDecision Filter(
        int level,
        const Key& key,
        const std::string& existing_value,
        std::string* new_value,
        bool* value_changed) const override;
    
    const char* Name() const override { return "TTLCompactionFilter"; }

private:
    int64_t ttl_seconds_;
    std::string timestamp_field_;
    
    bool IsExpired(const std::string& value) const;
};

/**
 * @brief Tombstone cleanup filter
 * 
 * Removes tombstones after they've been compacted to bottom level.
 */
class TombstoneCleanupFilter : public CompactionFilter {
public:
    TombstoneCleanupFilter() = default;
    
    CompactionFilterDecision Filter(
        int level,
        const Key& key,
        const std::string& existing_value,
        std::string* new_value,
        bool* value_changed) const override;
    
    const char* Name() const override { return "TombstoneCleanupFilter"; }
    
    bool IgnoreSnapshots() const override { return false; }

private:
    static const int kMaxLevel = 6;  // Drop tombstones at bottom level
};

/**
 * @brief Custom value transformation filter
 * 
 * Allows modifying values during compaction (e.g., compression, encryption).
 */
class ValueTransformFilter : public CompactionFilter {
public:
    using TransformFunc = std::function<std::string(const std::string&)>;
    
    explicit ValueTransformFilter(TransformFunc transform_func)
        : transform_func_(transform_func) {}
    
    CompactionFilterDecision Filter(
        int level,
        const Key& key,
        const std::string& existing_value,
        std::string* new_value,
        bool* value_changed) const override;
    
    const char* Name() const override { return "ValueTransformFilter"; }

private:
    TransformFunc transform_func_;
};

/**
 * @brief Conditional delete filter
 * 
 * Drops keys matching a predicate.
 * 
 * Example: Delete all keys with prefix "temp_"
 */
class ConditionalDeleteFilter : public CompactionFilter {
public:
    using PredicateFunc = std::function<bool(const Key&, const std::string&)>;
    
    explicit ConditionalDeleteFilter(PredicateFunc predicate)
        : predicate_(predicate) {}
    
    CompactionFilterDecision Filter(
        int level,
        const Key& key,
        const std::string& existing_value,
        std::string* new_value,
        bool* value_changed) const override;
    
    const char* Name() const override { return "ConditionalDeleteFilter"; }

private:
    PredicateFunc predicate_;
};

/**
 * @brief Composite compaction filter
 * 
 * Chains multiple filters together.
 */
class CompositeCompactionFilter : public CompactionFilter {
public:
    CompositeCompactionFilter() = default;
    
    /**
     * @brief Add filter to chain
     */
    void AddFilter(std::shared_ptr<CompactionFilter> filter) {
        filters_.push_back(filter);
    }
    
    CompactionFilterDecision Filter(
        int level,
        const Key& key,
        const std::string& existing_value,
        std::string* new_value,
        bool* value_changed) const override;
    
    const char* Name() const override { return "CompositeCompactionFilter"; }

private:
    std::vector<std::shared_ptr<CompactionFilter>> filters_;
};

/**
 * @brief Factory for creating common compaction filters
 */
class CompactionFilterFactory {
public:
    /**
     * @brief Create TTL filter
     * 
     * @param ttl_seconds Drop keys older than this
     * @param timestamp_field Field containing timestamp
     */
    static std::shared_ptr<CompactionFilter> CreateTTLFilter(
        int64_t ttl_seconds,
        const std::string& timestamp_field = "ts"
    );
    
    /**
     * @brief Create tombstone cleanup filter
     */
    static std::shared_ptr<CompactionFilter> CreateTombstoneCleanup();
    
    /**
     * @brief Create conditional delete filter
     * 
     * @param predicate Function returning true if key should be deleted
     */
    static std::shared_ptr<CompactionFilter> CreateConditionalDelete(
        std::function<bool(const Key&, const std::string&)> predicate
    );
};

} // namespace marble

