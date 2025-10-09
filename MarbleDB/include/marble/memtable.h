#pragma once

#include <memory>
#include <string>
#include <vector>
#include <unordered_map>
#include <map>
#include <mutex>
#include <marble/status.h>
#include <marble/sstable.h>

namespace marble {

/**
 * @brief MemTable entry - represents a key-value pair with operation type
 */
struct MemTableEntry {
    enum Operation {
        kPut,    // Insert/update operation
        kDelete  // Tombstone delete operation
    };

    uint64_t key;
    std::string value;
    Operation op;
    uint64_t timestamp;  // For conflict resolution and ordering

    MemTableEntry() : key(0), op(kPut), timestamp(0) {}
    MemTableEntry(uint64_t k, const std::string& v, Operation operation = kPut)
        : key(k), value(v), op(operation), timestamp(0) {}
};

/**
 * @brief MemTable - in-memory write buffer for LSM tree
 *
 * The MemTable provides:
 * - Fast in-memory writes using a sorted data structure
 * - Atomic operations with proper locking
 * - Size-based flushing triggers
 * - Snapshot isolation for concurrent readers
 */
class MemTable {
public:
    virtual ~MemTable() = default;

    /**
     * @brief Insert or update a key-value pair
     */
    virtual Status Put(uint64_t key, const std::string& value) = 0;

    /**
     * @brief Delete a key (tombstone operation)
     */
    virtual Status Delete(uint64_t key) = 0;

    /**
     * @brief Get a value by key
     */
    virtual Status Get(uint64_t key, std::string* value) const = 0;

    /**
     * @brief Check if key exists
     */
    virtual bool Contains(uint64_t key) const = 0;

    /**
     * @brief Get all entries in sorted order
     */
    virtual Status GetAllEntries(std::vector<MemTableEntry>* entries) const = 0;

    /**
     * @brief Get entries in a key range
     */
    virtual Status Scan(uint64_t start_key, uint64_t end_key,
                       std::vector<MemTableEntry>* entries) const = 0;

    /**
     * @brief Get approximate memory usage
     */
    virtual size_t GetMemoryUsage() const = 0;

    /**
     * @brief Get number of entries
     */
    virtual size_t GetEntryCount() const = 0;

    /**
     * @brief Check if MemTable should be flushed to disk
     */
    virtual bool ShouldFlush(size_t max_size_bytes) const = 0;

    /**
     * @brief Create a snapshot for consistent reads during flush
     */
    virtual std::unique_ptr<MemTable> CreateSnapshot() const = 0;

    /**
     * @brief Clear all entries (after successful flush)
     */
    virtual void Clear() = 0;

    /**
     * @brief Get statistics about the MemTable
     */
    virtual void GetStats(uint64_t* min_key, uint64_t* max_key,
                         size_t* entry_count, size_t* memory_usage) const = 0;
};

/**
 * @brief MemTable Factory - creates MemTable instances
 */
class MemTableFactory {
public:
    virtual ~MemTableFactory() = default;

    /**
     * @brief Create a new empty MemTable
     */
    virtual std::unique_ptr<MemTable> CreateMemTable() = 0;

    /**
     * @brief Create a MemTable from existing entries
     */
    virtual std::unique_ptr<MemTable> CreateMemTableFromEntries(
        const std::vector<MemTableEntry>& entries) = 0;
};

/**
 * @brief SkipList-based MemTable implementation
 *
 * Uses a concurrent skip list for efficient sorted storage with:
 * - O(log n) insertions and lookups
 * - Lock-free reads
 * - Atomic writes with proper synchronization
 */
class SkipListMemTable : public MemTable {
public:
    SkipListMemTable();
    ~SkipListMemTable() override;

    Status Put(uint64_t key, const std::string& value) override;
    Status Delete(uint64_t key) override;
    Status Get(uint64_t key, std::string* value) const override;
    bool Contains(uint64_t key) const override;
    Status GetAllEntries(std::vector<MemTableEntry>* entries) const override;
    Status Scan(uint64_t start_key, uint64_t end_key,
               std::vector<MemTableEntry>* entries) const override;
    size_t GetMemoryUsage() const override;
    size_t GetEntryCount() const override;
    bool ShouldFlush(size_t max_size_bytes) const override;
    std::unique_ptr<MemTable> CreateSnapshot() const override;
    void Clear() override;
    void GetStats(uint64_t* min_key, uint64_t* max_key,
                 size_t* entry_count, size_t* memory_usage) const override;

private:
    // Skip list node structure
    struct SkipNode {
        uint64_t key;
        MemTableEntry entry;
        std::vector<SkipNode*> forward;

        SkipNode(uint64_t k, const MemTableEntry& e, int level);
    };

    // Skip list implementation
    int GetRandomLevel() const;
    SkipNode* Find(uint64_t key) const;

    std::unique_ptr<SkipNode> header_;
    int max_level_;
    size_t entry_count_;
    size_t memory_usage_;
    mutable std::mutex mutex_;
    uint64_t min_key_;
    uint64_t max_key_;

    static constexpr int kMaxLevel = 32;
    static constexpr double kProbability = 0.25;
};

/**
 * @brief Standard MemTable Factory implementation
 */
class StandardMemTableFactory : public MemTableFactory {
public:
    std::unique_ptr<MemTable> CreateMemTable() override;
    std::unique_ptr<MemTable> CreateMemTableFromEntries(
        const std::vector<MemTableEntry>& entries) override;
};

// Factory functions
std::unique_ptr<MemTableFactory> CreateMemTableFactory();

} // namespace marble

