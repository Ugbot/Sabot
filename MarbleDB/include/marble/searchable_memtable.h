#pragma once

#include <marble/memtable.h>
#include <marble/status.h>
#include <string>
#include <vector>
#include <memory>
#include <map>
#include <unordered_map>
#include <mutex>

namespace marble {

/**
 * @brief Index configuration for MemTable secondary indexes
 */
struct IndexConfig {
    std::string name;
    std::vector<std::string> columns;

    enum BuildStrategy {
        kEager,        // Build immediately (default)
        kLazy,         // Build on first use
        kBackground    // Build in background thread
    };

    BuildStrategy build_strategy = kEager;

    IndexConfig() = default;
    IndexConfig(const std::string& n, const std::vector<std::string>& c, BuildStrategy s = kEager)
        : name(n), columns(c), build_strategy(s) {}
};

/**
 * @brief Index query specification
 */
struct IndexQuery {
    enum Type {
        kEquality,  // Exact match (uses hash index)
        kRange      // Range query (uses B-tree index)
    };

    Type type;
    std::string index_name;
    std::string value;         // For equality
    std::string start_value;   // For range
    std::string end_value;     // For range

    IndexQuery() : type(kEquality) {}
};

/**
 * @brief MemTable configuration with index support
 */
struct MemTableConfig {
    // Index configurations
    std::vector<IndexConfig> indexes;

    // Index types enabled
    bool enable_hash_index = true;   // For equality queries
    bool enable_btree_index = true;  // For range queries

    // Memory limits
    size_t max_index_memory_mb = 32;  // Limit index memory overhead

    MemTableConfig() = default;
};

/**
 * @brief Searchable MemTable with in-memory secondary indexes
 *
 * Extends base MemTable with:
 * - Hash indexes for O(1) equality queries
 * - B-tree indexes for O(log n) range queries
 * - Index maintenance on Put/Delete
 * - Query interface for indexed access
 */
class SearchableMemTable : public MemTable {
public:
    SearchableMemTable();
    explicit SearchableMemTable(const MemTableConfig& config);
    ~SearchableMemTable() override;

    // Base MemTable interface
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

    // NEW: Index operations
    /**
     * @brief Search using a secondary index
     */
    Status SearchByIndex(const std::string& index_name,
                         const IndexQuery& query,
                         std::vector<MemTableEntry>* results) const;

    /**
     * @brief Configure indexes
     */
    Status Configure(const MemTableConfig& config);

    /**
     * @brief Get index statistics
     */
    struct IndexStats {
        std::string name;
        size_t num_entries;
        size_t memory_bytes;
        bool is_hash;
        bool is_btree;
    };

    std::vector<IndexStats> GetIndexStats() const;

private:
    /**
     * @brief In-memory hash index for equality queries
     */
    struct HashIndex {
        std::string name;
        std::unordered_map<std::string, std::vector<uint64_t>> hash_map_;

        void Insert(const std::string& value, uint64_t key) {
            hash_map_[value].push_back(key);
        }

        void Remove(const std::string& value, uint64_t key) {
            auto it = hash_map_.find(value);
            if (it != hash_map_.end()) {
                auto& keys = it->second;
                keys.erase(std::remove(keys.begin(), keys.end(), key), keys.end());
                if (keys.empty()) {
                    hash_map_.erase(it);
                }
            }
        }

        Status Search(const std::string& value, std::vector<uint64_t>* keys) const {
            auto it = hash_map_.find(value);
            if (it != hash_map_.end()) {
                *keys = it->second;
                return Status::OK();
            }
            return Status::NotFound("Value not found in hash index");
        }

        size_t GetMemoryUsage() const {
            size_t memory = 0;
            for (const auto& [value, keys] : hash_map_) {
                memory += value.size();
                memory += keys.size() * sizeof(uint64_t);
            }
            return memory;
        }

        void Clear() {
            hash_map_.clear();
        }
    };

    /**
     * @brief In-memory B-tree index for range queries
     */
    struct BTreeIndex {
        std::string name;
        std::map<std::string, std::vector<uint64_t>> btree_;

        void Insert(const std::string& value, uint64_t key) {
            btree_[value].push_back(key);
        }

        void Remove(const std::string& value, uint64_t key) {
            auto it = btree_.find(value);
            if (it != btree_.end()) {
                auto& keys = it->second;
                keys.erase(std::remove(keys.begin(), keys.end(), key), keys.end());
                if (keys.empty()) {
                    btree_.erase(it);
                }
            }
        }

        Status SearchRange(const std::string& start_value,
                          const std::string& end_value,
                          std::vector<uint64_t>* keys) const {
            auto start_it = btree_.lower_bound(start_value);
            auto end_it = btree_.upper_bound(end_value);

            for (auto it = start_it; it != end_it; ++it) {
                keys->insert(keys->end(), it->second.begin(), it->second.end());
            }

            return Status::OK();
        }

        size_t GetMemoryUsage() const {
            size_t memory = 0;
            for (const auto& [value, keys] : btree_) {
                memory += value.size();
                memory += keys.size() * sizeof(uint64_t);
            }
            return memory;
        }

        void Clear() {
            btree_.clear();
        }
    };

    /**
     * @brief Internal index structure (combines hash + B-tree)
     */
    struct InMemoryIndex {
        IndexConfig config;
        std::unique_ptr<HashIndex> hash_index;
        std::unique_ptr<BTreeIndex> btree_index;

        InMemoryIndex(const IndexConfig& cfg, bool use_hash, bool use_btree)
            : config(cfg) {
            if (use_hash) {
                hash_index = std::make_unique<HashIndex>();
                hash_index->name = cfg.name;
            }
            if (use_btree) {
                btree_index = std::make_unique<BTreeIndex>();
                btree_index->name = cfg.name;
            }
        }

        Status Insert(const std::string& value, uint64_t key) {
            if (hash_index) {
                hash_index->Insert(value, key);
            }
            if (btree_index) {
                btree_index->Insert(value, key);
            }
            return Status::OK();
        }

        Status Remove(const std::string& value, uint64_t key) {
            if (hash_index) {
                hash_index->Remove(value, key);
            }
            if (btree_index) {
                btree_index->Remove(value, key);
            }
            return Status::OK();
        }

        size_t GetMemoryUsage() const {
            size_t memory = 0;
            if (hash_index) memory += hash_index->GetMemoryUsage();
            if (btree_index) memory += btree_index->GetMemoryUsage();
            return memory;
        }

        void Clear() {
            if (hash_index) hash_index->Clear();
            if (btree_index) btree_index->Clear();
        }
    };

    // Extract index value from payload
    // NOTE: This is a simplified version. In production, would parse structured data
    std::string ExtractIndexValue(const std::string& payload,
                                  const std::vector<std::string>& columns) const;

    // Update all indexes for a key-value pair
    Status UpdateIndexes(uint64_t key, const std::string& value, bool is_delete);

    // Underlying skip-list implementation
    std::unique_ptr<SkipListMemTable> skip_list_;

    // Secondary indexes
    std::unordered_map<std::string, std::unique_ptr<InMemoryIndex>> indexes_;

    // Configuration
    MemTableConfig config_;

    // Thread safety
    mutable std::mutex index_mutex_;
};

/**
 * @brief Searchable MemTable Factory
 */
class SearchableMemTableFactory : public MemTableFactory {
public:
    explicit SearchableMemTableFactory(const MemTableConfig& config = MemTableConfig())
        : config_(config) {}

    std::unique_ptr<MemTable> CreateMemTable() override;
    std::unique_ptr<MemTable> CreateMemTableFromEntries(
        const std::vector<MemTableEntry>& entries) override;

private:
    MemTableConfig config_;
};

// Factory functions
std::unique_ptr<MemTableFactory> CreateSearchableMemTableFactory(
    const MemTableConfig& config = MemTableConfig());

std::unique_ptr<SearchableMemTable> CreateSearchableMemTable(
    const MemTableConfig& config = MemTableConfig());

} // namespace marble
