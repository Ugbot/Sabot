#include "marble/searchable_memtable.h"
#include "marble/memtable.h"
#include <sstream>
#include <algorithm>

namespace marble {

// ============================================================================
// SearchableMemTable Implementation
// ============================================================================

SearchableMemTable::SearchableMemTable()
    : skip_list_(std::make_unique<SkipListSimpleMemTable>()) {
}

SearchableMemTable::SearchableMemTable(const MemTableConfig& config)
    : config_(config), skip_list_(std::make_unique<SkipListSimpleMemTable>()) {

    // Create indexes from config
    for (const auto& idx_config : config.indexes) {
        auto index = std::make_unique<InMemoryIndex>(
            idx_config,
            config.enable_hash_index,
            config.enable_btree_index
        );

        indexes_[idx_config.name] = std::move(index);
    }
}

SearchableMemTable::~SearchableMemTable() = default;

Status SearchableMemTable::Put(uint64_t key, const std::string& value) {
    // Write to underlying skip list
    auto status = skip_list_->Put(key, value);
    if (!status.ok()) {
        return status;
    }

    // Update secondary indexes
    return UpdateIndexes(key, value, false);
}

Status SearchableMemTable::Delete(uint64_t key) {
    // Get old value BEFORE deletion for proper index cleanup
    std::string old_value;
    auto get_status = skip_list_->Get(key, &old_value);

    // Mark as deleted in skip list
    auto status = skip_list_->Delete(key);
    if (!status.ok()) {
        return status;
    }

    // Update indexes with old value so index can find and remove the entry
    if (get_status.ok()) {
        return UpdateIndexes(key, old_value, true);
    }

    return Status::OK();
}

Status SearchableMemTable::Get(uint64_t key, std::string* value) const {
    return skip_list_->Get(key, value);
}

bool SearchableMemTable::Contains(uint64_t key) const {
    return skip_list_->Contains(key);
}

bool SearchableMemTable::HasEntry(uint64_t key) const {
    return skip_list_->HasEntry(key);
}

Status SearchableMemTable::GetAllEntries(std::vector<SimpleMemTableEntry>* entries) const {
    return skip_list_->GetAllEntries(entries);
}

Status SearchableMemTable::Scan(uint64_t start_key, uint64_t end_key,
                                std::vector<SimpleMemTableEntry>* entries) const {
    return skip_list_->Scan(start_key, end_key, entries);
}

size_t SearchableMemTable::GetMemoryUsage() const {
    size_t base_memory = skip_list_->GetMemoryUsage();

    std::lock_guard<std::mutex> lock(index_mutex_);

    // Add index memory
    size_t index_memory = 0;
    for (const auto& [name, index] : indexes_) {
        index_memory += index->GetMemoryUsage();
    }

    return base_memory + index_memory;
}

size_t SearchableMemTable::GetEntryCount() const {
    return skip_list_->GetEntryCount();
}

bool SearchableMemTable::ShouldFlush(size_t max_size_bytes) const {
    return skip_list_->ShouldFlush(max_size_bytes);
}

std::unique_ptr<SimpleMemTable> SearchableMemTable::CreateSnapshot() const {
    // Create snapshot of skip list
    auto snapshot = std::make_unique<SearchableMemTable>(config_);

    // Copy skip list data
    std::vector<SimpleMemTableEntry> entries;
    skip_list_->GetAllEntries(&entries);

    for (const auto& entry : entries) {
        snapshot->skip_list_->Put(entry.key, entry.value);
    }

    // Indexes will be rebuilt automatically via Put()
    return snapshot;
}

void SearchableMemTable::Clear() {
    skip_list_->Clear();

    std::lock_guard<std::mutex> lock(index_mutex_);
    for (auto& [name, index] : indexes_) {
        index->Clear();
    }
}

void SearchableMemTable::GetStats(uint64_t* min_key, uint64_t* max_key,
                                  size_t* entry_count, size_t* memory_usage) const {
    skip_list_->GetStats(min_key, max_key, entry_count, memory_usage);

    // Add index memory
    std::lock_guard<std::mutex> lock(index_mutex_);
    for (const auto& [name, index] : indexes_) {
        *memory_usage += index->GetMemoryUsage();
    }
}

// ============================================================================
// Index Operations
// ============================================================================

Status SearchableMemTable::SearchByIndex(const std::string& index_name,
                                        const IndexQuery& query,
                                        std::vector<SimpleMemTableEntry>* results) const {
    std::lock_guard<std::mutex> lock(index_mutex_);

    auto it = indexes_.find(index_name);
    if (it == indexes_.end()) {
        return Status::NotFound("Index not found: " + index_name);
    }

    const auto& index = it->second;

    // Collect matching keys
    std::vector<uint64_t> matching_keys;

    if (query.type == IndexQuery::kEquality) {
        // Hash index lookup (O(1))
        if (index->hash_index) {
            auto status = index->hash_index->Search(query.value, &matching_keys);
            if (!status.ok()) {
                return status;
            }
        } else {
            return Status::InvalidArgument("Hash index not enabled");
        }
    }
    else if (query.type == IndexQuery::kRange) {
        // B-tree range scan (O(log n + k))
        if (index->btree_index) {
            auto status = index->btree_index->SearchRange(
                query.start_value,
                query.end_value,
                &matching_keys
            );
            if (!status.ok()) {
                return status;
            }
        } else {
            return Status::InvalidArgument("B-tree index not enabled");
        }
    }

    // Fetch actual records from skip list
    results->clear();
    results->reserve(matching_keys.size());

    for (uint64_t key : matching_keys) {
        std::string value;
        auto status = skip_list_->Get(key, &value);

        if (status.ok()) {
            SimpleMemTableEntry entry;
            entry.key = key;
            entry.value = value;
            entry.op = SimpleMemTableEntry::kPut;
            entry.timestamp = 0;  // Would be populated in full impl
            results->push_back(entry);
        }
    }

    return Status::OK();
}

Status SearchableMemTable::Configure(const MemTableConfig& config) {
    config_ = config;

    std::lock_guard<std::mutex> lock(index_mutex_);

    // Clear existing indexes
    indexes_.clear();

    // Create new indexes
    for (const auto& idx_config : config.indexes) {
        auto index = std::make_unique<InMemoryIndex>(
            idx_config,
            config.enable_hash_index,
            config.enable_btree_index
        );

        indexes_[idx_config.name] = std::move(index);
    }

    // Rebuild indexes from existing data
    std::vector<SimpleMemTableEntry> entries;
    skip_list_->GetAllEntries(&entries);

    for (const auto& entry : entries) {
        UpdateIndexes(entry.key, entry.value, entry.op == SimpleMemTableEntry::kDelete);
    }

    return Status::OK();
}

std::vector<SearchableMemTable::IndexStats> SearchableMemTable::GetIndexStats() const {
    std::lock_guard<std::mutex> lock(index_mutex_);

    std::vector<IndexStats> stats;
    stats.reserve(indexes_.size());

    for (const auto& [name, index] : indexes_) {
        IndexStats s;
        s.name = name;
        s.is_hash = (index->hash_index != nullptr);
        s.is_btree = (index->btree_index != nullptr);
        s.memory_bytes = index->GetMemoryUsage();

        if (index->hash_index) {
            s.num_entries = index->hash_index->hash_map_.size();
        } else if (index->btree_index) {
            s.num_entries = index->btree_index->btree_.size();
        }

        stats.push_back(s);
    }

    return stats;
}

// ============================================================================
// Private Helper Methods
// ============================================================================

std::string SearchableMemTable::ExtractIndexValue(
    const std::string& payload,
    const std::vector<std::string>& columns) const {

    // NOTE: This is a simplified implementation
    // In production, would parse structured data (e.g., Arrow, JSON, Protobuf)

    // For benchmark purposes, simulate extraction
    // Assume payload format: "user_id:X|event_type:Y|timestamp:Z|..."

    for (const auto& column : columns) {
        // Find column in payload
        std::string prefix = column + ":";
        size_t pos = payload.find(prefix);

        if (pos != std::string::npos) {
            size_t start = pos + prefix.length();
            size_t end = payload.find('|', start);

            if (end == std::string::npos) {
                end = payload.length();
            }

            return payload.substr(start, end - start);
        }
    }

    // Fallback: hash payload as index value
    return std::to_string(std::hash<std::string>{}(payload));
}

Status SearchableMemTable::UpdateIndexes(uint64_t key,
                                        const std::string& value,
                                        bool is_delete) {
    std::lock_guard<std::mutex> lock(index_mutex_);

    for (auto& [name, index] : indexes_) {
        // Extract index value from payload
        std::string index_value = ExtractIndexValue(value, index->config.columns);

        if (is_delete) {
            index->Remove(index_value, key);
        } else {
            index->Insert(index_value, key);
        }
    }

    return Status::OK();
}

// ============================================================================
// Factory Implementation
// ============================================================================

std::unique_ptr<SimpleMemTable> SearchableMemTableFactory::CreateMemTable() {
    return std::make_unique<SearchableMemTable>(config_);
}

std::unique_ptr<SimpleMemTable> SearchableMemTableFactory::CreateMemTableFromEntries(
    const std::vector<SimpleMemTableEntry>& entries) {

    auto memtable = std::make_unique<SearchableMemTable>(config_);

    for (const auto& entry : entries) {
        if (entry.op == SimpleMemTableEntry::kPut) {
            memtable->Put(entry.key, entry.value);
        } else {
            memtable->Delete(entry.key);
        }
    }

    return memtable;
}

// Factory functions

std::unique_ptr<SimpleMemTableFactory> CreateSearchableMemTableFactory(
    const MemTableConfig& config) {
    return std::make_unique<SearchableMemTableFactory>(config);
}

std::unique_ptr<SearchableMemTable> CreateSearchableMemTable(
    const MemTableConfig& config) {
    return std::make_unique<SearchableMemTable>(config);
}

} // namespace marble
