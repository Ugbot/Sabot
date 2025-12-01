#include "marble/memtable.h"
#include "marble/node_pool.h"
#include <random>
#include <cmath>
#include <cstring>
#include <algorithm>
#include <iostream>

namespace marble {

// Global shared node pool - allocated once for entire database
// This enables zero-allocation writes across all memtables
static std::shared_ptr<ObjectPool<SkipListSimpleMemTable::SkipNode>> GetGlobalNodePool() {
    static auto pool = std::make_shared<ObjectPool<SkipListSimpleMemTable::SkipNode>>(1000000);
    static bool initialized = false;
    if (!initialized) {
        std::cerr << "MarbleDB: Initialized global node pool with 1,000,000 nodes\n";
        initialized = true;
    }
    return pool;
}

// SkipListMemTable implementation
SkipListSimpleMemTable::SkipNode::SkipNode(uint64_t k, const SimpleMemTableEntry& e, int level)
    : key(k), entry(e), forward(level + 1, nullptr) {}

SkipListSimpleMemTable::SkipListSimpleMemTable()
    : max_level_(1), entry_count_(0), memory_usage_(0),
      min_key_(UINT64_MAX), max_key_(0) {
    // Use global shared node pool
    node_pool_ = GetGlobalNodePool();

    // Create header node with maximum possible level
    header_ = std::make_unique<SkipNode>(0, SimpleMemTableEntry(), kMaxLevel);
    allocated_nodes_.reserve(100000);  // Reserve space for 100K node tracking
}

SkipListSimpleMemTable::SkipListSimpleMemTable(std::shared_ptr<ObjectPool<SkipNode>> node_pool)
    : node_pool_(node_pool), max_level_(1), entry_count_(0), memory_usage_(0),
      min_key_(UINT64_MAX), max_key_(0) {
    // Create header node with maximum possible level
    header_ = std::make_unique<SkipNode>(0, SimpleMemTableEntry(), kMaxLevel);
    allocated_nodes_.reserve(100000);  // Reserve space for 100K node tracking
}

SkipListSimpleMemTable::~SkipListSimpleMemTable() {
    // Return all allocated nodes to the pool
    if (node_pool_ && !allocated_nodes_.empty()) {
        std::cerr << "SkipListSimpleMemTable: Returning " << allocated_nodes_.size() << " nodes to pool\n";
        for (SkipNode* node : allocated_nodes_) {
            if (node) {
                node->Reset();
            }
        }
        node_pool_->ReleaseBatch(allocated_nodes_);
        allocated_nodes_.clear();
    }
}

int SkipListSimpleMemTable::GetRandomLevel() const {
    int level = 0;
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_real_distribution<> dis(0.0, 1.0);

    while (dis(gen) < kProbability && level < kMaxLevel) {
        level++;
    }
    return level;
}

SkipListSimpleMemTable::SkipNode* SkipListSimpleMemTable::Find(uint64_t key) const {
    SkipNode* current = header_.get();

    // Start from the highest level
    for (int i = max_level_; i >= 0; --i) {
        while (current->forward[i] && current->forward[i]->key < key) {
            current = current->forward[i];
        }
    }

    // Move to the next node at level 0
    current = current->forward[0];

    // Check if we found the key
    if (current && current->key == key) {
        return current;
    }

    return nullptr;
}

Status SkipListSimpleMemTable::Put(uint64_t key, const std::string& value) {
    std::lock_guard<std::mutex> lock(mutex_);

    // Check if key already exists
    SkipNode* existing = Find(key);
    if (existing) {
        // Update existing entry
        size_t old_size = existing->entry.value.size();
        size_t new_size = value.size();

        existing->entry.value = value;
        existing->entry.op = SimpleMemTableEntry::kPut;
        existing->entry.timestamp = std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();

        memory_usage_ += (new_size - old_size);
        return Status::OK();
    }

    // Create new entry
    SimpleMemTableEntry entry(key, value, SimpleMemTableEntry::kPut);
    entry.timestamp = std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();

    int level = GetRandomLevel();
    if (level > max_level_) {
        max_level_ = level;
        // Extend header forward pointers
        header_->forward.resize(level + 1, nullptr);
    }

    // Acquire node from pool (zero-allocation design)
    SkipNode* new_node = node_pool_->Acquire();
    if (!new_node) {
        // Pool exhausted - memtable is full, trigger flush
        std::cerr << "SkipListSimpleMemTable: Node pool exhausted, memtable full\n";
        return Status::ResourceExhausted("MemTable full - node pool exhausted");
    }

    // Initialize the node
    new_node->Init(key, entry, level);

    // Track allocated node for cleanup
    allocated_nodes_.push_back(new_node);

    // Update key range
    min_key_ = std::min(min_key_, key);
    max_key_ = std::max(max_key_, key);

    SkipNode* current = header_.get();
    std::vector<SkipNode*> update(max_level_ + 1);

    // Find insertion points at each level
    for (int i = max_level_; i >= 0; --i) {
        while (current->forward[i] && current->forward[i]->key < key) {
            current = current->forward[i];
        }
        update[i] = current;
    }

    // Insert the new node
    for (int i = 0; i <= level; ++i) {
        new_node->forward[i] = update[i]->forward[i];
        update[i]->forward[i] = new_node;
    }

    // Update statistics
    entry_count_++;
    memory_usage_ += sizeof(SkipNode) + value.size() +
                    (level + 1) * sizeof(SkipNode*) +
                    sizeof(SimpleMemTableEntry);

    return Status::OK();
}

Status SkipListSimpleMemTable::Delete(uint64_t key) {
    std::lock_guard<std::mutex> lock(mutex_);

    SkipNode* existing = Find(key);
    if (existing) {
        // Mark as deleted
        existing->entry.op = SimpleMemTableEntry::kDelete;
        existing->entry.timestamp = std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();
        return Status::OK();
    }

    // Create tombstone entry
    SimpleMemTableEntry entry(key, "", SimpleMemTableEntry::kDelete);
    entry.timestamp = std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();

    int level = GetRandomLevel();
    if (level > max_level_) {
        max_level_ = level;
        header_->forward.resize(level + 1, nullptr);
    }

    auto new_node = std::make_unique<SkipNode>(key, entry, level);

    // Update key range
    min_key_ = std::min(min_key_, key);
    max_key_ = std::max(max_key_, key);

    SkipNode* current = header_.get();
    std::vector<SkipNode*> update(max_level_ + 1);

    // Find insertion points
    for (int i = max_level_; i >= 0; --i) {
        while (current->forward[i] && current->forward[i]->key < key) {
            current = current->forward[i];
        }
        update[i] = current;
    }

    // Insert the new node
    for (int i = 0; i <= level; ++i) {
        new_node->forward[i] = update[i]->forward[i];
        update[i]->forward[i] = new_node.get();
    }

    // Update statistics
    entry_count_++;
    memory_usage_ += sizeof(SkipNode) + (level + 1) * sizeof(std::unique_ptr<SkipNode>) +
                    sizeof(SimpleMemTableEntry);

    new_node.release();

    return Status::OK();
}

Status SkipListSimpleMemTable::Get(uint64_t key, std::string* value) const {
    std::lock_guard<std::mutex> lock(mutex_);

    SkipNode* node = Find(key);
    if (!node) {
        return Status::NotFound("Key not found");
    }

    // Check if it's a delete operation
    if (node->entry.op == SimpleMemTableEntry::kDelete) {
        return Status::NotFound("Key was deleted");
    }

    *value = node->entry.value;
    return Status::OK();
}

bool SkipListSimpleMemTable::Contains(uint64_t key) const {
    std::lock_guard<std::mutex> lock(mutex_);
    SkipNode* node = Find(key);
    return node && node->entry.op != SimpleMemTableEntry::kDelete;
}

bool SkipListSimpleMemTable::HasEntry(uint64_t key) const {
    std::lock_guard<std::mutex> lock(mutex_);
    // Returns true if key has ANY entry (Put or Delete tombstone)
    // This is used to determine if we should stop searching older memtables
    return Find(key) != nullptr;
}

Status SkipListSimpleMemTable::GetAllEntries(std::vector<SimpleMemTableEntry>* entries) const {
    std::lock_guard<std::mutex> lock(mutex_);

    entries->clear();
    entries->reserve(entry_count_);

    SkipNode* current = header_->forward[0];
    while (current) {
        entries->push_back(current->entry);
        current = current->forward[0];
    }

    return Status::OK();
}

Status SkipListSimpleMemTable::Scan(uint64_t start_key, uint64_t end_key,
                             std::vector<SimpleMemTableEntry>* entries) const {
    std::lock_guard<std::mutex> lock(mutex_);

    entries->clear();

    // Find the starting point
    SkipNode* current = header_.get();
    for (int i = max_level_; i >= 0; --i) {
        while (current->forward[i] && current->forward[i]->key < start_key) {
            current = current->forward[i];
        }
    }

    current = current->forward[0];

    // Scan until end_key
    while (current && current->key <= end_key) {
        entries->push_back(current->entry);
        current = current->forward[0];
    }

    return Status::OK();
}

size_t SkipListSimpleMemTable::GetMemoryUsage() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return memory_usage_;
}

size_t SkipListSimpleMemTable::GetEntryCount() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return entry_count_;
}

bool SkipListSimpleMemTable::ShouldFlush(size_t max_size_bytes) const {
    std::lock_guard<std::mutex> lock(mutex_);
    return memory_usage_ >= max_size_bytes;
}

std::unique_ptr<SimpleMemTable> SkipListSimpleMemTable::CreateSnapshot() const {
    std::lock_guard<std::mutex> lock(mutex_);

    auto snapshot = std::make_unique<SkipListSimpleMemTable>();

    // Copy all entries
    SkipNode* current = header_->forward[0];
    while (current) {
        snapshot->Put(current->key, current->entry.value);
        // Copy operation type and timestamp
        SkipNode* snapshot_node = snapshot->Find(current->key);
        if (snapshot_node) {
            snapshot_node->entry.op = current->entry.op;
            snapshot_node->entry.timestamp = current->entry.timestamp;
        }
        current = current->forward[0];
    }

    return snapshot;
}

void SkipListSimpleMemTable::Clear() {
    std::lock_guard<std::mutex> lock(mutex_);

    // Return all allocated nodes to the pool (zero-allocation design)
    if (node_pool_ && !allocated_nodes_.empty()) {
        std::cerr << "SkipListSimpleMemTable::Clear: Returning " << allocated_nodes_.size()
                  << " nodes to pool\n";
        for (SkipNode* node : allocated_nodes_) {
            if (node) {
                node->Reset();
            }
        }
        node_pool_->ReleaseBatch(allocated_nodes_);
        allocated_nodes_.clear();
    }

    // Reset header
    header_ = std::make_unique<SkipNode>(0, SimpleMemTableEntry(), kMaxLevel);
    max_level_ = 1;
    entry_count_ = 0;
    memory_usage_ = 0;
    min_key_ = UINT64_MAX;
    max_key_ = 0;
}

void SkipListSimpleMemTable::GetStats(uint64_t* min_key, uint64_t* max_key,
                               size_t* entry_count, size_t* memory_usage) const {
    std::lock_guard<std::mutex> lock(mutex_);
    *min_key = min_key_;
    *max_key = max_key_;
    *entry_count = entry_count_;
    *memory_usage = memory_usage_;
}

// StandardSimpleMemTableFactory implementation
std::unique_ptr<SimpleMemTable> StandardSimpleMemTableFactory::CreateMemTable() {
    return std::make_unique<SkipListSimpleMemTable>();
}

std::unique_ptr<SimpleMemTable> StandardSimpleMemTableFactory::CreateMemTableFromEntries(
    const std::vector<SimpleMemTableEntry>& entries) {
    auto memtable = std::make_unique<SkipListSimpleMemTable>();

    for (const auto& entry : entries) {
        if (entry.op == SimpleMemTableEntry::kPut) {
            memtable->Put(entry.key, entry.value);
        } else {
            memtable->Delete(entry.key);
        }
    }

    return memtable;
}

// Factory function
std::unique_ptr<SimpleMemTableFactory> CreateSimpleMemTableFactory() {
    return std::make_unique<StandardSimpleMemTableFactory>();
}

} // namespace marble

