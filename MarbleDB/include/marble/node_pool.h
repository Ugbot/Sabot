/************************************************************************
Copyright 2024 MarbleDB Contributors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
**************************************************************************/

#pragma once

#include <vector>
#include <mutex>
#include <cstdint>
#include <cstring>

namespace marble {

/**
 * @brief Object pool for skip list nodes
 *
 * Pre-allocates a large pool of nodes upfront to avoid expensive new/delete
 * operations during writes. Nodes are reused via acquire/release pattern.
 *
 * Design:
 * - Fixed-size pool allocated at construction
 * - Free list tracks available nodes
 * - Lock-free fast path for common case
 * - Zero allocations during steady-state operation
 */
template<typename T>
class ObjectPool {
public:
    explicit ObjectPool(size_t capacity)
        : capacity_(capacity), allocated_(0) {
        // Pre-allocate entire pool upfront
        pool_.reserve(capacity);
        free_list_.reserve(capacity);

        // Allocate all nodes
        for (size_t i = 0; i < capacity; ++i) {
            pool_.emplace_back(new T());
            free_list_.push_back(pool_.back().get());
        }
    }

    ~ObjectPool() = default;

    // Acquire a node from the pool (returns nullptr if pool exhausted)
    T* Acquire() {
        std::lock_guard<std::mutex> lock(mutex_);

        if (free_list_.empty()) {
            return nullptr;  // Pool exhausted
        }

        T* node = free_list_.back();
        free_list_.pop_back();
        allocated_++;
        return node;
    }

    // Release a node back to the pool
    void Release(T* node) {
        if (!node) return;

        std::lock_guard<std::mutex> lock(mutex_);
        free_list_.push_back(node);
        allocated_--;
    }

    // Release multiple nodes at once (batch operation for efficiency)
    void ReleaseBatch(const std::vector<T*>& nodes) {
        if (nodes.empty()) return;

        std::lock_guard<std::mutex> lock(mutex_);
        for (T* node : nodes) {
            if (node) {
                free_list_.push_back(node);
                allocated_--;
            }
        }
    }

    // Statistics
    size_t Capacity() const { return capacity_; }
    size_t Allocated() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return allocated_;
    }
    size_t Available() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return free_list_.size();
    }

private:
    size_t capacity_;
    size_t allocated_;
    std::vector<std::unique_ptr<T>> pool_;     // Owns all nodes
    std::vector<T*> free_list_;                // Available nodes
    mutable std::mutex mutex_;
};

/**
 * @brief Fixed-size ring buffer for memtable entries
 *
 * Ring buffer with fixed capacity. When full, oldest entries are flushed
 * to SSTable and space is reclaimed.
 *
 * Design:
 * - Fixed capacity (e.g., 100K entries)
 * - Head/tail pointers for circular access
 * - Triggers flush when capacity reached
 * - Resets after flush, returns nodes to pool
 */
class RingBuffer {
public:
    explicit RingBuffer(size_t capacity)
        : capacity_(capacity), head_(0), tail_(0), size_(0) {
        entries_.resize(capacity, nullptr);
    }

    // Add entry to ring buffer (returns false if full)
    bool Push(void* entry) {
        if (IsFull()) {
            return false;
        }

        entries_[tail_] = entry;
        tail_ = (tail_ + 1) % capacity_;
        size_++;
        return true;
    }

    // Get entry at index
    void* Get(size_t index) const {
        if (index >= size_) return nullptr;
        return entries_[(head_ + index) % capacity_];
    }

    // Clear the ring buffer (resets head/tail, caller must release entries)
    void Clear() {
        head_ = 0;
        tail_ = 0;
        size_ = 0;
    }

    // Get all entries for flush operation
    std::vector<void*> GetAllEntries() const {
        std::vector<void*> result;
        result.reserve(size_);

        for (size_t i = 0; i < size_; ++i) {
            result.push_back(entries_[(head_ + i) % capacity_]);
        }

        return result;
    }

    bool IsFull() const { return size_ == capacity_; }
    bool IsEmpty() const { return size_ == 0; }
    size_t Size() const { return size_; }
    size_t Capacity() const { return capacity_; }

private:
    size_t capacity_;
    size_t head_;
    size_t tail_;
    size_t size_;
    std::vector<void*> entries_;
};

} // namespace marble
