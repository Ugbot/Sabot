#include "marble/memory_pool.h"
#include <cstring>
#include <stdexcept>
#include <new>

namespace marble {

// ============================================================================
// MemoryPool Implementation
// ============================================================================

MemoryPool::MemoryPool(size_t chunk_size, size_t num_chunks, bool thread_safe)
    : chunk_size_(chunk_size),
      num_chunks_(num_chunks),
      thread_safe_(thread_safe),
      free_list_(nullptr) {

    // Pre-allocate all chunks
    chunks_.reserve(num_chunks);

    for (size_t i = 0; i < num_chunks; ++i) {
        Chunk chunk;

        // Allocate aligned memory (required for O_DIRECT)
        // Use posix_memalign for 4096-byte alignment
        void* ptr = nullptr;
        int ret = posix_memalign(&ptr, 4096, chunk_size);
        if (ret != 0) {
            throw std::bad_alloc();
        }

        chunk.data = static_cast<uint8_t*>(ptr);
        chunk.size = chunk_size;
        chunk.in_use = false;
        chunk.next = nullptr;

        chunks_.push_back(chunk);
    }

    // Build free list
    for (size_t i = 0; i < num_chunks; ++i) {
        chunks_[i].next = free_list_;
        free_list_ = &chunks_[i];
    }
}

MemoryPool::~MemoryPool() {
    // Free all chunks
    for (auto& chunk : chunks_) {
        std::free(chunk.data);
    }
}

MemoryPool::Buffer MemoryPool::Allocate() {
    if (thread_safe_) {
        std::lock_guard<std::mutex> lock(mutex_);

        if (!free_list_) {
            // Pool exhausted - could grow here, but for now return empty
            return Buffer();
        }

        Chunk* chunk = free_list_;
        free_list_ = chunk->next;
        chunk->in_use = true;
        chunk->next = nullptr;

        return Buffer(this, chunk);
    }
    else {
        // No locking (thread-local pool)
        if (!free_list_) {
            return Buffer();
        }

        Chunk* chunk = free_list_;
        free_list_ = chunk->next;
        chunk->in_use = true;
        chunk->next = nullptr;

        return Buffer(this, chunk);
    }
}

void MemoryPool::Release(Chunk* chunk) {
    if (!chunk) return;

    if (thread_safe_) {
        std::lock_guard<std::mutex> lock(mutex_);
        chunk->in_use = false;
        chunk->next = free_list_;
        free_list_ = chunk;
    }
    else {
        chunk->in_use = false;
        chunk->next = free_list_;
        free_list_ = chunk;
    }
}

MemoryPool::Stats MemoryPool::GetStats() const {
    Stats stats;
    stats.total_chunks = num_chunks_;
    stats.total_bytes = num_chunks_ * chunk_size_;

    if (thread_safe_) {
        std::lock_guard<std::mutex> lock(mutex_);

        size_t free_count = 0;
        Chunk* current = free_list_;
        while (current) {
            free_count++;
            current = current->next;
        }

        stats.free_chunks = free_count;
        stats.in_use_chunks = num_chunks_ - free_count;
        stats.free_bytes = free_count * chunk_size_;
    }
    else {
        size_t free_count = 0;
        Chunk* current = free_list_;
        while (current) {
            free_count++;
            current = current->next;
        }

        stats.free_chunks = free_count;
        stats.in_use_chunks = num_chunks_ - free_count;
        stats.free_bytes = free_count * chunk_size_;
    }

    return stats;
}

// ============================================================================
// MemoryPoolManager Implementation
// ============================================================================

MemoryPoolManager::MemoryPoolManager(size_t chunks_per_pool, bool thread_safe)
    : chunks_per_pool_(chunks_per_pool), thread_safe_(thread_safe) {

    // Create pools for each size
    for (size_t i = 0; i < NUM_POOLS; ++i) {
        pools_[i] = std::make_unique<MemoryPool>(
            POOL_SIZES[i],
            chunks_per_pool,
            thread_safe
        );
    }
}

MemoryPool::Buffer MemoryPoolManager::Allocate(size_t size) {
    // Find smallest pool that fits
    for (size_t i = 0; i < NUM_POOLS; ++i) {
        if (size <= POOL_SIZES[i]) {
            return pools_[i]->Allocate();
        }
    }

    // Size too large - would fall back to malloc in production
    // For now, return empty buffer
    return MemoryPool::Buffer();
}

MemoryPool* MemoryPoolManager::GetPool(size_t size) {
    for (size_t i = 0; i < NUM_POOLS; ++i) {
        if (size <= POOL_SIZES[i]) {
            return pools_[i].get();
        }
    }
    return nullptr;
}

MemoryPoolManager::AggregateStats MemoryPoolManager::GetStats() const {
    AggregateStats stats;
    stats.total_pools = NUM_POOLS;
    stats.total_chunks = 0;
    stats.total_free_chunks = 0;
    stats.total_bytes = 0;
    stats.total_free_bytes = 0;

    for (size_t i = 0; i < NUM_POOLS; ++i) {
        stats.pool_stats[i] = pools_[i]->GetStats();
        stats.total_chunks += stats.pool_stats[i].total_chunks;
        stats.total_free_chunks += stats.pool_stats[i].free_chunks;
        stats.total_bytes += stats.pool_stats[i].total_bytes;
        stats.total_free_bytes += stats.pool_stats[i].free_bytes;
    }

    return stats;
}

// ============================================================================
// ThreadLocalPoolManager Implementation
// ============================================================================

MemoryPoolManager& ThreadLocalPoolManager::Get() {
    // Thread-local pool manager (no locking needed)
    static thread_local MemoryPoolManager manager(1000, false);
    return manager;
}

MemoryPool::Buffer ThreadLocalPoolManager::Allocate(size_t size) {
    return Get().Allocate(size);
}

} // namespace marble
