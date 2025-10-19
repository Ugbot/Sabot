#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <vector>
#include <mutex>
#include <array>

namespace marble {

/**
 * @brief Fixed-size memory pool for zero-allocation buffer management
 *
 * Inspired by ScyllaDB/Seastar pool allocators. Pre-allocates memory chunks
 * and reuses them to eliminate malloc/free overhead.
 *
 * Thread-safe with optional locking (disabled for thread-local pools).
 */
class MemoryPool {
public:
    /**
     * @brief Pre-allocated memory chunk
     */
    struct Chunk {
        uint8_t* data;
        size_t size;
        bool in_use;
        Chunk* next;  // Free list
    };

    /**
     * @brief RAII wrapper for automatic return to pool
     */
    class Buffer {
    public:
        Buffer() : pool_(nullptr), chunk_(nullptr) {}

        Buffer(MemoryPool* pool, Chunk* chunk)
            : pool_(pool), chunk_(chunk) {
        }

        ~Buffer() {
            if (pool_ && chunk_) {
                pool_->Release(chunk_);
            }
        }

        // Move semantics only (no copy)
        Buffer(Buffer&& other) noexcept
            : pool_(other.pool_), chunk_(other.chunk_) {
            other.pool_ = nullptr;
            other.chunk_ = nullptr;
        }

        Buffer& operator=(Buffer&& other) noexcept {
            if (this != &other) {
                if (pool_ && chunk_) {
                    pool_->Release(chunk_);
                }
                pool_ = other.pool_;
                chunk_ = other.chunk_;
                other.pool_ = nullptr;
                other.chunk_ = nullptr;
            }
            return *this;
        }

        Buffer(const Buffer&) = delete;
        Buffer& operator=(const Buffer&) = delete;

        uint8_t* Data() { return chunk_ ? chunk_->data : nullptr; }
        const uint8_t* Data() const { return chunk_ ? chunk_->data : nullptr; }
        size_t Size() const { return chunk_ ? chunk_->size : 0; }
        bool IsValid() const { return chunk_ != nullptr; }

    private:
        MemoryPool* pool_;
        Chunk* chunk_;
    };

    /**
     * @brief Create memory pool with pre-allocated chunks
     *
     * @param chunk_size Size of each chunk in bytes
     * @param num_chunks Number of chunks to pre-allocate
     * @param thread_safe Enable thread-safety (adds mutex overhead)
     */
    MemoryPool(size_t chunk_size, size_t num_chunks, bool thread_safe = false);
    ~MemoryPool();

    /**
     * @brief Allocate a buffer from the pool
     * @return RAII buffer wrapper (auto-returns to pool on destruction)
     */
    Buffer Allocate();

    /**
     * @brief Get current statistics
     */
    struct Stats {
        size_t total_chunks;
        size_t free_chunks;
        size_t in_use_chunks;
        size_t total_bytes;
        size_t free_bytes;
    };

    Stats GetStats() const;

private:
    void Release(Chunk* chunk);

    size_t chunk_size_;
    size_t num_chunks_;
    bool thread_safe_;

    std::vector<Chunk> chunks_;
    Chunk* free_list_;  // Head of free list

    mutable std::mutex mutex_;
};

/**
 * @brief Multi-size memory pool manager
 *
 * Manages multiple pools for different sizes (ScyllaDB-style).
 * Automatically selects appropriate pool based on requested size.
 */
class MemoryPoolManager {
public:
    /**
     * @brief Standard pool sizes (optimized for database workloads)
     */
    static constexpr size_t NUM_POOLS = 6;
    static constexpr std::array<size_t, NUM_POOLS> POOL_SIZES = {
        512,              // Small records
        1024,             // 1KB records
        4096,             // 4KB page-aligned
        8192,             // 8KB blocks
        65536,            // 64KB large blocks
        1048576           // 1MB very large blocks
    };

    /**
     * @brief Create pool manager with default configuration
     *
     * @param chunks_per_pool Number of chunks to pre-allocate per pool
     * @param thread_safe Enable thread-safety
     */
    MemoryPoolManager(size_t chunks_per_pool = 1000, bool thread_safe = false);
    ~MemoryPoolManager() = default;

    /**
     * @brief Allocate buffer of requested size
     *
     * Selects smallest pool that fits the request.
     * Falls back to malloc if larger than largest pool.
     */
    MemoryPool::Buffer Allocate(size_t size);

    /**
     * @brief Get pool for specific size
     */
    MemoryPool* GetPool(size_t size);

    /**
     * @brief Get aggregate statistics across all pools
     */
    struct AggregateStats {
        size_t total_pools;
        size_t total_chunks;
        size_t total_free_chunks;
        size_t total_bytes;
        size_t total_free_bytes;
        std::array<MemoryPool::Stats, NUM_POOLS> pool_stats;
    };

    AggregateStats GetStats() const;

private:
    std::array<std::unique_ptr<MemoryPool>, NUM_POOLS> pools_;
    size_t chunks_per_pool_;
    bool thread_safe_;
};

/**
 * @brief Thread-local pool manager (zero-lock overhead)
 *
 * Each thread gets its own pool manager, eliminating all locking.
 * Best performance for thread-per-core architectures (ScyllaDB-style).
 */
class ThreadLocalPoolManager {
public:
    static MemoryPoolManager& Get();
    static MemoryPool::Buffer Allocate(size_t size);
};

} // namespace marble
