#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <memory>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <atomic>
#include <vector>
#include <fcntl.h>
#include <unistd.h>
#include "marble/memory_pool.h"

namespace marble {

/**
 * @brief Direct I/O writer with O_DIRECT flag
 *
 * Bypasses OS page cache for direct-to-disk writes.
 * Requires aligned buffers (provided by MemoryPool).
 *
 * Inspired by ScyllaDB's direct I/O implementation.
 */
class DirectIOWriter {
public:
    /**
     * @brief Open file for direct I/O writes
     *
     * @param filename Path to file
     * @param use_direct Enable O_DIRECT flag
     * @param use_datasync Use fdatasync instead of fsync (2x faster)
     */
    DirectIOWriter(const std::string& filename, bool use_direct = true, bool use_datasync = true);
    ~DirectIOWriter();

    /**
     * @brief Write data to file
     *
     * Buffer must be aligned (4096 bytes) for O_DIRECT.
     * Use MemoryPool to get aligned buffers.
     *
     * @param data Pointer to data (must be 4096-byte aligned)
     * @param size Size of data (must be multiple of 4096 for O_DIRECT)
     * @return Number of bytes written, or -1 on error
     */
    ssize_t Write(const void* data, size_t size);

    /**
     * @brief Sync data to disk
     *
     * Uses fdatasync (data only) or fsync (data + metadata).
     * fdatasync is 2x faster as it doesn't flush metadata.
     */
    int Sync();

    /**
     * @brief Get file descriptor
     */
    int GetFd() const { return fd_; }

    /**
     * @brief Check if using direct I/O
     */
    bool IsDirectIO() const { return use_direct_; }

    /**
     * @brief Check if using fdatasync
     */
    bool UseDataSync() const { return use_datasync_; }

    /**
     * @brief Get current file offset
     */
    off_t GetOffset() const { return offset_; }

    /**
     * @brief Get statistics
     */
    struct Stats {
        size_t total_writes;
        size_t total_syncs;
        size_t total_bytes_written;
        double total_write_time_ms;
        double total_sync_time_ms;
    };

    Stats GetStats() const { return stats_; }

private:
    int fd_;
    std::string filename_;
    bool use_direct_;
    bool use_datasync_;
    off_t offset_;
    Stats stats_;
};

/**
 * @brief Group commit coordinator
 *
 * Batches multiple writes and performs single fdatasync.
 * Amortizes sync overhead over multiple operations.
 *
 * Inspired by MySQL/PostgreSQL group commit and ScyllaDB batch writes.
 */
class GroupCommitWriter {
public:
    /**
     * @brief Pending write request
     */
    struct WriteRequest {
        const void* data;
        size_t size;
        bool completed;
        ssize_t result;
    };

    /**
     * @brief Create group commit writer
     *
     * @param filename Path to file
     * @param window_ms Group commit window in milliseconds (1-10ms recommended)
     * @param use_direct Enable O_DIRECT
     * @param use_datasync Use fdatasync instead of fsync
     */
    GroupCommitWriter(const std::string& filename, int window_ms = 1,
                     bool use_direct = true, bool use_datasync = true);
    ~GroupCommitWriter();

    /**
     * @brief Submit write request (non-blocking)
     *
     * Adds write to batch. Will be flushed when:
     * 1. Group commit window expires (window_ms)
     * 2. Batch is full (max_batch_size)
     * 3. Explicit Flush() call
     *
     * @param data Pointer to data (must be aligned for O_DIRECT)
     * @param size Size of data
     * @return Request ID for tracking
     */
    uint64_t SubmitWrite(const void* data, size_t size);

    /**
     * @brief Force flush of pending writes
     *
     * Writes all pending requests and syncs to disk.
     */
    void Flush();

    /**
     * @brief Get statistics
     */
    struct Stats {
        size_t total_submits;
        size_t total_flushes;
        size_t total_syncs;
        size_t total_bytes_written;
        double avg_batch_size;
        double avg_commit_latency_us;
    };

    Stats GetStats() const;

private:
    void FlushThread();  // Background flush thread

    std::unique_ptr<DirectIOWriter> writer_;
    int window_ms_;
    size_t max_batch_size_;

    std::vector<WriteRequest> pending_writes_;
    std::mutex mutex_;
    std::condition_variable cv_;
    std::thread flush_thread_;
    std::atomic<bool> stop_;

    // Statistics
    mutable std::mutex stats_mutex_;
    Stats stats_;
};

} // namespace marble
