#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <memory>
#include <vector>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <atomic>
#include <functional>
#include <unordered_map>

// Platform detection
#ifdef __linux__
    #define MARBLE_HAS_IO_URING 1
    #include <liburing.h>
#else
    #define MARBLE_HAS_IO_URING 0
#endif

#include "marble/memory_pool.h"
#include "marble/direct_io_writer.h"

namespace marble {

/**
 * @brief Async I/O writer using io_uring (Linux only)
 *
 * On Linux: Uses io_uring for high-performance async I/O
 * On other platforms: Falls back to DirectIOWriter
 *
 * Inspired by ScyllaDB/Seastar io_uring implementation.
 */
class IOUringWriter {
public:
    /**
     * @brief Write completion callback
     */
    using CompletionCallback = std::function<void(ssize_t bytes_written, int error)>;

    /**
     * @brief Pending write request
     */
    struct WriteRequest {
        const void* data;
        size_t size;
        off_t offset;
        CompletionCallback callback;
        uint64_t id;
    };

    /**
     * @brief Create io_uring writer
     *
     * @param filename Path to file
     * @param queue_depth io_uring submission queue depth (default 256)
     * @param use_direct Enable O_DIRECT
     */
    IOUringWriter(const std::string& filename, size_t queue_depth = 256, bool use_direct = true);
    ~IOUringWriter();

    /**
     * @brief Submit async write request
     *
     * @param data Data to write (must be aligned for O_DIRECT)
     * @param size Size of data
     * @param offset File offset
     * @param callback Completion callback (optional)
     * @return Request ID
     */
    uint64_t SubmitWrite(const void* data, size_t size, off_t offset,
                        CompletionCallback callback = nullptr);

    /**
     * @brief Wait for all pending writes to complete
     */
    void WaitAll();

    /**
     * @brief Check if io_uring is available on this platform
     */
    static bool IsAvailable() {
#ifdef MARBLE_HAS_IO_URING
        return true;
#else
        return false;
#endif
    }

    /**
     * @brief Get statistics
     */
    struct Stats {
        size_t total_submits;
        size_t total_completions;
        size_t total_bytes_written;
        size_t pending_requests;
        double avg_completion_time_us;
    };

    Stats GetStats() const;

private:
    void ProcessCompletions();

    std::string filename_;
    int fd_;
    bool use_direct_;
    size_t queue_depth_;
    std::atomic<uint64_t> next_request_id_;

#if MARBLE_HAS_IO_URING
    struct io_uring ring_;
    bool ring_initialized_;
#else
    // Fallback to DirectIOWriter on non-Linux platforms
    std::unique_ptr<DirectIOWriter> fallback_writer_;
#endif

    // Statistics
    mutable std::mutex stats_mutex_;
    Stats stats_;

    // Pending requests tracking
    std::mutex requests_mutex_;
    std::unordered_map<uint64_t, WriteRequest> pending_requests_;
};

/**
 * @brief Multi-threaded I/O writer using io_uring
 *
 * Uses thread-per-core model to saturate NVMe SSDs.
 * Distributes writes across multiple io_uring instances.
 *
 * Inspired by ScyllaDB thread-per-core architecture.
 */
class MultiThreadedIOWriter {
public:
    /**
     * @brief Create multi-threaded writer
     *
     * @param filename Path to file
     * @param num_threads Number of I/O threads (default: CPU cores)
     * @param queue_depth Queue depth per thread
     * @param use_direct Enable O_DIRECT
     */
    MultiThreadedIOWriter(const std::string& filename, size_t num_threads = 0,
                         size_t queue_depth = 256, bool use_direct = true);
    ~MultiThreadedIOWriter();

    /**
     * @brief Submit write to thread pool
     *
     * Automatically selects least-loaded thread.
     *
     * @param data Data to write
     * @param size Size of data
     * @return Request ID
     */
    uint64_t SubmitWrite(const void* data, size_t size);

    /**
     * @brief Wait for all pending writes across all threads
     */
    void WaitAll();

    /**
     * @brief Get aggregate statistics
     */
    struct AggregateStats {
        size_t num_threads;
        size_t total_submits;
        size_t total_completions;
        size_t total_bytes_written;
        double total_throughput_mb_sec;
        std::vector<IOUringWriter::Stats> thread_stats;
    };

    AggregateStats GetStats() const;

private:
    size_t SelectThread();  // Round-robin or least-loaded

    std::string filename_;
    size_t num_threads_;
    std::vector<std::unique_ptr<IOUringWriter>> writers_;
    std::atomic<size_t> next_thread_;  // Round-robin counter
    std::unique_ptr<std::atomic<off_t>[]> thread_offsets_;  // Array of atomics (can't use vector)
};

/**
 * @brief Helper to determine optimal thread count for I/O
 */
size_t GetOptimalIOThreadCount();

} // namespace marble
