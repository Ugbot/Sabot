#include "marble/io_uring_writer.h"
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <errno.h>
#include <cstring>
#include <stdexcept>
#include <chrono>

#ifdef __linux__
    #include <sys/uio.h>
#endif

namespace marble {

// ============================================================================
// IOUringWriter Implementation
// ============================================================================

IOUringWriter::IOUringWriter(const std::string& filename, size_t queue_depth, bool use_direct)
    : filename_(filename), fd_(-1), use_direct_(use_direct),
      queue_depth_(queue_depth), next_request_id_(0) {

    memset(&stats_, 0, sizeof(stats_));

#if MARBLE_HAS_IO_URING
    ring_initialized_ = false;

    // Open file with appropriate flags
    int flags = O_WRONLY | O_CREAT | O_TRUNC;
    if (use_direct) {
        flags |= O_DIRECT;
    }

    fd_ = open(filename.c_str(), flags, 0644);
    if (fd_ < 0) {
        throw std::runtime_error("Failed to open file: " + filename + ", error: " + std::strerror(errno));
    }

    // Initialize io_uring
    int ret = io_uring_queue_init(queue_depth, &ring_, 0);
    if (ret < 0) {
        close(fd_);
        throw std::runtime_error("Failed to initialize io_uring: " + std::string(std::strerror(-ret)));
    }

    ring_initialized_ = true;

#else
    // Fallback to DirectIOWriter on non-Linux platforms
    fallback_writer_ = std::make_unique<DirectIOWriter>(filename, use_direct, true);
#endif
}

IOUringWriter::~IOUringWriter() {
#if MARBLE_HAS_IO_URING
    if (ring_initialized_) {
        WaitAll();  // Wait for pending requests
        io_uring_queue_exit(&ring_);
    }
    if (fd_ >= 0) {
        close(fd_);
    }
#endif
}

uint64_t IOUringWriter::SubmitWrite(const void* data, size_t size, off_t offset,
                                   CompletionCallback callback) {
    uint64_t request_id = next_request_id_.fetch_add(1);

#if MARBLE_HAS_IO_URING
    if (!ring_initialized_) {
        if (callback) {
            callback(-1, EINVAL);
        }
        return request_id;
    }

    // Get submission queue entry
    struct io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
    if (!sqe) {
        // Queue full - submit and retry
        io_uring_submit(&ring_);
        sqe = io_uring_get_sqe(&ring_);
        if (!sqe) {
            if (callback) {
                callback(-1, EAGAIN);
            }
            return request_id;
        }
    }

    // Prepare write operation
    io_uring_prep_write(sqe, fd_, data, size, offset);
    io_uring_sqe_set_data(sqe, reinterpret_cast<void*>(request_id));

    // Store request for completion tracking
    {
        std::lock_guard<std::mutex> lock(requests_mutex_);
        WriteRequest req;
        req.data = data;
        req.size = size;
        req.offset = offset;
        req.callback = callback;
        req.id = request_id;
        pending_requests_[request_id] = req;
    }

    // Submit to kernel
    io_uring_submit(&ring_);

    {
        std::lock_guard<std::mutex> lock(stats_mutex_);
        stats_.total_submits++;
        stats_.pending_requests = pending_requests_.size();
    }

    // Process any completions
    ProcessCompletions();

    return request_id;

#else
    // Fallback to DirectIOWriter
    auto start = std::chrono::high_resolution_clock::now();

    ssize_t written = fallback_writer_->Write(data, size);

    auto end = std::chrono::high_resolution_clock::now();
    double elapsed_us = std::chrono::duration<double, std::micro>(end - start).count();

    {
        std::lock_guard<std::mutex> lock(stats_mutex_);
        stats_.total_submits++;
        stats_.total_completions++;
        if (written > 0) {
            stats_.total_bytes_written += written;
        }
        stats_.avg_completion_time_us =
            (stats_.avg_completion_time_us * (stats_.total_completions - 1) + elapsed_us) /
            stats_.total_completions;
    }

    if (callback) {
        callback(written, written < 0 ? errno : 0);
    }

    return request_id;
#endif
}

void IOUringWriter::ProcessCompletions() {
#if MARBLE_HAS_IO_URING
    struct io_uring_cqe* cqe;
    unsigned head;
    unsigned count = 0;

    // Process all available completions
    io_uring_for_each_cqe(&ring_, head, cqe) {
        uint64_t request_id = reinterpret_cast<uint64_t>(io_uring_cqe_get_data(cqe));

        WriteRequest req;
        {
            std::lock_guard<std::mutex> lock(requests_mutex_);
            auto it = pending_requests_.find(request_id);
            if (it != pending_requests_.end()) {
                req = it->second;
                pending_requests_.erase(it);
            }
        }

        // Update statistics
        {
            std::lock_guard<std::mutex> lock(stats_mutex_);
            stats_.total_completions++;
            if (cqe->res > 0) {
                stats_.total_bytes_written += cqe->res;
            }
            stats_.pending_requests = pending_requests_.size();
        }

        // Call completion callback
        if (req.callback) {
            req.callback(cqe->res, cqe->res < 0 ? -cqe->res : 0);
        }

        count++;
    }

    if (count > 0) {
        io_uring_cq_advance(&ring_, count);
    }
#endif
}

void IOUringWriter::WaitAll() {
#if MARBLE_HAS_IO_URING
    if (!ring_initialized_) return;

    // Wait for all pending requests to complete
    while (true) {
        {
            std::lock_guard<std::mutex> lock(requests_mutex_);
            if (pending_requests_.empty()) {
                break;
            }
        }

        struct io_uring_cqe* cqe;
        int ret = io_uring_wait_cqe(&ring_, &cqe);
        if (ret < 0) {
            break;
        }

        ProcessCompletions();
    }
#endif
}

IOUringWriter::Stats IOUringWriter::GetStats() const {
    std::lock_guard<std::mutex> lock(stats_mutex_);
    return stats_;
}

// ============================================================================
// MultiThreadedIOWriter Implementation
// ============================================================================

MultiThreadedIOWriter::MultiThreadedIOWriter(const std::string& filename, size_t num_threads,
                                            size_t queue_depth, bool use_direct)
    : filename_(filename), num_threads_(num_threads), next_thread_(0) {

    // Determine optimal thread count
    if (num_threads_ == 0) {
        num_threads_ = GetOptimalIOThreadCount();
    }

    // Create one writer per thread
    writers_.reserve(num_threads_);
    thread_offsets_ = std::make_unique<std::atomic<off_t>[]>(num_threads_);

    for (size_t i = 0; i < num_threads_; ++i) {
        writers_.push_back(std::make_unique<IOUringWriter>(filename, queue_depth, use_direct));
        thread_offsets_[i].store(0);
    }
}

MultiThreadedIOWriter::~MultiThreadedIOWriter() {
    WaitAll();
}

size_t MultiThreadedIOWriter::SelectThread() {
    // Simple round-robin distribution
    return next_thread_.fetch_add(1) % num_threads_;
}

uint64_t MultiThreadedIOWriter::SubmitWrite(const void* data, size_t size) {
    size_t thread_idx = SelectThread();

    // Get and increment offset for this thread
    off_t offset = thread_offsets_[thread_idx].fetch_add(size);

    return writers_[thread_idx]->SubmitWrite(data, size, offset, nullptr);
}

void MultiThreadedIOWriter::WaitAll() {
    for (auto& writer : writers_) {
        writer->WaitAll();
    }
}

MultiThreadedIOWriter::AggregateStats MultiThreadedIOWriter::GetStats() const {
    AggregateStats agg;
    agg.num_threads = num_threads_;
    agg.total_submits = 0;
    agg.total_completions = 0;
    agg.total_bytes_written = 0;

    agg.thread_stats.reserve(num_threads_);

    for (const auto& writer : writers_) {
        auto stats = writer->GetStats();
        agg.thread_stats.push_back(stats);

        agg.total_submits += stats.total_submits;
        agg.total_completions += stats.total_completions;
        agg.total_bytes_written += stats.total_bytes_written;
    }

    // Calculate aggregate throughput (placeholder - would need time tracking)
    agg.total_throughput_mb_sec = 0;

    return agg;
}

// ============================================================================
// Helper Functions
// ============================================================================

size_t GetOptimalIOThreadCount() {
    // Get number of CPU cores
    size_t num_cores = std::thread::hardware_concurrency();

    // For I/O-bound workloads, typically use 25-50% of cores
    // or 4-8 threads, whichever is smaller
    size_t optimal = std::min(num_cores / 2, size_t(8));

    return std::max(optimal, size_t(1));  // At least 1 thread
}

} // namespace marble
