#include "marble/direct_io_writer.h"
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <errno.h>
#include <cstring>
#include <stdexcept>
#include <chrono>

namespace marble {

// ============================================================================
// DirectIOWriter Implementation
// ============================================================================

DirectIOWriter::DirectIOWriter(const std::string& filename, bool use_direct, bool use_datasync)
    : fd_(-1), filename_(filename), use_direct_(use_direct),
      use_datasync_(use_datasync), offset_(0) {

    memset(&stats_, 0, sizeof(stats_));

    // Open file with appropriate flags
    int flags = O_WRONLY | O_CREAT | O_TRUNC;

    if (use_direct) {
#ifdef O_DIRECT
        flags |= O_DIRECT;
#else
        // O_DIRECT not available on this platform (e.g., macOS)
        use_direct_ = false;
#endif
    }

    fd_ = open(filename.c_str(), flags, 0644);
    if (fd_ < 0) {
        throw std::runtime_error("Failed to open file: " + filename + ", error: " + std::strerror(errno));
    }
}

DirectIOWriter::~DirectIOWriter() {
    if (fd_ >= 0) {
        close(fd_);
    }
}

ssize_t DirectIOWriter::Write(const void* data, size_t size) {
    if (fd_ < 0) {
        errno = EBADF;
        return -1;
    }

    auto start = std::chrono::high_resolution_clock::now();

    ssize_t written = pwrite(fd_, data, size, offset_);

    auto end = std::chrono::high_resolution_clock::now();
    double elapsed_ms = std::chrono::duration<double, std::milli>(end - start).count();

    if (written > 0) {
        offset_ += written;
        stats_.total_writes++;
        stats_.total_bytes_written += written;
        stats_.total_write_time_ms += elapsed_ms;
    }

    return written;
}

int DirectIOWriter::Sync() {
    if (fd_ < 0) {
        errno = EBADF;
        return -1;
    }

    auto start = std::chrono::high_resolution_clock::now();

    int result;
    if (use_datasync_) {
#ifdef __APPLE__
        // macOS doesn't have fdatasync, use fsync
        result = fsync(fd_);
#else
        result = fdatasync(fd_);
#endif
    } else {
        result = fsync(fd_);
    }

    auto end = std::chrono::high_resolution_clock::now();
    double elapsed_ms = std::chrono::duration<double, std::milli>(end - start).count();

    stats_.total_syncs++;
    stats_.total_sync_time_ms += elapsed_ms;

    return result;
}

// ============================================================================
// GroupCommitWriter Implementation
// ============================================================================

GroupCommitWriter::GroupCommitWriter(const std::string& filename, int window_ms,
                                    bool use_direct, bool use_datasync)
    : writer_(std::make_unique<DirectIOWriter>(filename, use_direct, use_datasync)),
      window_ms_(window_ms),
      max_batch_size_(1000),  // Batch up to 1000 writes
      stop_(false) {

    memset(&stats_, 0, sizeof(stats_));

    // Start background flush thread
    flush_thread_ = std::thread(&GroupCommitWriter::FlushThread, this);
}

GroupCommitWriter::~GroupCommitWriter() {
    stop_.store(true);
    cv_.notify_all();

    if (flush_thread_.joinable()) {
        flush_thread_.join();
    }
}

uint64_t GroupCommitWriter::SubmitWrite(const void* data, size_t size) {
    std::unique_lock<std::mutex> lock(mutex_);

    WriteRequest req;
    req.data = data;
    req.size = size;
    req.completed = false;
    req.result = 0;

    pending_writes_.push_back(req);

    {
        std::lock_guard<std::mutex> stats_lock(stats_mutex_);
        stats_.total_submits++;
    }

    // Wake flush thread if batch is full
    if (pending_writes_.size() >= max_batch_size_) {
        cv_.notify_one();
    }

    return pending_writes_.size() - 1;
}

void GroupCommitWriter::Flush() {
    std::unique_lock<std::mutex> lock(mutex_);

    if (pending_writes_.empty()) {
        return;
    }

    auto start = std::chrono::high_resolution_clock::now();

    // Write all pending requests
    for (auto& req : pending_writes_) {
        req.result = writer_->Write(req.data, req.size);
        req.completed = true;
    }

    // Single sync for all writes (group commit!)
    writer_->Sync();

    auto end = std::chrono::high_resolution_clock::now();
    double elapsed_us = std::chrono::duration<double, std::micro>(end - start).count();

    // Update statistics
    {
        std::lock_guard<std::mutex> stats_lock(stats_mutex_);
        stats_.total_flushes++;
        stats_.total_syncs++;

        size_t batch_size = pending_writes_.size();
        size_t bytes_written = 0;
        for (const auto& req : pending_writes_) {
            if (req.result > 0) {
                bytes_written += req.result;
            }
        }
        stats_.total_bytes_written += bytes_written;

        // Running average of batch size
        stats_.avg_batch_size = (stats_.avg_batch_size * (stats_.total_flushes - 1) + batch_size) / stats_.total_flushes;

        // Running average of commit latency
        stats_.avg_commit_latency_us = (stats_.avg_commit_latency_us * (stats_.total_flushes - 1) + elapsed_us) / stats_.total_flushes;
    }

    pending_writes_.clear();
}

void GroupCommitWriter::FlushThread() {
    while (!stop_.load()) {
        std::unique_lock<std::mutex> lock(mutex_);

        // Wait for group commit window or batch full
        auto wait_result = cv_.wait_for(lock, std::chrono::milliseconds(window_ms_));

        if (!pending_writes_.empty()) {
            lock.unlock();
            Flush();
        }
    }

    // Final flush on shutdown
    Flush();
}

GroupCommitWriter::Stats GroupCommitWriter::GetStats() const {
    std::lock_guard<std::mutex> lock(stats_mutex_);
    return stats_;
}

} // namespace marble
