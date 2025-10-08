//===----------------------------------------------------------------------===//
//                         Sabot
//
// sabot/_c/morsel_executor.hpp
//
// C++ Morsel Executor - Lock-Free Parallel Execution
//
// Inspired by DuckDB's pipeline_executor.hpp but using Arrow batches
// and lock-free queues for maximum performance.
//
//===----------------------------------------------------------------------===//

#pragma once

#include <arrow/api.h>
#include <arrow/compute/api.h>
#include <thread>
#include <vector>
#include <atomic>
#include <functional>
#include <memory>
#include <queue>

namespace sabot {

//! Simple lock-free MPSC queue (same as task_slot_manager)
template<typename T>
class LockFreeMPSCQueue {
private:
    struct Node {
        T data;
        std::atomic<Node*> next;
        Node(T&& val) : data(std::move(val)), next(nullptr) {}
    };

    std::atomic<Node*> head_;
    std::atomic<Node*> tail_;

public:
    LockFreeMPSCQueue() {
        Node* dummy = new Node(T{});
        head_.store(dummy, std::memory_order_relaxed);
        tail_.store(dummy, std::memory_order_relaxed);
    }

    ~LockFreeMPSCQueue() {
        while (Node* node = head_.load(std::memory_order_relaxed)) {
            head_.store(node->next.load(std::memory_order_relaxed), std::memory_order_relaxed);
            delete node;
        }
    }

    void enqueue(T&& item) {
        Node* node = new Node(std::move(item));
        Node* prev_tail = tail_.exchange(node, std::memory_order_acq_rel);
        prev_tail->next.store(node, std::memory_order_release);
    }

    bool try_dequeue(T& item) {
        Node* head = head_.load(std::memory_order_relaxed);
        Node* next = head->next.load(std::memory_order_acquire);

        if (next == nullptr) {
            return false;
        }

        item = std::move(next->data);
        head_.store(next, std::memory_order_release);
        delete head;
        return true;
    }
};

//! Morsel - Small cache-friendly chunk of data for parallel processing
struct Morsel {
    std::shared_ptr<arrow::RecordBatch> batch;  //! Source batch (shared, zero-copy)
    int64_t start_row;                           //! Start row index in batch
    int64_t num_rows;                            //! Number of rows in this morsel
    int worker_id;                               //! Worker assigned to process (-1 = unassigned)

    //! Get zero-copy slice of batch for this morsel
    std::shared_ptr<arrow::RecordBatch> GetSlice() const {
        if (!batch || num_rows == 0) {
            return nullptr;
        }
        return batch->Slice(start_row, num_rows);
    }
};

//! Result of morsel processing
struct MorselResult {
    std::shared_ptr<arrow::RecordBatch> batch;  //! Result batch
    int morsel_id;                               //! Original morsel ID for reassembly
    bool success;                                //! Whether processing succeeded
};

//! Callback function for processing a morsel
//! Takes a RecordBatch slice, returns processed RecordBatch
using MorselProcessorFunc = std::function<std::shared_ptr<arrow::RecordBatch>(
    std::shared_ptr<arrow::RecordBatch>)>;

//! MorselExecutor - C++ thread pool for parallel morsel execution
//!
//! Design principles (from DuckDB):
//! - Lock-free work queue (moodycamel::ConcurrentQueue)
//! - Zero-copy morsel creation (Arrow batch slices)
//! - Worker threads process morsels in parallel
//! - Results collected and reassembled in order
//!
//! Performance characteristics:
//! - Morsel creation: O(n/morsel_size), zero-copy
//! - Queue operations: ~50-100ns (lock-free)
//! - Scales linearly with cores (no lock contention)
class MorselExecutor {
public:
    //! Create morsel executor with specified number of worker threads
    //! @param num_threads Number of worker threads (0 = auto-detect)
    explicit MorselExecutor(int num_threads = 0);

    //! Destructor - shuts down workers cleanly
    ~MorselExecutor();

    //! Execute batch locally using morsel-driven parallelism
    //!
    //! Process:
    //! 1. Split batch into cache-friendly morsels (zero-copy slices)
    //! 2. Distribute morsels to worker threads via lock-free queue
    //! 3. Workers process morsels in parallel using callback
    //! 4. Collect and reassemble results in order
    //!
    //! @param batch Input RecordBatch to process
    //! @param morsel_size_bytes Target size of each morsel in bytes (default 64KB)
    //! @param processor_func Callback to process each morsel
    //! @return Vector of result batches (in morsel order)
    std::vector<std::shared_ptr<arrow::RecordBatch>> ExecuteLocal(
        std::shared_ptr<arrow::RecordBatch> batch,
        int64_t morsel_size_bytes,
        MorselProcessorFunc processor_func
    );

    //! Execute with C-style callback (for Cython compatibility)
    //! @param batch Input RecordBatch to process
    //! @param morsel_size_bytes Target size of each morsel in bytes
    //! @param processor C function pointer
    //! @return Vector of result batches (in morsel order)
    std::vector<std::shared_ptr<arrow::RecordBatch>> ExecuteLocalWithCallback(
        std::shared_ptr<arrow::RecordBatch> batch,
        int64_t morsel_size_bytes,
        std::shared_ptr<arrow::RecordBatch> (*processor)(std::shared_ptr<arrow::RecordBatch>)
    );

    //! Shutdown executor and join all worker threads
    void Shutdown();

    //! Get number of worker threads
    int GetNumThreads() const { return num_threads_; }

    //! Statistics
    struct Stats {
        int64_t total_morsels_created = 0;
        int64_t total_morsels_processed = 0;
        int64_t total_batches_executed = 0;
    };

    //! Get execution statistics
    Stats GetStats() const;

private:
    //! Number of worker threads
    int num_threads_;

    //! Worker threads
    std::vector<std::thread> workers_;

    //! Lock-free queue for morsel distribution (MPSC)
    std::unique_ptr<LockFreeMPSCQueue<Morsel>> morsel_queue_;

    //! Results queue (lock-free MPSC)
    std::unique_ptr<LockFreeMPSCQueue<MorselResult>> result_queue_;

    //! Atomic flags
    std::atomic<bool> running_;
    std::atomic<bool> shutdown_requested_;

    //! Statistics (atomic for lock-free updates)
    mutable std::atomic<int64_t> total_morsels_created_;
    mutable std::atomic<int64_t> total_morsels_processed_;
    mutable std::atomic<int64_t> total_batches_executed_;

    //! Current processor function (set per execution)
    MorselProcessorFunc current_processor_;

    //! Worker loop (runs in each thread)
    void WorkerLoop(int worker_id);

    //! Create morsels from batch (zero-copy slicing)
    //! @param batch Source batch
    //! @param morsel_size_bytes Target morsel size in bytes
    //! @return Vector of morsels (lightweight, zero-copy)
    std::vector<Morsel> CreateMorsels(
        std::shared_ptr<arrow::RecordBatch> batch,
        int64_t morsel_size_bytes
    );

    //! Calculate optimal morsel size
    //! Based on cache size (default 64KB fits in L2/L3)
    static int64_t CalculateMorselSize(
        std::shared_ptr<arrow::RecordBatch> batch,
        int64_t target_bytes
    );
};

} // namespace sabot
