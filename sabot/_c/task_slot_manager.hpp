//===----------------------------------------------------------------------===//
//                         Sabot
//
// sabot/_c/task_slot_manager.hpp
//
// Task Slot Manager - Dynamic Worker Pool with Work Stealing
//
// Inspired by DuckDB's TaskScheduler but adapted for morsel-driven execution.
// Provides elastic task slots that can process both local and network morsels.
//
//===----------------------------------------------------------------------===//

#pragma once

#include <arrow/api.h>
#include <thread>
#include <vector>
#include <atomic>
#include <functional>
#include <memory>
#include <chrono>
#include <mutex>
#include <queue>

namespace sabot {

//! Simple lock-free MPSC queue for morsels
//! Uses atomic CAS operations for multi-producer enqueue
template<typename T>
class LockFreeMPSCQueue {
private:
    struct Node {
        T data;
        std::atomic<Node*> next;
        Node(T&& val) : data(std::move(val)), next(nullptr) {}
    };

    std::atomic<Node*> head_;  // Consumer reads from head
    std::atomic<Node*> tail_;  // Producers write to tail

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

    // Multi-producer enqueue (lock-free CAS)
    void enqueue(T&& item) {
        Node* node = new Node(std::move(item));
        Node* prev_tail = tail_.exchange(node, std::memory_order_acq_rel);
        prev_tail->next.store(node, std::memory_order_release);
    }

    // Single-consumer dequeue (lock-free)
    bool try_dequeue(T& item) {
        Node* head = head_.load(std::memory_order_relaxed);
        Node* next = head->next.load(std::memory_order_acquire);

        if (next == nullptr) {
            return false;  // Queue empty
        }

        item = std::move(next->data);
        head_.store(next, std::memory_order_release);
        delete head;
        return true;
    }
};

//! Morsel source type
enum class MorselSource {
    LOCAL,   //! Created locally on this agent
    NETWORK  //! Received from network shuffle
};

//! Morsel - Lightweight task unit
struct Morsel {
    std::shared_ptr<arrow::RecordBatch> batch;  //! Data to process
    int64_t start_row;                           //! Start row in batch
    int64_t num_rows;                            //! Number of rows
    int partition_id;                            //! Partition ID
    MorselSource source;                         //! Where morsel came from

    //! Get zero-copy slice
    std::shared_ptr<arrow::RecordBatch> GetSlice() const {
        if (!batch || num_rows == 0) return nullptr;
        return batch->Slice(start_row, num_rows);
    }
};

//! Processed morsel result
struct MorselResult {
    std::shared_ptr<arrow::RecordBatch> batch;  //! Result batch
    int morsel_id;                               //! For reassembly ordering
    bool success;                                //! Processing succeeded
};

//! Morsel processor callback
using MorselProcessorFunc = std::function<std::shared_ptr<arrow::RecordBatch>(
    std::shared_ptr<arrow::RecordBatch>)>;

//! Task slot statistics
struct SlotStats {
    int slot_id;
    bool busy;
    int64_t morsels_processed;
    int64_t total_processing_time_us;
    int64_t local_morsels;
    int64_t network_morsels;
};

//! Forward declarations
class TaskSlot;

//! TaskSlotManager - Dynamic worker pool for morsel execution
//!
//! Architecture:
//! - N worker threads (task slots), each with work-stealing capability
//! - Global lock-free queue for morsel distribution
//! - Dynamic scaling: add/remove slots based on load
//! - Unified handling of local and network morsels
//!
//! Key properties:
//! - Lock-free: All queue operations use atomics
//! - Work stealing: Idle slots steal from global queue
//! - Elastic: Scale up/down without stopping
//! - Fair: Round-robin distribution across slots
class TaskSlotManager {
public:
    //! Create task slot manager
    //! @param num_slots Initial number of worker threads
    explicit TaskSlotManager(int num_slots = 0);

    //! Destructor - shutdown all slots
    ~TaskSlotManager();

    //! Execute morsels using task slots
    //! @param morsels Vector of morsels to process
    //! @param processor Callback function to process each morsel
    //! @return Vector of processed results
    std::vector<MorselResult> ExecuteMorsels(
        std::vector<Morsel> morsels,
        MorselProcessorFunc processor
    );

    //! Execute morsels with C-style callback (for Cython)
    //! @param morsels Vector of morsels to process
    //! @param processor C function pointer
    //! @return Vector of processed results
    std::vector<MorselResult> ExecuteMorselsWithCallback(
        std::vector<Morsel> morsels,
        std::shared_ptr<arrow::RecordBatch> (*processor)(std::shared_ptr<arrow::RecordBatch>)
    );

    //! Enqueue single morsel (for network morsels arriving asynchronously)
    //! @param morsel Morsel to enqueue
    //! @return True if enqueued successfully
    bool EnqueueMorsel(const Morsel& morsel);

    //! Dynamic slot management
    void AddSlots(int count);           //! Add worker threads
    void RemoveSlots(int count);        //! Remove worker threads (graceful)
    int GetNumSlots() const;            //! Get current slot count
    int GetAvailableSlots() const;      //! Count idle slots

    //! Queue depth (for monitoring/autoscaling)
    int64_t GetQueueDepth() const;

    //! Get per-slot statistics
    std::vector<SlotStats> GetSlotStats() const;

    //! Shutdown all slots (called by destructor)
    void Shutdown();

private:
    //! Number of worker threads
    std::atomic<int> num_slots_;

    //! Worker threads (task slots)
    std::vector<std::unique_ptr<TaskSlot>> slots_;

    //! Global work queue (lock-free MPSC)
    std::unique_ptr<LockFreeMPSCQueue<Morsel>> global_queue_;

    //! Result queue (lock-free MPSC)
    std::unique_ptr<LockFreeMPSCQueue<MorselResult>> result_queue_;

    //! Atomic flags
    std::atomic<bool> running_;
    std::atomic<bool> shutdown_requested_;

    //! Current processor function (set per ExecuteMorsels call)
    MorselProcessorFunc current_processor_;

    //! Statistics (atomic)
    mutable std::atomic<int64_t> total_morsels_processed_;
    mutable std::atomic<int64_t> total_morsels_enqueued_;

    //! Mutex for adding/removing slots
    std::mutex slots_mutex_;

    //! Create a new task slot
    std::unique_ptr<TaskSlot> CreateSlot(int slot_id);

    friend class TaskSlot;
};

//! TaskSlot - Individual worker thread with work stealing
class TaskSlot {
public:
    TaskSlot(int slot_id, TaskSlotManager* manager);
    ~TaskSlot();

    //! Start worker thread
    void Start();

    //! Stop worker thread (graceful)
    void Stop();

    //! Get statistics
    SlotStats GetStats() const;

    //! Check if busy
    bool IsBusy() const { return busy_.load(std::memory_order_relaxed); }

private:
    //! Slot ID
    int slot_id_;

    //! Reference to manager (for accessing global queue)
    TaskSlotManager* manager_;

    //! Worker thread
    std::thread worker_thread_;

    //! Atomic flags
    std::atomic<bool> running_;
    std::atomic<bool> busy_;

    //! Statistics (atomic)
    std::atomic<int64_t> morsels_processed_;
    std::atomic<int64_t> local_morsels_processed_;
    std::atomic<int64_t> network_morsels_processed_;
    std::atomic<int64_t> total_processing_time_us_;

    //! Worker loop - continuously pull morsels and process
    void WorkerLoop();
};

} // namespace sabot
