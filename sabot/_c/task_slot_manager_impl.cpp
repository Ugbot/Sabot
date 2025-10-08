//===----------------------------------------------------------------------===//
//                         Sabot
//
// sabot/_c/task_slot_manager.cpp
//
// Task Slot Manager Implementation
//
//===----------------------------------------------------------------------===//

#include "task_slot_manager.hpp"
#include <algorithm>
#include <chrono>

namespace sabot {

//============================================================================
// TaskSlotManager Implementation
//============================================================================

TaskSlotManager::TaskSlotManager(int num_slots)
    : num_slots_(num_slots),
      running_(false),
      shutdown_requested_(false),
      total_morsels_processed_(0),
      total_morsels_enqueued_(0) {

    // Auto-detect slot count if not specified
    if (num_slots_ <= 0) {
        num_slots_ = std::thread::hardware_concurrency();
        if (num_slots_ <= 0) {
            num_slots_ = 4;  // Fallback
        }
    }

    // Create lock-free queues
    global_queue_ = std::make_unique<LockFreeMPSCQueue<Morsel>>();
    result_queue_ = std::make_unique<LockFreeMPSCQueue<MorselResult>>();

    // Create and start worker slots
    running_ = true;
    for (int i = 0; i < num_slots_; ++i) {
        auto slot = CreateSlot(i);
        slot->Start();
        slots_.push_back(std::move(slot));
    }
}

TaskSlotManager::~TaskSlotManager() {
    Shutdown();
}

void TaskSlotManager::Shutdown() {
    if (!running_) {
        return;
    }

    // Signal shutdown
    shutdown_requested_ = true;
    running_ = false;

    // Stop all slots
    for (auto& slot : slots_) {
        slot->Stop();
    }

    slots_.clear();
}

std::vector<MorselResult> TaskSlotManager::ExecuteMorsels(
    std::vector<Morsel> morsels,
    MorselProcessorFunc processor
) {
    if (morsels.empty()) {
        return {};
    }

    // Set processor function
    current_processor_ = processor;

    // Enqueue all morsels
    for (auto& morsel : morsels) {
        global_queue_->enqueue(std::move(Morsel(morsel)));  // Copy then move
        total_morsels_enqueued_.fetch_add(1, std::memory_order_relaxed);
    }

    // Collect results
    std::vector<MorselResult> results;
    results.reserve(morsels.size());

    size_t collected = 0;
    while (collected < morsels.size()) {
        MorselResult result;
        if (result_queue_->try_dequeue(result)) {
            if (result.success) {
                results.push_back(result);
            }
            collected++;
        } else {
            // No results yet - yield CPU
            std::this_thread::yield();
        }
    }

    // Sort results by morsel_id to maintain order
    std::sort(results.begin(), results.end(),
              [](const MorselResult& a, const MorselResult& b) {
                  return a.morsel_id < b.morsel_id;
              });

    return results;
}

std::vector<MorselResult> TaskSlotManager::ExecuteMorselsWithCallback(
    std::vector<Morsel> morsels,
    std::shared_ptr<arrow::RecordBatch> (*processor)(std::shared_ptr<arrow::RecordBatch>)
) {
    // Wrap C function pointer in std::function
    MorselProcessorFunc wrapped = [processor](std::shared_ptr<arrow::RecordBatch> batch) {
        return processor(batch);
    };

    return ExecuteMorsels(morsels, wrapped);
}

bool TaskSlotManager::EnqueueMorsel(const Morsel& morsel) {
    if (!running_) {
        return false;
    }

    global_queue_->enqueue(std::move(Morsel(morsel)));  // Copy then move
    total_morsels_enqueued_.fetch_add(1, std::memory_order_relaxed);
    return true;
}

void TaskSlotManager::AddSlots(int count) {
    if (count <= 0 || shutdown_requested_) {
        return;
    }

    std::lock_guard<std::mutex> lock(slots_mutex_);

    int current_count = num_slots_.load();
    for (int i = 0; i < count; ++i) {
        auto slot = CreateSlot(current_count + i);
        slot->Start();
        slots_.push_back(std::move(slot));
    }

    num_slots_.fetch_add(count, std::memory_order_release);
}

void TaskSlotManager::RemoveSlots(int count) {
    if (count <= 0 || shutdown_requested_) {
        return;
    }

    std::lock_guard<std::mutex> lock(slots_mutex_);

    int current_count = num_slots_.load();
    int to_remove = std::min(count, current_count - 1);  // Keep at least 1 slot

    // Remove slots from the end
    for (int i = 0; i < to_remove; ++i) {
        if (!slots_.empty()) {
            auto& slot = slots_.back();
            slot->Stop();
            slots_.pop_back();
        }
    }

    num_slots_.fetch_sub(to_remove, std::memory_order_release);
}

int TaskSlotManager::GetNumSlots() const {
    return num_slots_.load(std::memory_order_acquire);
}

int TaskSlotManager::GetAvailableSlots() const {
    int available = 0;
    for (const auto& slot : slots_) {
        if (!slot->IsBusy()) {
            available++;
        }
    }
    return available;
}

int64_t TaskSlotManager::GetQueueDepth() const {
    return total_morsels_enqueued_.load(std::memory_order_relaxed) -
           total_morsels_processed_.load(std::memory_order_relaxed);
}

std::vector<SlotStats> TaskSlotManager::GetSlotStats() const {
    std::vector<SlotStats> stats;
    stats.reserve(slots_.size());

    for (const auto& slot : slots_) {
        stats.push_back(slot->GetStats());
    }

    return stats;
}

std::unique_ptr<TaskSlot> TaskSlotManager::CreateSlot(int slot_id) {
    return std::make_unique<TaskSlot>(slot_id, this);
}

//============================================================================
// TaskSlot Implementation
//============================================================================

TaskSlot::TaskSlot(int slot_id, TaskSlotManager* manager)
    : slot_id_(slot_id),
      manager_(manager),
      running_(false),
      busy_(false),
      morsels_processed_(0),
      local_morsels_processed_(0),
      network_morsels_processed_(0),
      total_processing_time_us_(0) {
}

TaskSlot::~TaskSlot() {
    Stop();
}

void TaskSlot::Start() {
    if (running_) {
        return;
    }

    running_ = true;
    worker_thread_ = std::thread(&TaskSlot::WorkerLoop, this);
}

void TaskSlot::Stop() {
    if (!running_) {
        return;
    }

    running_ = false;

    if (worker_thread_.joinable()) {
        worker_thread_.join();
    }
}

void TaskSlot::WorkerLoop() {
    Morsel morsel;

    while (running_ && !manager_->shutdown_requested_) {
        // Try to pull morsel from global queue (lock-free)
        if (manager_->global_queue_->try_dequeue(morsel)) {
            // Mark as busy
            busy_.store(true, std::memory_order_release);

            // Get morsel slice (zero-copy)
            auto slice = morsel.GetSlice();
            if (!slice) {
                busy_.store(false, std::memory_order_release);
                continue;
            }

            // Process morsel
            auto start_time = std::chrono::high_resolution_clock::now();

            MorselResult result;
            result.morsel_id = morsels_processed_.fetch_add(1, std::memory_order_relaxed);

            try {
                if (manager_->current_processor_) {
                    result.batch = manager_->current_processor_(slice);
                    result.success = (result.batch != nullptr);
                } else {
                    result.success = false;
                }
            } catch (...) {
                result.success = false;
            }

            auto end_time = std::chrono::high_resolution_clock::now();
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                end_time - start_time
            ).count();

            total_processing_time_us_.fetch_add(duration, std::memory_order_relaxed);

            // Update source-specific counters
            if (morsel.source == MorselSource::LOCAL) {
                local_morsels_processed_.fetch_add(1, std::memory_order_relaxed);
            } else {
                network_morsels_processed_.fetch_add(1, std::memory_order_relaxed);
            }

            // Enqueue result (lock-free)
            manager_->result_queue_->enqueue(std::move(result));
            manager_->total_morsels_processed_.fetch_add(1, std::memory_order_relaxed);

            // Mark as idle
            busy_.store(false, std::memory_order_release);

        } else {
            // No work available - yield CPU
            std::this_thread::yield();
        }
    }
}

SlotStats TaskSlot::GetStats() const {
    SlotStats stats;
    stats.slot_id = slot_id_;
    stats.busy = busy_.load(std::memory_order_acquire);
    stats.morsels_processed = morsels_processed_.load(std::memory_order_relaxed);
    stats.total_processing_time_us = total_processing_time_us_.load(std::memory_order_relaxed);
    stats.local_morsels = local_morsels_processed_.load(std::memory_order_relaxed);
    stats.network_morsels = network_morsels_processed_.load(std::memory_order_relaxed);
    return stats;
}

} // namespace sabot
