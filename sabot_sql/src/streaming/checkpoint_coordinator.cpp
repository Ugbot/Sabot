#include "sabot_sql/streaming/checkpoint_coordinator.h"
#include <chrono>
#include <sstream>
#include <algorithm>

namespace sabot_sql {
namespace streaming {

CheckpointCoordinator::CheckpointCoordinator() = default;
CheckpointCoordinator::~CheckpointCoordinator() {
    auto status = Shutdown();
    // Ignore shutdown errors in destructor
}

// ========== Lifecycle ==========

arrow::Status CheckpointCoordinator::Initialize(
    const std::string& coordinator_id,
    int64_t checkpoint_interval_ms,
    std::shared_ptr<marble::Client> marble_client
) {
    coordinator_id_ = coordinator_id;
    checkpoint_interval_ms_ = checkpoint_interval_ms;
    marble_client_ = marble_client;
    
    // Initialize MarbleDB table for persistence
    checkpoint_table_name_ = "checkpoint_state_" + coordinator_id_;
    ARROW_RETURN_NOT_OK(InitializeMarbleDBTable());
    
    // Start background checkpoint thread
    shutdown_requested_ = false;
    checkpoint_thread_ = std::thread(&CheckpointCoordinator::CheckpointThreadMain, this);
    
    return arrow::Status::OK();
}

arrow::Status CheckpointCoordinator::Shutdown() {
    // Stop background thread
    shutdown_requested_ = true;
    checkpoint_cv_.notify_all();
    
    if (checkpoint_thread_.joinable()) {
        checkpoint_thread_.join();
    }
    
    // Save final state
    if (last_checkpoint_id_.load() > 0) {
        ARROW_RETURN_NOT_OK(SaveCheckpointState(last_checkpoint_id_.load()));
    }
    
    return arrow::Status::OK();
}

arrow::Status CheckpointCoordinator::InitializeMarbleDBTable() {
    // TODO: Initialize MarbleDB table for checkpoint state
    // For now, we'll use in-memory storage
    // In production, this would create a MarbleDB table with schema:
    // [checkpoint_id: int64, operator_id: utf8, state_data: utf8, timestamp: int64]
    
    return arrow::Status::OK();
}

// ========== Participant Management ==========

arrow::Status CheckpointCoordinator::RegisterParticipant(
    const std::string& operator_id,
    const std::string& state_table_name
) {
    std::lock_guard<std::mutex> lock(mutex_);
    participants_[operator_id] = state_table_name;
    return arrow::Status::OK();
}

arrow::Status CheckpointCoordinator::UnregisterParticipant(const std::string& operator_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    participants_.erase(operator_id);
    return arrow::Status::OK();
}

std::vector<std::string> CheckpointCoordinator::GetParticipants() const {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<std::string> participant_ids;
    for (const auto& [id, table_name] : participants_) {
        participant_ids.push_back(id);
    }
    return participant_ids;
}

// ========== Checkpoint Execution ==========

arrow::Result<int64_t> CheckpointCoordinator::TriggerCheckpoint() {
    int64_t checkpoint_id = ++total_checkpoints_;
    
    // Create checkpoint barrier
    std::vector<std::string> participant_ids;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        for (const auto& [id, table_name] : participants_) {
            participant_ids.push_back(id);
        }
    }
    
    auto barrier = std::make_shared<CheckpointBarrier>(
        checkpoint_id,
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()
        ).count(),
        participant_ids
    );
    
    // Store barrier
    {
        std::lock_guard<std::mutex> lock(mutex_);
        active_checkpoints_[checkpoint_id] = barrier;
    }
    
    // Inject barrier to all participants
    ARROW_RETURN_NOT_OK(InjectBarrier(checkpoint_id));
    
    last_checkpoint_time_ = barrier->timestamp;
    return checkpoint_id;
}

arrow::Status CheckpointCoordinator::WaitForCheckpoint(int64_t checkpoint_id, int64_t timeout_ms) {
    std::shared_ptr<CheckpointBarrier> barrier;
    
    // Get barrier
    {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = active_checkpoints_.find(checkpoint_id);
        if (it == active_checkpoints_.end()) {
            return arrow::Status::Invalid("Checkpoint not found: " + std::to_string(checkpoint_id));
        }
        barrier = it->second;
    }
    
    // Wait for completion
    auto start_time = std::chrono::steady_clock::now();
    while (!barrier->is_complete.load()) {
        auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - start_time
        ).count();
        
        if (elapsed >= timeout_ms) {
            return arrow::Status::Invalid("Checkpoint timeout: " + std::to_string(checkpoint_id));
        }
        
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    
    return arrow::Status::OK();
}

arrow::Result<bool> CheckpointCoordinator::IsCheckpointCompleted(int64_t checkpoint_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    auto it = active_checkpoints_.find(checkpoint_id);
    if (it == active_checkpoints_.end()) {
        return false;
    }
    
    return it->second->is_complete.load();
}

arrow::Status CheckpointCoordinator::AcknowledgeCheckpoint(
    const std::string& operator_id,
    int64_t checkpoint_id,
    const std::unordered_map<std::string, std::string>& state_snapshot
) {
    std::shared_ptr<CheckpointBarrier> barrier;
    
    // Get barrier
    {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = active_checkpoints_.find(checkpoint_id);
        if (it == active_checkpoints_.end()) {
            return arrow::Status::Invalid("Checkpoint not found: " + std::to_string(checkpoint_id));
        }
        barrier = it->second;
    }
    
    // Store checkpoint state
    {
        std::lock_guard<std::mutex> lock(mutex_);
        checkpoint_states_[checkpoint_id][operator_id] = CheckpointState(operator_id, checkpoint_id);
        checkpoint_states_[checkpoint_id][operator_id].state_snapshot = state_snapshot;
        checkpoint_states_[checkpoint_id][operator_id].timestamp = barrier->timestamp;
    }
    
    // Increment acknowledgment count
    int acks = ++barrier->acknowledgments;
    
    // Check if all participants have acknowledged
    if (acks >= static_cast<int>(barrier->participant_ids.size())) {
        // Commit checkpoint
        ARROW_RETURN_NOT_OK(CommitCheckpoint(checkpoint_id));
        
        // Mark as complete
        barrier->is_complete = true;
        
        // Update statistics
        completed_checkpoints_++;
        last_checkpoint_id_ = checkpoint_id;
        
        // Clean up old checkpoints
        CleanupOldCheckpoints();
    }
    
    return arrow::Status::OK();
}

// ========== Recovery ==========

int64_t CheckpointCoordinator::GetLastCheckpointId() const {
    return last_checkpoint_id_.load();
}

arrow::Result<std::unordered_map<std::string, std::unordered_map<std::string, std::string>>>
CheckpointCoordinator::GetCheckpointState(int64_t checkpoint_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    auto it = checkpoint_states_.find(checkpoint_id);
    if (it == checkpoint_states_.end()) {
        return arrow::Status::Invalid("Checkpoint state not found: " + std::to_string(checkpoint_id));
    }
    
    std::unordered_map<std::string, std::unordered_map<std::string, std::string>> result;
    for (const auto& [operator_id, state] : it->second) {
        result[operator_id] = state.state_snapshot;
    }
    
    return result;
}

arrow::Status CheckpointCoordinator::RecoverFromCheckpoint(int64_t checkpoint_id) {
    // Load checkpoint state
    ARROW_ASSIGN_OR_RAISE(
        auto checkpoint_state,
        GetCheckpointState(checkpoint_id)
    );
    
    // TODO: Notify all participants to restore state
    // This would involve calling RestoreState on each CheckpointParticipant
    
    return arrow::Status::OK();
}

// ========== Monitoring ==========

CheckpointCoordinator::CheckpointStats CheckpointCoordinator::GetStats() const {
    std::lock_guard<std::mutex> lock(mutex_);
    
    CheckpointStats stats;
    stats.total_checkpoints = total_checkpoints_.load();
    stats.completed_checkpoints = completed_checkpoints_.load();
    stats.failed_checkpoints = failed_checkpoints_.load();
    stats.last_checkpoint_id = last_checkpoint_id_.load();
    stats.last_checkpoint_time = last_checkpoint_time_.load();
    stats.registered_participants = participants_.size();
    stats.checkpoint_interval_ms = checkpoint_interval_ms_;
    
    return stats;
}

// ========== Configuration ==========

void CheckpointCoordinator::SetCheckpointInterval(int64_t interval_ms) {
    checkpoint_interval_ms_ = interval_ms;
}

void CheckpointCoordinator::SetAutoCheckpointing(bool enabled) {
    auto_checkpointing_ = enabled;
    if (enabled) {
        checkpoint_cv_.notify_all();
    }
}

// ========== Helper Methods ==========

arrow::Status CheckpointCoordinator::SaveCheckpointState(int64_t checkpoint_id) {
    // TODO: Save checkpoint state to MarbleDB
    // For now, we'll use in-memory storage
    // In production, this would serialize checkpoint_states_ to MarbleDB
    
    return arrow::Status::OK();
}

arrow::Status CheckpointCoordinator::LoadCheckpointState(int64_t checkpoint_id) {
    // TODO: Load checkpoint state from MarbleDB
    // For now, we'll start with empty state
    // In production, this would deserialize from MarbleDB
    
    return arrow::Status::OK();
}

void CheckpointCoordinator::CheckpointThreadMain() {
    while (!shutdown_requested_.load()) {
        if (auto_checkpointing_.load()) {
            // Trigger checkpoint
            auto result = TriggerCheckpoint();
            if (!result.ok()) {
                failed_checkpoints_++;
                // Log error but continue
            }
        }
        
        // Wait for next checkpoint interval
        std::unique_lock<std::mutex> lock(mutex_);
        checkpoint_cv_.wait_for(
            lock,
            std::chrono::milliseconds(checkpoint_interval_ms_),
            [this] { return shutdown_requested_.load(); }
        );
    }
}

arrow::Status CheckpointCoordinator::InjectBarrier(int64_t checkpoint_id) {
    // TODO: Inject barrier into streaming pipeline
    // This would involve:
    // 1. Sending barrier to Kafka sources
    // 2. Propagating barrier through operator pipeline
    // 3. Ensuring all operators handle the barrier
    
    return arrow::Status::OK();
}

arrow::Status CheckpointCoordinator::CommitCheckpoint(int64_t checkpoint_id) {
    // TODO: Atomically commit checkpoint state
    // This would involve:
    // 1. Snapshotting all MarbleDB state tables
    // 2. Committing Kafka offsets to MarbleDB RAFT
    // 3. Marking checkpoint as committed
    
    return arrow::Status::OK();
}

void CheckpointCoordinator::CleanupOldCheckpoints() {
    // Keep only the last 10 checkpoints
    const int max_checkpoints = 10;
    
    std::lock_guard<std::mutex> lock(mutex_);
    
    if (checkpoint_states_.size() > max_checkpoints) {
        // Remove oldest checkpoints
        std::vector<int64_t> checkpoint_ids;
        for (const auto& [id, state] : checkpoint_states_) {
            checkpoint_ids.push_back(id);
        }
        
        std::sort(checkpoint_ids.begin(), checkpoint_ids.end());
        
        int to_remove = checkpoint_ids.size() - max_checkpoints;
        for (int i = 0; i < to_remove; ++i) {
            checkpoint_states_.erase(checkpoint_ids[i]);
            active_checkpoints_.erase(checkpoint_ids[i]);
        }
    }
}

} // namespace streaming
} // namespace sabot_sql
