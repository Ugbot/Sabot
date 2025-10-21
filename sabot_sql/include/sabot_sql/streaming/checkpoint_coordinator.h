#pragma once

#include <arrow/api.h>
#include <memory>
#include <string>
#include <vector>
#include <unordered_map>
#include <atomic>
#include <mutex>
#include <thread>
#include <condition_variable>

namespace marble {
    class Client;  // Forward declare MarbleDB client
}

namespace sabot_sql {
namespace streaming {

/**
 * Checkpoint coordinator for exactly-once processing
 * 
 * Coordinates checkpoint barriers across streaming operators and sources.
 * Integrates with Sabot's checkpoint system for distributed exactly-once semantics.
 * 
 * Features:
 * - Barrier injection at Kafka sources
 * - Align barriers across partitions
 * - Snapshot MarbleDB state tables
 * - Atomic commit of offsets + state
 * - Recovery after node failures
 */
class CheckpointCoordinator {
public:
    /**
     * Checkpoint state for a single operator/source
     */
    struct CheckpointState {
        std::string operator_id;
        std::string state_table_name;
        std::unordered_map<std::string, std::string> state_snapshot;
        int64_t checkpoint_id;
        int64_t timestamp;
        bool is_committed;
        
        CheckpointState() = default;
        CheckpointState(const std::string& id, int64_t cp_id)
            : operator_id(id), checkpoint_id(cp_id), timestamp(0), is_committed(false) {}
    };
    
    /**
     * Checkpoint barrier for coordination
     */
    struct CheckpointBarrier {
        int64_t checkpoint_id;
        int64_t timestamp;
        std::vector<std::string> participant_ids;
        std::atomic<int> acknowledgments{0};
        std::atomic<bool> is_complete{false};
        
        CheckpointBarrier(int64_t id, int64_t ts, const std::vector<std::string>& participants)
            : checkpoint_id(id), timestamp(ts), participant_ids(participants) {}
    };
    
    CheckpointCoordinator();
    ~CheckpointCoordinator();
    
    // ========== Lifecycle ==========
    
    /**
     * Initialize checkpoint coordinator
     * 
     * @param coordinator_id Unique identifier for this coordinator
     * @param checkpoint_interval_ms How often to trigger checkpoints
     * @param marble_client MarbleDB client for state persistence
     */
    arrow::Status Initialize(
        const std::string& coordinator_id,
        int64_t checkpoint_interval_ms,
        std::shared_ptr<marble::Client> marble_client
    );
    
    /**
     * Shutdown checkpoint coordinator
     */
    arrow::Status Shutdown();
    
    // ========== Participant Management ==========
    
    /**
     * Register a streaming operator/source for checkpointing
     * 
     * @param operator_id Unique identifier for the operator
     * @param state_table_name MarbleDB table name for state storage
     * @return Status indicating success/failure
     */
    arrow::Status RegisterParticipant(
        const std::string& operator_id,
        const std::string& state_table_name
    );
    
    /**
     * Unregister a streaming operator/source
     */
    arrow::Status UnregisterParticipant(const std::string& operator_id);
    
    /**
     * Get list of registered participants
     */
    std::vector<std::string> GetParticipants() const;
    
    // ========== Checkpoint Execution ==========
    
    /**
     * Trigger a checkpoint
     * 
     * @return Checkpoint ID if successful
     */
    arrow::Result<int64_t> TriggerCheckpoint();
    
    /**
     * Wait for checkpoint to complete
     * 
     * @param checkpoint_id Checkpoint ID to wait for
     * @param timeout_ms Maximum time to wait
     * @return Status indicating success/failure
     */
    arrow::Status WaitForCheckpoint(int64_t checkpoint_id, int64_t timeout_ms = 30000);
    
    /**
     * Check if checkpoint is completed
     * 
     * @param checkpoint_id Checkpoint ID to check
     * @return true if checkpoint is completed
     */
    arrow::Result<bool> IsCheckpointCompleted(int64_t checkpoint_id);
    
    /**
     * Acknowledge checkpoint completion from a participant
     * 
     * @param operator_id Participant operator ID
     * @param checkpoint_id Checkpoint ID being acknowledged
     * @param state_snapshot Snapshot of operator state
     * @return Status indicating success/failure
     */
    arrow::Status AcknowledgeCheckpoint(
        const std::string& operator_id,
        int64_t checkpoint_id,
        const std::unordered_map<std::string, std::string>& state_snapshot
    );
    
    // ========== Recovery ==========
    
    /**
     * Get the last completed checkpoint ID
     */
    int64_t GetLastCheckpointId() const;
    
    /**
     * Get checkpoint state for recovery
     * 
     * @param checkpoint_id Checkpoint ID to recover from
     * @return Map of operator_id -> state_snapshot
     */
    arrow::Result<std::unordered_map<std::string, std::unordered_map<std::string, std::string>>>
    GetCheckpointState(int64_t checkpoint_id);
    
    /**
     * Recover from a specific checkpoint
     * 
     * @param checkpoint_id Checkpoint ID to recover from
     * @return Status indicating success/failure
     */
    arrow::Status RecoverFromCheckpoint(int64_t checkpoint_id);
    
    // ========== Monitoring ==========
    
    /**
     * Get checkpoint coordinator statistics
     */
    struct CheckpointStats {
        int64_t total_checkpoints;
        int64_t completed_checkpoints;
        int64_t failed_checkpoints;
        int64_t last_checkpoint_id;
        int64_t last_checkpoint_time;
        size_t registered_participants;
        int64_t checkpoint_interval_ms;
    };
    
    CheckpointStats GetStats() const;
    
    // ========== Configuration ==========
    
    /**
     * Set checkpoint interval
     */
    void SetCheckpointInterval(int64_t interval_ms);
    
    /**
     * Enable/disable automatic checkpointing
     */
    void SetAutoCheckpointing(bool enabled);
    
private:
    // Configuration
    std::string coordinator_id_;
    int64_t checkpoint_interval_ms_;
    std::shared_ptr<marble::Client> marble_client_;
    std::atomic<bool> auto_checkpointing_{true};
    
    // State
    mutable std::mutex mutex_;
    std::unordered_map<std::string, std::string> participants_;  // operator_id -> state_table_name
    std::unordered_map<int64_t, std::shared_ptr<CheckpointBarrier>> active_checkpoints_;
    std::unordered_map<int64_t, std::unordered_map<std::string, CheckpointState>> checkpoint_states_;
    
    // Statistics
    std::atomic<int64_t> total_checkpoints_{0};
    std::atomic<int64_t> completed_checkpoints_{0};
    std::atomic<int64_t> failed_checkpoints_{0};
    std::atomic<int64_t> last_checkpoint_id_{0};
    std::atomic<int64_t> last_checkpoint_time_{0};
    
    // Background thread for automatic checkpointing
    std::thread checkpoint_thread_;
    std::atomic<bool> shutdown_requested_{false};
    std::condition_variable checkpoint_cv_;
    
    // MarbleDB table for persistence
    std::string checkpoint_table_name_;
    
    // Helper methods
    arrow::Status InitializeMarbleDBTable();
    arrow::Status SaveCheckpointState(int64_t checkpoint_id);
    arrow::Status LoadCheckpointState(int64_t checkpoint_id);
    void CheckpointThreadMain();
    arrow::Status InjectBarrier(int64_t checkpoint_id);
    arrow::Status CommitCheckpoint(int64_t checkpoint_id);
    void CleanupOldCheckpoints();
};

/**
 * Checkpoint participant interface
 * 
 * Streaming operators implement this interface to participate in checkpointing.
 */
class CheckpointParticipant {
public:
    virtual ~CheckpointParticipant() = default;
    
    /**
     * Get operator ID for checkpointing
     */
    virtual std::string GetOperatorId() const = 0;
    
    /**
     * Snapshot current state for checkpoint
     * 
     * @param checkpoint_id Checkpoint ID
     * @return State snapshot as key-value pairs
     */
    virtual arrow::Result<std::unordered_map<std::string, std::string>>
    SnapshotState(int64_t checkpoint_id) = 0;
    
    /**
     * Restore state from checkpoint
     * 
     * @param checkpoint_id Checkpoint ID
     * @param state_snapshot State snapshot to restore
     * @return Status indicating success/failure
     */
    virtual arrow::Status RestoreState(
        int64_t checkpoint_id,
        const std::unordered_map<std::string, std::string>& state_snapshot
    ) = 0;
    
    /**
     * Handle checkpoint barrier
     * 
     * @param checkpoint_id Checkpoint ID
     * @return Status indicating success/failure
     */
    virtual arrow::Status HandleCheckpointBarrier(int64_t checkpoint_id) = 0;
};

} // namespace streaming
} // namespace sabot_sql
