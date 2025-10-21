#pragma once

#include <arrow/api.h>
#include <string>
#include <vector>
#include <memory>
#include <unordered_map>
#include <functional>
#include <atomic>
#include <mutex>
#include <condition_variable>
#include <thread>
#include <chrono>

namespace sabot_sql {
namespace streaming {

// Forward declarations
class CheckpointCoordinator;
class MarbleDBIntegration;

/**
 * CheckpointBarrierInjector - Integrates streaming SQL with Sabot's checkpoint system
 * 
 * This class handles the injection of checkpoint barriers into streaming data pipelines:
 * 1. Receives checkpoint triggers from CheckpointCoordinator
 * 2. Injects barriers into Kafka partition streams
 * 3. Coordinates barrier alignment across partitions
 * 4. Manages checkpoint state persistence
 * 5. Handles recovery from checkpoints
 */
class CheckpointBarrierInjector {
public:
    struct BarrierConfig {
        int64_t checkpoint_id;
        std::string execution_id;
        std::vector<std::string> partition_sources;
        int64_t timeout_ms = 60000;
        bool enable_state_snapshots = true;
        bool enable_offset_commits = true;
    };

    struct BarrierStatus {
        int64_t checkpoint_id;
        std::string execution_id;
        bool is_completed;
        bool has_failed;
        std::string error_message;
        int64_t start_timestamp;
        int64_t completion_timestamp;
        std::unordered_map<std::string, bool> partition_status;
        std::unordered_map<std::string, int64_t> partition_offsets;
    };

    struct PartitionBarrier {
        std::string partition_id;
        int64_t checkpoint_id;
        int64_t barrier_timestamp;
        int64_t offset_before_barrier;
        int64_t offset_after_barrier;
        bool is_aligned;
        std::string error_message;
    };

public:
    CheckpointBarrierInjector();
    ~CheckpointBarrierInjector();

    // Lifecycle
    arrow::Status Initialize(
        std::shared_ptr<CheckpointCoordinator> checkpoint_coordinator,
        std::shared_ptr<MarbleDBIntegration> marbledb
    );
    arrow::Status Shutdown();

    // Barrier injection
    arrow::Result<int64_t> InjectBarrier(const BarrierConfig& config);
    arrow::Status WaitForBarrierAlignment(
        int64_t checkpoint_id,
        int64_t timeout_ms = 60000
    );
    arrow::Status CancelBarrier(int64_t checkpoint_id);

    // Barrier status
    arrow::Result<BarrierStatus> GetBarrierStatus(int64_t checkpoint_id);
    arrow::Result<std::vector<BarrierStatus>> GetActiveBarriers();
    arrow::Result<bool> IsBarrierCompleted(int64_t checkpoint_id);

    // Partition management
    arrow::Status RegisterPartitionSource(
        const std::string& execution_id,
        const std::string& partition_id,
        const std::string& source_type,
        const std::unordered_map<std::string, std::string>& config
    );
    arrow::Status UnregisterPartitionSource(
        const std::string& execution_id,
        const std::string& partition_id
    );

    // State management
    arrow::Status SnapshotOperatorState(
        int64_t checkpoint_id,
        const std::string& operator_id,
        const std::unordered_map<std::string, std::string>& state
    );
    arrow::Result<std::unordered_map<std::string, std::string>> RestoreOperatorState(
        int64_t checkpoint_id,
        const std::string& operator_id
    );

    // Offset management
    arrow::Status CommitPartitionOffsets(
        int64_t checkpoint_id,
        const std::unordered_map<std::string, int64_t>& offsets
    );
    arrow::Result<std::unordered_map<std::string, int64_t>> GetCommittedOffsets(
        int64_t checkpoint_id
    );

    // Recovery
    arrow::Status RecoverFromCheckpoint(int64_t checkpoint_id);
    arrow::Result<std::vector<int64_t>> GetAvailableCheckpoints();

private:
    // Internal state
    std::shared_ptr<CheckpointCoordinator> checkpoint_coordinator_;
    std::shared_ptr<MarbleDBIntegration> marbledb_;
    
    // Barrier tracking
    std::unordered_map<int64_t, BarrierStatus> active_barriers_;
    std::unordered_map<int64_t, std::vector<PartitionBarrier>> barrier_partitions_;
    std::mutex barriers_mutex_;
    
    // Partition sources
    std::unordered_map<std::string, std::unordered_map<std::string, std::string>> execution_partitions_;
    std::mutex partitions_mutex_;
    
    // State snapshots
    std::unordered_map<int64_t, std::unordered_map<std::string, std::unordered_map<std::string, std::string>>> checkpoint_states_;
    std::mutex states_mutex_;
    
    // Offset tracking
    std::unordered_map<int64_t, std::unordered_map<std::string, int64_t>> checkpoint_offsets_;
    std::mutex offsets_mutex_;
    
    // Background thread
    std::atomic<bool> running_;
    std::thread barrier_thread_;
    std::condition_variable barrier_cv_;
    
    // Internal methods
    void BarrierThreadLoop();
    arrow::Status ProcessBarrier(int64_t checkpoint_id);
    arrow::Status InjectBarrierIntoPartition(
        const std::string& partition_id,
        int64_t checkpoint_id
    );
    arrow::Status WaitForPartitionAlignment(
        int64_t checkpoint_id,
        const std::string& partition_id,
        int64_t timeout_ms
    );
    arrow::Status CompleteCheckpoint(int64_t checkpoint_id);
    arrow::Status FailCheckpoint(int64_t checkpoint_id, const std::string& error_message);
    
    // State persistence
    arrow::Status PersistCheckpointState(int64_t checkpoint_id);
    arrow::Status LoadCheckpointState(int64_t checkpoint_id);
    arrow::Status CleanupCheckpoint(int64_t checkpoint_id);
    
    // Offset persistence
    arrow::Status PersistCheckpointOffsets(int64_t checkpoint_id);
    arrow::Status LoadCheckpointOffsets(int64_t checkpoint_id);
};

/**
 * PartitionBarrierHandler - Handles barrier injection for individual partitions
 * 
 * This class manages the injection of barriers into specific partition streams
 * and tracks their alignment status.
 */
class PartitionBarrierHandler {
public:
    struct PartitionConfig {
        std::string partition_id;
        std::string source_type;  // "kafka", "file", etc.
        std::unordered_map<std::string, std::string> source_config;
        int64_t barrier_timeout_ms = 30000;
        bool enable_offset_tracking = true;
    };

public:
    PartitionBarrierHandler();
    ~PartitionBarrierHandler();

    // Lifecycle
    arrow::Status Initialize(const PartitionConfig& config);
    arrow::Status Shutdown();

    // Barrier injection
    arrow::Status InjectBarrier(int64_t checkpoint_id);
    arrow::Status WaitForBarrierAlignment(int64_t checkpoint_id, int64_t timeout_ms);
    arrow::Status CancelBarrier(int64_t checkpoint_id);

    // Status tracking
    arrow::Result<bool> IsBarrierAligned(int64_t checkpoint_id);
    arrow::Result<int64_t> GetBarrierOffset(int64_t checkpoint_id);
    arrow::Result<std::string> GetBarrierError(int64_t checkpoint_id);

    // Offset management
    arrow::Status CommitOffset(int64_t checkpoint_id, int64_t offset);
    arrow::Result<int64_t> GetCommittedOffset(int64_t checkpoint_id);

private:
    PartitionConfig config_;
    std::atomic<bool> running_;
    
    // Barrier tracking
    std::unordered_map<int64_t, bool> barrier_aligned_;
    std::unordered_map<int64_t, int64_t> barrier_offsets_;
    std::unordered_map<int64_t, std::string> barrier_errors_;
    std::mutex barrier_mutex_;
    
    // Offset tracking
    std::unordered_map<int64_t, int64_t> committed_offsets_;
    std::mutex offset_mutex_;
    
    // Internal methods
    arrow::Status InjectKafkaBarrier(int64_t checkpoint_id);
    arrow::Status InjectFileBarrier(int64_t checkpoint_id);
    arrow::Status WaitForKafkaBarrierAlignment(int64_t checkpoint_id, int64_t timeout_ms);
    arrow::Status WaitForFileBarrierAlignment(int64_t checkpoint_id, int64_t timeout_ms);
};

} // namespace streaming
} // namespace sabot_sql
