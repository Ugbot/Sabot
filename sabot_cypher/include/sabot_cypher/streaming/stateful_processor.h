// Stateful Stream and Batch Processor
//
// High-performance stateful processing for streaming and batch graph analytics.

#pragma once

#include "sabot_cypher/state/unified_state_store.h"
#include <arrow/api.h>
#include <memory>
#include <string>
#include <vector>
#include <functional>

namespace sabot_cypher {
namespace streaming {

/// State snapshot for checkpointing
struct StateSnapshot {
    std::string snapshot_id;
    std::string processor_id;
    std::unordered_map<std::string, std::string> state_data;
    int64_t processed_events;
    Timestamp timestamp;
};

/// Processing mode
enum class ProcessingMode {
    STREAMING,  // Continuous stream processing
    BATCH,      // Batch processing
    HYBRID      // Both streaming and batch
};

/// Stateful operator for stream/batch processing
class StatefulOperator {
public:
    virtual ~StatefulOperator() = default;
    
    /// Process a batch of data
    /// @param input Input Arrow table
    /// @param state Current state
    /// @return Processed output and updated state
    virtual arrow::Result<std::pair<std::shared_ptr<arrow::Table>, std::unordered_map<std::string, std::string>>>
    Process(std::shared_ptr<arrow::Table> input, 
            const std::unordered_map<std::string, std::string>& state) = 0;
    
    /// Get operator name
    virtual std::string GetName() const = 0;
};

/// Stateful aggregation operator
class StatefulAggregator : public StatefulOperator {
public:
    StatefulAggregator(const std::string& key_column,
                       const std::string& value_column,
                       const std::string& agg_function);
    
    arrow::Result<std::pair<std::shared_ptr<arrow::Table>, std::unordered_map<std::string, std::string>>>
    Process(std::shared_ptr<arrow::Table> input,
            const std::unordered_map<std::string, std::string>& state) override;
    
    std::string GetName() const override { return "StatefulAggregator"; }

private:
    std::string key_column_;
    std::string value_column_;
    std::string agg_function_;
};

/// Stateful join operator
class StatefulJoin : public StatefulOperator {
public:
    StatefulJoin(const std::string& left_key,
                 const std::string& right_key,
                 const std::string& join_type);
    
    arrow::Result<std::pair<std::shared_ptr<arrow::Table>, std::unordered_map<std::string, std::string>>>
    Process(std::shared_ptr<arrow::Table> input,
            const std::unordered_map<std::string, std::string>& state) override;
    
    std::string GetName() const override { return "StatefulJoin"; }

private:
    std::string left_key_;
    std::string right_key_;
    std::string join_type_;
};

/// Stateful stream and batch processor
///
/// Features:
/// - Stateful stream processing (continuous)
/// - Batch processing (scheduled)
/// - State snapshots and recovery
/// - Exactly-once semantics
/// - Fault tolerance
class StatefulProcessor {
public:
    /// Constructor
    /// @param processor_id Unique processor ID
    /// @param state_store Unified state store
    /// @param mode Processing mode
    StatefulProcessor(const std::string& processor_id,
                      std::shared_ptr<state::UnifiedStateStore> state_store,
                      ProcessingMode mode = ProcessingMode::STREAMING);
    
    ~StatefulProcessor() = default;
    
    // ============================================================
    // STREAM PROCESSING
    // ============================================================
    
    /// Process stream batch
    /// @param input Input Arrow table
    /// @return Processed output
    arrow::Result<std::shared_ptr<arrow::Table>> ProcessStreamBatch(std::shared_ptr<arrow::Table> input);
    
    /// Register stateful operator
    /// @param operator_name Operator name
    /// @param op Stateful operator
    /// @return Status
    arrow::Status RegisterOperator(const std::string& operator_name,
                                    std::shared_ptr<StatefulOperator> op);
    
    /// Start stream processing
    /// @return Status
    arrow::Status StartStreaming();
    
    /// Stop stream processing
    /// @return Status
    arrow::Status StopStreaming();
    
    // ============================================================
    // BATCH PROCESSING
    // ============================================================
    
    /// Process batch
    /// @param input Input Arrow table
    /// @return Processed output
    arrow::Result<std::shared_ptr<arrow::Table>> ProcessBatch(std::shared_ptr<arrow::Table> input);
    
    /// Schedule batch job
    /// @param job_id Job ID
    /// @param query Cypher query or processing logic
    /// @param schedule Cron schedule
    /// @return Status
    arrow::Status ScheduleBatchJob(const std::string& job_id,
                                    const std::string& query,
                                    const std::string& schedule);
    
    /// Execute batch job
    /// @param job_id Job ID
    /// @return Job result
    arrow::Result<std::shared_ptr<arrow::Table>> ExecuteBatchJob(const std::string& job_id);
    
    // ============================================================
    // STATE MANAGEMENT
    // ============================================================
    
    /// Save state snapshot
    /// @return Snapshot ID
    arrow::Result<std::string> SaveSnapshot();
    
    /// Load state snapshot
    /// @param snapshot_id Snapshot ID
    /// @return Status
    arrow::Status LoadSnapshot(const std::string& snapshot_id);
    
    /// Get current state
    /// @return State data
    std::unordered_map<std::string, std::string> GetState() const;
    
    /// Update state
    /// @param key State key
    /// @param value State value
    /// @return Status
    arrow::Status UpdateState(const std::string& key, const std::string& value);
    
    // ============================================================
    // FAULT TOLERANCE
    // ============================================================
    
    /// Enable exactly-once semantics
    /// @param enable Enable or disable
    /// @return Status
    arrow::Status SetExactlyOnce(bool enable);
    
    /// Set checkpoint interval
    /// @param interval Checkpoint interval in milliseconds
    /// @return Status
    arrow::Status SetCheckpointInterval(int64_t interval_ms);
    
    /// Recover from failure
    /// @return Status
    arrow::Status Recover();
    
    // ============================================================
    // STATISTICS
    // ============================================================
    
    struct Stats {
        int64_t processed_events;
        int64_t processed_batches;
        int64_t snapshots_saved;
        double avg_batch_time_ms;
        double avg_snapshot_time_ms;
        int64_t failures;
        int64_t recoveries;
    };
    
    Stats GetStats() const;

private:
    /// Apply operators to data
    arrow::Result<std::shared_ptr<arrow::Table>> ApplyOperators(std::shared_ptr<arrow::Table> input);
    
    /// Checkpoint state
    arrow::Status Checkpoint();

private:
    std::string processor_id_;
    std::shared_ptr<state::UnifiedStateStore> state_store_;
    ProcessingMode mode_;
    
    // Stateful operators
    std::vector<std::pair<std::string, std::shared_ptr<StatefulOperator>>> operators_;
    
    // Current state
    std::unordered_map<std::string, std::string> state_;
    
    // Statistics
    int64_t processed_events_;
    int64_t processed_batches_;
    int64_t snapshots_saved_;
    int64_t failures_;
    int64_t recoveries_;
    
    // Configuration
    bool exactly_once_;
    int64_t checkpoint_interval_ms_;
    int64_t last_checkpoint_time_;
    
    // Running state
    bool is_running_;
};

} // namespace streaming
} // namespace sabot_cypher

