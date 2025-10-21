#pragma once

#include <arrow/api.h>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>
#include <mutex>
#include <atomic>

namespace marble {
    class Client;  // Forward declare MarbleDB client
    class Table;   // Forward declare MarbleDB table
}

namespace sabot_sql {
namespace streaming {

/**
 * MarbleDB integration for streaming SQL
 * 
 * Provides unified state management for streaming SQL using MarbleDB:
 * - RAFT-replicated tables for dimension tables and connector offsets
 * - Local tables for streaming state (window aggregates, join buffers)
 * - Timer API for watermark triggers
 * - Checkpoint coordination for exactly-once processing
 */
class MarbleDBIntegration {
public:
    /**
     * Table configuration for MarbleDB
     */
    struct TableConfig {
        std::string table_name;
        std::shared_ptr<arrow::Schema> schema;
        bool is_raft_replicated;
        std::string description;
        
        TableConfig() = default;
        TableConfig(const std::string& name, std::shared_ptr<arrow::Schema> s, bool raft)
            : table_name(name), schema(std::move(s)), is_raft_replicated(raft) {}
    };
    
    /**
     * Timer configuration for MarbleDB Timer API
     */
    struct TimerConfig {
        std::string timer_name;
        int64_t trigger_time;
        std::string callback_data;
        bool is_recurring;
        int64_t interval_ms;
        
        TimerConfig() = default;
        TimerConfig(const std::string& name, int64_t trigger, const std::string& data)
            : timer_name(name), trigger_time(trigger), callback_data(data), is_recurring(false), interval_ms(0) {}
    };
    
    /**
     * Checkpoint state for MarbleDB
     */
    struct CheckpointState {
        int64_t checkpoint_id;
        int64_t timestamp;
        std::unordered_map<std::string, std::string> state_snapshots;
        bool is_committed;
        
        CheckpointState() = default;
        CheckpointState(int64_t id, int64_t ts)
            : checkpoint_id(id), timestamp(ts), is_committed(false) {}
    };
    
    MarbleDBIntegration();
    ~MarbleDBIntegration();
    
    // ========== Lifecycle ==========
    
    /**
     * Initialize embedded MarbleDB integration
     * 
     * @param integration_id Unique identifier for this integration
     * @param db_path Path for embedded MarbleDB database
     * @param enable_raft Whether to enable RAFT replication for this instance
     * @return Status indicating success/failure
     */
    arrow::Status Initialize(
        const std::string& integration_id,
        const std::string& db_path = "./marbledb_embedded",
        bool enable_raft = false
    );
    
    /**
     * Shutdown MarbleDB integration
     */
    arrow::Status Shutdown();
    
    // ========== Table Management ==========
    
    /**
     * Create a table in MarbleDB
     * 
     * @param config Table configuration
     * @return Status indicating success/failure
     */
    arrow::Status CreateTable(const TableConfig& config);
    
    /**
     * Drop a table from MarbleDB
     * 
     * @param table_name Name of the table to drop
     * @return Status indicating success/failure
     */
    arrow::Status DropTable(const std::string& table_name);
    
    /**
     * Check if a table exists
     * 
     * @param table_name Name of the table
     * @return true if table exists
     */
    arrow::Result<bool> TableExists(const std::string& table_name);
    
    /**
     * Get table schema
     * 
     * @param table_name Name of the table
     * @return Table schema
     */
    arrow::Result<std::shared_ptr<arrow::Schema>> GetTableSchema(const std::string& table_name);
    
    // ========== Data Operations ==========
    
    /**
     * Write data to a table
     * 
     * @param table_name Name of the table
     * @param batch Data to write
     * @return Status indicating success/failure
     */
    arrow::Status WriteTable(
        const std::string& table_name,
        std::shared_ptr<arrow::RecordBatch> batch
    );
    
    /**
     * Read data from a table
     * 
     * @param table_name Name of the table
     * @param filter Optional filter expression
     * @param limit Optional row limit
     * @return Data from the table
     */
    arrow::Result<std::shared_ptr<arrow::Table>> ReadTable(
        const std::string& table_name,
        const std::string& filter = "",
        int64_t limit = -1
    );
    
    /**
     * Update data in a table
     * 
     * @param table_name Name of the table
     * @param batch Data to update
     * @param key_columns Columns to use for matching
     * @return Status indicating success/failure
     */
    arrow::Status UpdateTable(
        const std::string& table_name,
        std::shared_ptr<arrow::RecordBatch> batch,
        const std::vector<std::string>& key_columns
    );
    
    /**
     * Delete data from a table
     * 
     * @param table_name Name of the table
     * @param filter Filter expression for rows to delete
     * @return Status indicating success/failure
     */
    arrow::Status DeleteFromTable(
        const std::string& table_name,
        const std::string& filter
    );
    
    // ========== RAFT Operations ==========
    
    /**
     * Check if table is RAFT replicated
     * 
     * @param table_name Name of the table
     * @return true if table is RAFT replicated
     */
    arrow::Result<bool> IsTableRaftReplicated(const std::string& table_name);
    
    /**
     * Get RAFT replication status
     * 
     * @param table_name Name of the table
     * @return Replication status information
     */
    arrow::Result<std::unordered_map<std::string, std::string>> GetRaftStatus(const std::string& table_name);
    
    /**
     * Wait for RAFT replication to complete
     * 
     * @param table_name Name of the table
     * @param timeout_ms Maximum time to wait
     * @return Status indicating success/failure
     */
    arrow::Status WaitForRaftReplication(
        const std::string& table_name,
        int64_t timeout_ms = 30000
    );
    
    // ========== Timer API ==========
    
    /**
     * Register a timer with MarbleDB
     * 
     * @param config Timer configuration
     * @return Status indicating success/failure
     */
    arrow::Status RegisterTimer(const TimerConfig& config);
    
    /**
     * Cancel a timer
     * 
     * @param timer_name Name of the timer to cancel
     * @return Status indicating success/failure
     */
    arrow::Status CancelTimer(const std::string& timer_name);
    
    /**
     * Get pending timers
     * 
     * @return List of pending timer names
     */
    arrow::Result<std::vector<std::string>> GetPendingTimers();
    
    /**
     * Check if timer exists
     * 
     * @param timer_name Name of the timer
     * @return true if timer exists
     */
    arrow::Result<bool> TimerExists(const std::string& timer_name);
    
    // ========== Checkpoint Operations ==========
    
    /**
     * Create a checkpoint
     * 
     * @param checkpoint_id Checkpoint identifier
     * @param state_snapshots State snapshots from all operators
     * @return Status indicating success/failure
     */
    arrow::Status CreateCheckpoint(
        int64_t checkpoint_id,
        const std::unordered_map<std::string, std::string>& state_snapshots
    );
    
    /**
     * Get checkpoint state
     * 
     * @param checkpoint_id Checkpoint identifier
     * @return Checkpoint state
     */
    arrow::Result<CheckpointState> GetCheckpointState(int64_t checkpoint_id);
    
    /**
     * Commit a checkpoint
     * 
     * @param checkpoint_id Checkpoint identifier
     * @return Status indicating success/failure
     */
    arrow::Status CommitCheckpoint(int64_t checkpoint_id);
    
    /**
     * Get the last committed checkpoint ID
     * 
     * @return Last committed checkpoint ID
     */
    arrow::Result<int64_t> GetLastCommittedCheckpoint();
    
    /**
     * Clean up old checkpoints
     * 
     * @param keep_count Number of checkpoints to keep
     * @return Status indicating success/failure
     */
    arrow::Status CleanupOldCheckpoints(int keep_count = 10);
    
    // ========== State Operations ==========
    
    /**
     * Write state to MarbleDB
     * 
     * @param key State key
     * @param value State value
     * @return Status indicating success/failure
     */
    arrow::Status WriteState(const std::string& key, const std::string& value);
    
    /**
     * Read state from MarbleDB
     * 
     * @param key State key
     * @return State value
     */
    arrow::Result<std::string> ReadState(const std::string& key);
    
    // ========== Monitoring ==========
    
    /**
     * Get integration statistics
     */
    struct IntegrationStats {
        size_t total_tables;
        size_t raft_replicated_tables;
        size_t local_tables;
        size_t active_timers;
        int64_t last_checkpoint_id;
        int64_t last_checkpoint_time;
        std::string integration_id;
    };
    
    IntegrationStats GetStats() const;
    
    /**
     * Get list of all tables
     */
    std::vector<std::string> GetTableNames() const;
    
    /**
     * Get table information
     */
    arrow::Result<TableConfig> GetTableInfo(const std::string& table_name);
    
    // ========== Configuration ==========
    
    /**
     * Set default RAFT replication timeout
     */
    void SetRaftTimeout(int64_t timeout_ms);
    
    /**
     * Enable/disable automatic checkpoint cleanup
     */
    void SetAutoCheckpointCleanup(bool enabled);
    
private:
    // Configuration
    std::string integration_id_;
    std::string db_path_;
    bool enable_raft_;
    bool initialized_;
    
    // Embedded MarbleDB instance
    void* embedded_db_;  // Will be marble::Database* in real implementation
    int64_t raft_timeout_ms_{30000};
    std::atomic<bool> auto_checkpoint_cleanup_{true};
    
    // State
    mutable std::mutex mutex_;
    std::unordered_map<std::string, TableConfig> table_configs_;
    std::unordered_map<std::string, bool> raft_status_;
    std::unordered_map<int64_t, CheckpointState> checkpoint_states_;
    
    // Statistics
    std::atomic<int64_t> last_checkpoint_id_{0};
    std::atomic<int64_t> last_checkpoint_time_{0};
    
    // MarbleDB table names
    std::string metadata_table_name_;
    std::string checkpoint_table_name_;
    std::string timer_table_name_;
    
    // Helper methods
    arrow::Status InitializeMarbleDBTables();
    arrow::Status SaveTableConfig(const std::string& table_name, const TableConfig& config);
    arrow::Result<TableConfig> LoadTableConfig(const std::string& table_name);
    arrow::Status UpdateRaftStatus(const std::string& table_name, bool is_replicated);
    arrow::Status SaveCheckpointState(int64_t checkpoint_id, const CheckpointState& state);
    arrow::Result<CheckpointState> LoadCheckpointState(int64_t checkpoint_id);
    arrow::Status SaveTimerConfig(const std::string& timer_name, const TimerConfig& config);
    arrow::Result<TimerConfig> LoadTimerConfig(const std::string& timer_name);
};

/**
 * MarbleDB state backend for streaming operators
 * 
 * Provides a unified interface for streaming operators to interact with MarbleDB.
 */
class MarbleDBStateBackend {
public:
    /**
     * State key for streaming operators
     */
    struct StateKey {
        std::string operator_id;
        std::string key;
        int64_t window_start;
        int64_t window_end;
        
        StateKey() = default;
        StateKey(const std::string& op_id, const std::string& k, int64_t start, int64_t end)
            : operator_id(op_id), key(k), window_start(start), window_end(end) {}
        
        std::string ToString() const {
            return operator_id + ":" + key + ":" + std::to_string(window_start) + ":" + std::to_string(window_end);
        }
    };
    
    /**
     * State value for streaming operators
     */
    struct StateValue {
        std::unordered_map<std::string, std::string> data;
        int64_t timestamp;
        int64_t version;
        
        StateValue() = default;
        StateValue(const std::unordered_map<std::string, std::string>& d, int64_t ts)
            : data(d), timestamp(ts), version(0) {}
    };
    
    MarbleDBStateBackend();
    ~MarbleDBStateBackend();
    
    /**
     * Initialize state backend
     */
    arrow::Status Initialize(std::shared_ptr<MarbleDBIntegration> integration);
    
    /**
     * Get state value
     */
    arrow::Result<StateValue> GetState(const StateKey& key);
    
    /**
     * Set state value
     */
    arrow::Status SetState(const StateKey& key, const StateValue& value);
    
    /**
     * Delete state value
     */
    arrow::Status DeleteState(const StateKey& key);
    
    /**
     * List all state keys for an operator
     */
    arrow::Result<std::vector<StateKey>> ListStateKeys(const std::string& operator_id);
    
    /**
     * Clear all state for an operator
     */
    arrow::Status ClearOperatorState(const std::string& operator_id);
    
private:
    std::shared_ptr<MarbleDBIntegration> integration_;
    std::string state_table_name_;
};

} // namespace streaming
} // namespace sabot_sql
