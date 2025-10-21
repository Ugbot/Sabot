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
}

namespace sabot_sql {
namespace streaming {

/**
 * Dimension table broadcast manager
 * 
 * Manages dimension table loading and replication via MarbleDB RAFT.
 * Provides zero-shuffle joins by broadcasting dimension tables to all agents.
 * 
 * Features:
 * - Orchestrator loads dimension table
 * - Writes to MarbleDB with `is_raft_replicated=true`
 * - RAFT replicates to all agents automatically
 * - Agents read from local MarbleDB replica (zero shuffle)
 * - Updates via RAFT consensus
 */
class DimensionBroadcastManager {
public:
    /**
     * Dimension table metadata
     */
    struct DimensionTable {
        std::string table_name;
        std::shared_ptr<arrow::Table> table;
        bool is_raft_replicated;
        int64_t last_updated;
        size_t row_count;
        size_t column_count;
        
        DimensionTable() = default;
        DimensionTable(const std::string& name, std::shared_ptr<arrow::Table> t, bool raft)
            : table_name(name), table(std::move(t)), is_raft_replicated(raft)
            , last_updated(0), row_count(0), column_count(0) {
            if (table) {
                row_count = table->num_rows();
                column_count = table->num_columns();
            }
        }
    };
    
    DimensionBroadcastManager();
    ~DimensionBroadcastManager();
    
    // ========== Lifecycle ==========
    
    /**
     * Initialize dimension broadcast manager with embedded MarbleDB
     * 
     * @param manager_id Unique identifier for this manager
     * @param db_path Path for embedded MarbleDB database
     * @param enable_raft Whether to enable RAFT replication
     */
    arrow::Status Initialize(
        const std::string& manager_id,
        const std::string& db_path = "./dimension_marbledb",
        bool enable_raft = true
    );
    
    /**
     * Shutdown dimension broadcast manager
     */
    arrow::Status Shutdown();
    
    // ========== Dimension Table Management ==========
    
    /**
     * Register a dimension table for broadcast
     * 
     * @param table_name Name of the dimension table
     * @param table Arrow table with dimension data
     * @param is_raft_replicated If true, store in RAFT group (broadcast to all agents)
     * @return Status indicating success/failure
     */
    arrow::Status RegisterDimensionTable(
        const std::string& table_name,
        std::shared_ptr<arrow::Table> table,
        bool is_raft_replicated = true
    );
    
    /**
     * Update a dimension table
     * 
     * @param table_name Name of the dimension table
     * @param table New Arrow table data
     * @return Status indicating success/failure
     */
    arrow::Status UpdateDimensionTable(
        const std::string& table_name,
        std::shared_ptr<arrow::Table> table
    );
    
    /**
     * Remove a dimension table
     * 
     * @param table_name Name of the dimension table
     * @return Status indicating success/failure
     */
    arrow::Status RemoveDimensionTable(const std::string& table_name);
    
    /**
     * Get a dimension table
     * 
     * @param table_name Name of the dimension table
     * @return Dimension table if found
     */
    arrow::Result<std::shared_ptr<arrow::Table>> GetDimensionTable(const std::string& table_name);
    
    /**
     * Get dimension table metadata
     * 
     * @param table_name Name of the dimension table
     * @return Dimension table metadata
     */
    arrow::Result<DimensionTable> GetDimensionTableMetadata(const std::string& table_name);
    
    // ========== RAFT Operations ==========
    
    /**
     * Broadcast dimension table to all agents via RAFT
     * 
     * @param table_name Name of the dimension table
     * @return Status indicating success/failure
     */
    arrow::Status BroadcastTable(const std::string& table_name);
    
    /**
     * Check if table is replicated to all agents
     * 
     * @param table_name Name of the dimension table
     * @return true if table is replicated to all agents
     */
    arrow::Result<bool> IsTableReplicated(const std::string& table_name);
    
    /**
     * Get replication status for all tables
     * 
     * @return Map of table_name -> replication_status
     */
    arrow::Result<std::unordered_map<std::string, bool>> GetReplicationStatus();
    
    // ========== Join Operations ==========
    
    /**
     * Perform broadcast join with dimension table
     * 
     * @param fact_table Fact table (streaming data)
     * @param dimension_table_name Name of dimension table
     * @param join_keys Join key columns
     * @param join_type Type of join (INNER, LEFT, RIGHT, FULL)
     * @return Joined result table
     */
    arrow::Result<std::shared_ptr<arrow::Table>> BroadcastJoin(
        std::shared_ptr<arrow::Table> fact_table,
        const std::string& dimension_table_name,
        const std::vector<std::string>& join_keys,
        const std::string& join_type = "LEFT"
    );
    
    /**
     * Check if table is suitable for broadcast join
     * 
     * @param table_name Name of the dimension table
     * @param max_broadcast_size Maximum size for broadcast (bytes)
     * @return true if table is suitable for broadcast
     */
    arrow::Result<bool> IsSuitableForBroadcast(
        const std::string& table_name,
        size_t max_broadcast_size = 100 * 1024 * 1024  // 100MB default
    );
    
    // ========== Monitoring ==========
    
    /**
     * Get dimension broadcast manager statistics
     */
    struct DimensionStats {
        size_t total_tables;
        size_t raft_replicated_tables;
        size_t local_tables;
        size_t total_rows;
        size_t total_columns;
        int64_t last_broadcast_time;
        std::string manager_id;
    };
    
    DimensionStats GetStats() const;
    
    /**
     * Get list of all dimension tables
     */
    std::vector<std::string> GetDimensionTableNames() const;
    
    // ========== Configuration ==========
    
    /**
     * Set maximum broadcast size
     */
    void SetMaxBroadcastSize(size_t max_size_bytes);
    
    /**
     * Enable/disable automatic RAFT replication
     */
    void SetAutoReplication(bool enabled);
    
private:
    // Configuration
    std::string manager_id_;
    std::string db_path_;
    bool enable_raft_;
    bool initialized_;
    size_t max_broadcast_size_bytes_{100 * 1024 * 1024};  // 100MB default
    
    // Embedded MarbleDB instance
    void* embedded_db_;  // Will be marble::Database* in real implementation
    std::atomic<bool> auto_replication_{true};
    
    // State
    mutable std::mutex mutex_;
    std::unordered_map<std::string, DimensionTable> dimension_tables_;
    std::unordered_map<std::string, bool> replication_status_;
    
    // Statistics
    std::atomic<int64_t> last_broadcast_time_{0};
    
    // MarbleDB table for persistence
    std::string metadata_table_name_;
    
    // Helper methods
    arrow::Status InitializeMarbleDBTable();
    arrow::Status SaveTableToMarbleDB(const std::string& table_name, std::shared_ptr<arrow::Table> table);
    arrow::Result<std::shared_ptr<arrow::Table>> LoadTableFromMarbleDB(const std::string& table_name);
    arrow::Status UpdateReplicationStatus(const std::string& table_name, bool is_replicated);
    size_t EstimateTableSize(std::shared_ptr<arrow::Table> table);
    arrow::Status ValidateJoinKeys(
        std::shared_ptr<arrow::Table> fact_table,
        std::shared_ptr<arrow::Table> dimension_table,
        const std::vector<std::string>& join_keys
    );
};

/**
 * Dimension table registry for Python API
 * 
 * Provides Python-friendly interface for dimension table management.
 */
class DimensionTableRegistry {
public:
    DimensionTableRegistry();
    ~DimensionTableRegistry();
    
    /**
     * Initialize registry
     */
    arrow::Status Initialize(std::shared_ptr<DimensionBroadcastManager> manager);
    
    /**
     * Register dimension table
     */
    arrow::Status RegisterTable(
        const std::string& table_name,
        std::shared_ptr<arrow::Table> table,
        bool is_raft_replicated = true
    );
    
    /**
     * Get dimension table
     */
    arrow::Result<std::shared_ptr<arrow::Table>> GetTable(const std::string& table_name);
    
    /**
     * List all tables
     */
    std::vector<std::string> ListTables() const;
    
    /**
     * Get table info
     */
    arrow::Result<DimensionBroadcastManager::DimensionTable> GetTableInfo(const std::string& table_name);
    
private:
    std::shared_ptr<DimensionBroadcastManager> manager_;
};

} // namespace streaming
} // namespace sabot_sql
