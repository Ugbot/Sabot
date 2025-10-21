//===----------------------------------------------------------------------===//
//                         Sabot
//
// sabot/_c/agent_core.hpp
//
// C++ Agent Core - High-Performance Streaming SQL Execution Engine
//
// This is the core C++ implementation of the agent with minimal Python dependencies.
// Only high-level control and configuration comes from Python.
//
//===----------------------------------------------------------------------===//

#pragma once

#include <arrow/api.h>
#include <memory>
#include <string>
#include <atomic>
#include <thread>
#include <vector>
#include <unordered_map>
#include <functional>
#include <mutex>
#include <queue>
#include <condition_variable>

// Forward declarations
namespace sabot {
    class TaskSlotManager;
    class ShuffleTransport;
    class MarbleDBIntegration;
    class StreamingOperator;
    class StreamingSource;
    class DimensionTableManager;
    class CheckpointCoordinator;
    class WatermarkTracker;
}

namespace sabot {

//! Agent configuration (from Python)
struct AgentConfig {
    std::string agent_id;
    std::string host;
    int port;
    int memory_mb;
    int num_slots;
    int workers_per_slot;
    std::string marbledb_path;
    bool enable_raft;
    bool is_local_mode;
    int checkpoint_interval_ms;
    int watermark_idle_timeout_ms;
    
    AgentConfig() = default;
    
    AgentConfig(const std::string& id, const std::string& h, int p, 
                int mem, int slots, int workers, bool local = true)
        : agent_id(id), host(h), port(p), memory_mb(mem), 
          num_slots(slots), workers_per_slot(workers), is_local_mode(local),
          checkpoint_interval_ms(60000), watermark_idle_timeout_ms(30000) {
        marbledb_path = "./agent_" + agent_id + "_marbledb";
        enable_raft = !is_local_mode;
    }
};

//! Task specification
struct TaskSpec {
    std::string task_id;
    std::string operator_type;
    std::unordered_map<std::string, std::string> parameters;
    std::shared_ptr<arrow::Schema> input_schema;
    std::shared_ptr<arrow::Schema> output_schema;
    std::vector<std::string> input_tables;
    std::vector<std::string> output_tables;
    
    TaskSpec() = default;
};

//! Streaming operator configuration
struct StreamingOperatorConfig {
    std::string operator_id;
    std::string operator_type;
    std::unordered_map<std::string, std::string> parameters;
    std::shared_ptr<arrow::Schema> input_schema;
    std::shared_ptr<arrow::Schema> output_schema;
    bool is_stateful;
    bool requires_checkpointing;
    bool requires_watermarks;
    
    StreamingOperatorConfig() = default;
};

//! Agent status
struct AgentStatus {
    std::string agent_id;
    bool running;
    int active_tasks;
    int available_slots;
    int64_t total_morsels_processed;
    int64_t total_bytes_shuffled;
    std::string marbledb_path;
    bool marbledb_initialized;
    int64_t uptime_ms;
    double cpu_usage_percent;
    double memory_usage_percent;
    
    AgentStatus() : running(false), active_tasks(0), available_slots(0),
                   total_morsels_processed(0), total_bytes_shuffled(0),
                   marbledb_initialized(false), uptime_ms(0),
                   cpu_usage_percent(0.0), memory_usage_percent(0.0) {}
};

//! C++ Agent Core - High-Performance Streaming SQL Execution Engine
//!
//! This is the core C++ implementation with minimal Python dependencies.
//! Features:
//! - Embedded MarbleDB for all state management
//! - High-performance morsel parallelism
//! - Lock-free network transport
//! - Streaming SQL execution
//! - Automatic checkpointing and watermarks
//! - Dimension table management
//! - Connector state management
class AgentCore {
public:
    //! Constructor
    explicit AgentCore(const AgentConfig& config);
    
    //! Destructor
    ~AgentCore();
    
    //! Initialize agent core
    arrow::Status Initialize();
    
    //! Shutdown agent core
    arrow::Status Shutdown();
    
    //! Start agent services
    arrow::Status Start();
    
    //! Stop agent services
    arrow::Status Stop();
    
    //! Deploy streaming operator
    arrow::Status DeployStreamingOperator(const StreamingOperatorConfig& config);
    
    //! Stop streaming operator
    arrow::Status StopStreamingOperator(const std::string& operator_id);
    
    //! Register dimension table
    arrow::Status RegisterDimensionTable(
        const std::string& table_name,
        std::shared_ptr<arrow::Table> table,
        bool is_raft_replicated = false
    );
    
    //! Register streaming source
    arrow::Status RegisterStreamingSource(
        const std::string& source_name,
        const std::string& connector_type,
        const std::unordered_map<std::string, std::string>& config
    );
    
    //! Execute batch SQL query
    arrow::Result<std::shared_ptr<arrow::Table>> ExecuteBatchSQL(
        const std::string& query,
        const std::unordered_map<std::string, std::shared_ptr<arrow::Table>>& input_tables
    );
    
    //! Execute streaming SQL query
    arrow::Status ExecuteStreamingSQL(
        const std::string& query,
        const std::unordered_map<std::string, std::shared_ptr<arrow::Table>>& input_tables,
        std::function<void(std::shared_ptr<arrow::RecordBatch>)> output_callback = nullptr
    );
    
    //! Get agent status
    AgentStatus GetStatus() const;
    
    //! Get embedded MarbleDB instance
    std::shared_ptr<MarbleDBIntegration> GetMarbleDB() const;
    
    //! Get task slot manager
    std::shared_ptr<TaskSlotManager> GetTaskSlotManager() const;
    
    //! Get shuffle transport
    std::shared_ptr<ShuffleTransport> GetShuffleTransport() const;
    
    //! Get dimension table manager
    std::shared_ptr<DimensionTableManager> GetDimensionTableManager() const;
    
    //! Get checkpoint coordinator
    std::shared_ptr<CheckpointCoordinator> GetCheckpointCoordinator() const;
    
    //! Check if running
    bool IsRunning() const { return running_.load(std::memory_order_relaxed); }
    
    //! Get agent ID
    const std::string& GetAgentId() const { return config_.agent_id; }

private:
    //! Configuration
    AgentConfig config_;
    
    //! Running state
    std::atomic<bool> running_;
    std::atomic<bool> initialized_;
    std::atomic<bool> shutdown_requested_;
    
    //! Core components
    std::shared_ptr<TaskSlotManager> task_slot_manager_;
    std::shared_ptr<ShuffleTransport> shuffle_transport_;
    std::shared_ptr<MarbleDBIntegration> marbledb_;
    std::shared_ptr<DimensionTableManager> dimension_manager_;
    std::shared_ptr<CheckpointCoordinator> checkpoint_coordinator_;
    
    //! Streaming operators
    std::unordered_map<std::string, std::shared_ptr<StreamingOperator>> streaming_operators_;
    std::unordered_map<std::string, std::shared_ptr<StreamingSource>> streaming_sources_;
    
    //! Thread safety
    mutable std::mutex operators_mutex_;
    mutable std::mutex sources_mutex_;
    
    //! Background threads
    std::thread checkpoint_thread_;
    std::thread watermark_thread_;
    std::thread monitoring_thread_;
    
    //! Statistics
    mutable std::atomic<int64_t> total_morsels_processed_;
    mutable std::atomic<int64_t> total_bytes_shuffled_;
    mutable std::atomic<int64_t> start_time_ms_;
    
    //! Initialize core components
    arrow::Status InitializeMarbleDB();
    arrow::Status InitializeTaskSlotManager();
    arrow::Status InitializeShuffleTransport();
    arrow::Status InitializeDimensionManager();
    arrow::Status InitializeCheckpointCoordinator();
    
    //! Shutdown core components
    arrow::Status ShutdownMarbleDB();
    arrow::Status ShutdownTaskSlotManager();
    arrow::Status ShutdownShuffleTransport();
    arrow::Status ShutdownDimensionManager();
    arrow::Status ShutdownCheckpointCoordinator();
    
    //! Background thread functions
    void CheckpointThreadLoop();
    void WatermarkThreadLoop();
    void MonitoringThreadLoop();
    
    //! Internal helper functions
    arrow::Status ParseSQLQuery(const std::string& query, StreamingOperatorConfig& config);
    arrow::Status CreateStreamingOperator(const StreamingOperatorConfig& config, 
                                         std::shared_ptr<StreamingOperator>& operator);
    arrow::Status ValidateOperatorConfig(const StreamingOperatorConfig& config);
    
    //! Statistics collection
    void UpdateStatistics();
    double GetCPUUsage() const;
    double GetMemoryUsage() const;
};

//! Streaming Operator - C++ implementation
//!
//! High-performance streaming operator with embedded MarbleDB state.
class StreamingOperator {
public:
    StreamingOperator(const StreamingOperatorConfig& config,
                     std::shared_ptr<MarbleDBIntegration> marbledb,
                     std::shared_ptr<TaskSlotManager> slot_manager);
    
    ~StreamingOperator();
    
    //! Start operator
    arrow::Status Start();
    
    //! Stop operator
    arrow::Status Stop();
    
    //! Process batch
    arrow::Result<std::shared_ptr<arrow::RecordBatch>> ProcessBatch(
        std::shared_ptr<arrow::RecordBatch> batch);
    
    //! Get operator ID
    const std::string& GetOperatorId() const { return config_.operator_id; }
    
    //! Get operator type
    const std::string& GetOperatorType() const { return config_.operator_type; }
    
    //! Check if running
    bool IsRunning() const { return running_.load(std::memory_order_relaxed); }

private:
    StreamingOperatorConfig config_;
    std::shared_ptr<MarbleDBIntegration> marbledb_;
    std::shared_ptr<TaskSlotManager> slot_manager_;
    
    std::atomic<bool> running_;
    std::thread processing_thread_;
    
    //! State management
    std::string state_table_name_;
    std::string watermark_table_name_;
    
    //! Processing functions
    void ProcessingLoop();
    arrow::Status InitializeState();
    arrow::Status UpdateState(const std::string& key, const std::string& value);
    arrow::Status ReadState(const std::string& key, std::string& value);
    arrow::Status UpdateWatermark(int32_t partition_id, int64_t timestamp);
    int64_t GetWatermark(int32_t partition_id);
    
    //! Operator-specific processing
    arrow::Result<std::shared_ptr<arrow::RecordBatch>> ProcessWindowAggregate(
        std::shared_ptr<arrow::RecordBatch> batch);
    arrow::Result<std::shared_ptr<arrow::RecordBatch>> ProcessJoin(
        std::shared_ptr<arrow::RecordBatch> batch);
    arrow::Result<std::shared_ptr<arrow::RecordBatch>> ProcessFilter(
        std::shared_ptr<arrow::RecordBatch> batch);
};

//! Streaming Source - C++ implementation
//!
//! High-performance streaming source with embedded MarbleDB offset management.
class StreamingSource {
public:
    StreamingSource(const std::string& source_name,
                   const std::string& connector_type,
                   const std::unordered_map<std::string, std::string>& config,
                   std::shared_ptr<MarbleDBIntegration> marbledb);
    
    ~StreamingSource();
    
    //! Start source
    arrow::Status Start();
    
    //! Stop source
    arrow::Status Stop();
    
    //! Fetch next batch
    arrow::Result<std::shared_ptr<arrow::RecordBatch>> FetchNextBatch();
    
    //! Commit offset
    arrow::Status CommitOffset(int32_t partition, int64_t offset, int64_t timestamp);
    
    //! Get last offset
    arrow::Result<int64_t> GetLastOffset(int32_t partition);
    
    //! Get source name
    const std::string& GetSourceName() const { return source_name_; }
    
    //! Check if running
    bool IsRunning() const { return running_.load(std::memory_order_relaxed); }

private:
    std::string source_name_;
    std::string connector_type_;
    std::unordered_map<std::string, std::string> config_;
    std::shared_ptr<MarbleDBIntegration> marbledb_;
    
    std::atomic<bool> running_;
    std::thread fetching_thread_;
    
    //! Offset management
    std::string offset_table_name_;
    std::unordered_map<int32_t, int64_t> last_offsets_;
    
    //! Processing functions
    void FetchingLoop();
    arrow::Status InitializeOffsets();
    arrow::Status LoadOffsets();
    arrow::Status SaveOffsets();
    
    //! Connector-specific functions
    arrow::Result<std::shared_ptr<arrow::RecordBatch>> FetchFromKafka();
    arrow::Result<std::shared_ptr<arrow::RecordBatch>> FetchFromFile();
    arrow::Result<std::shared_ptr<arrow::RecordBatch>> FetchFromMemory();
};

//! Dimension Table Manager - C++ implementation
//!
//! Manages dimension tables with RAFT replication support.
class DimensionTableManager {
public:
    DimensionTableManager(std::shared_ptr<MarbleDBIntegration> marbledb);
    
    ~DimensionTableManager();
    
    //! Initialize manager
    arrow::Status Initialize();
    
    //! Shutdown manager
    arrow::Status Shutdown();
    
    //! Register dimension table
    arrow::Status RegisterTable(
        const std::string& table_name,
        std::shared_ptr<arrow::Table> table,
        bool is_raft_replicated = false
    );
    
    //! Get dimension table
    arrow::Result<std::shared_ptr<arrow::Table>> GetTable(const std::string& table_name);
    
    //! Update dimension table
    arrow::Status UpdateTable(
        const std::string& table_name,
        std::shared_ptr<arrow::Table> table
    );
    
    //! Remove dimension table
    arrow::Status RemoveTable(const std::string& table_name);
    
    //! List tables
    std::vector<std::string> ListTables() const;
    
    //! Check if table is RAFT replicated
    bool IsRaftReplicated(const std::string& table_name) const;

private:
    std::shared_ptr<MarbleDBIntegration> marbledb_;
    std::unordered_map<std::string, std::shared_ptr<arrow::Table>> tables_;
    std::unordered_map<std::string, bool> raft_replicated_;
    mutable std::mutex mutex_;
    
    arrow::Status SaveTableToMarbleDB(const std::string& table_name, std::shared_ptr<arrow::Table> table);
    arrow::Result<std::shared_ptr<arrow::Table>> LoadTableFromMarbleDB(const std::string& table_name);
};

} // namespace sabot
