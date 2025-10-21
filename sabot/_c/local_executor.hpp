//===----------------------------------------------------------------------===//
//                         Sabot
//
// sabot/_c/local_executor.hpp
//
// Local Executor - Automatic C++ Agent Management
//
// When running locally, automatically creates and manages a C++ agent
// behind the scenes for high-performance execution.
//
//===----------------------------------------------------------------------===//

#pragma once

#include <arrow/api.h>
#include <memory>
#include <string>
#include <atomic>
#include <thread>
#include <mutex>
#include <unordered_map>
#include <functional>

// Forward declarations
namespace sabot {
    class Agent;
    class TaskSlotManager;
    class ShuffleTransport;
    class MarbleDBIntegration;
}

namespace sabot {

//! Local execution configuration
struct LocalExecutorConfig {
    std::string executor_id;
    int memory_mb;
    int num_slots;
    int workers_per_slot;
    std::string marbledb_path;
    bool enable_raft;
    
    LocalExecutorConfig() = default;
    
    LocalExecutorConfig(const std::string& id, int mem = 1024, int slots = 4, int workers = 2)
        : executor_id(id), memory_mb(mem), num_slots(slots), workers_per_slot(workers) {
        marbledb_path = "./local_" + executor_id + "_marbledb";
        enable_raft = false;  // Local mode doesn't need RAFT
    }
};

//! Local Executor - Automatic C++ Agent Management
//!
//! When running locally, automatically creates and manages a C++ agent
//! behind the scenes for high-performance execution.
//!
//! Features:
//! - Automatic agent lifecycle management
//! - Embedded MarbleDB for state
//! - Morsel parallelism for performance
//! - Zero Python overhead in hot paths
//! - Transparent local execution
class LocalExecutor {
public:
    //! Constructor
    explicit LocalExecutor(const LocalExecutorConfig& config);
    
    //! Destructor
    ~LocalExecutor();
    
    //! Initialize local executor
    arrow::Status Initialize();
    
    //! Shutdown local executor
    arrow::Status Shutdown();
    
    //! Execute a streaming SQL query locally
    arrow::Status ExecuteStreamingSQL(
        const std::string& query,
        const std::unordered_map<std::string, std::shared_ptr<arrow::Table>>& input_tables,
        std::function<void(std::shared_ptr<arrow::RecordBatch>)> output_callback
    );
    
    //! Execute a batch SQL query locally
    arrow::Result<std::shared_ptr<arrow::Table>> ExecuteBatchSQL(
        const std::string& query,
        const std::unordered_map<std::string, std::shared_ptr<arrow::Table>>& input_tables
    );
    
    //! Register a dimension table
    arrow::Status RegisterDimensionTable(
        const std::string& table_name,
        std::shared_ptr<arrow::Table> table,
        bool is_raft_replicated = false
    );
    
    //! Register a streaming source
    arrow::Status RegisterStreamingSource(
        const std::string& source_name,
        const std::string& connector_type,
        const std::unordered_map<std::string, std::string>& config
    );
    
    //! Get embedded MarbleDB instance
    std::shared_ptr<MarbleDBIntegration> GetMarbleDB() const;
    
    //! Get task slot manager
    std::shared_ptr<TaskSlotManager> GetTaskSlotManager() const;
    
    //! Check if initialized
    bool IsInitialized() const { return initialized_.load(std::memory_order_relaxed); }
    
    //! Get executor ID
    const std::string& GetExecutorId() const { return config_.executor_id; }

private:
    //! Configuration
    LocalExecutorConfig config_;
    
    //! Initialization state
    std::atomic<bool> initialized_;
    
    //! C++ agent (created automatically)
    std::unique_ptr<Agent> agent_;
    
    //! Embedded MarbleDB
    std::shared_ptr<MarbleDBIntegration> marbledb_;
    
    //! Task slot manager
    std::shared_ptr<TaskSlotManager> task_slot_manager_;
    
    //! Shuffle transport
    std::shared_ptr<ShuffleTransport> shuffle_transport_;
    
    //! Registered tables
    std::unordered_map<std::string, std::shared_ptr<arrow::Table>> dimension_tables_;
    std::unordered_map<std::string, std::unordered_map<std::string, std::string>> streaming_sources_;
    
    //! Mutex for thread safety
    mutable std::mutex mutex_;
    
    //! Initialize C++ agent
    arrow::Status InitializeAgent();
    
    //! Shutdown C++ agent
    arrow::Status ShutdownAgent();
    
    //! Parse SQL query and create execution plan
    arrow::Result<std::string> ParseSQLQuery(const std::string& query);
    
    //! Create morsels from input tables
    std::vector<std::shared_ptr<arrow::RecordBatch>> CreateMorsels(
        const std::unordered_map<std::string, std::shared_ptr<arrow::Table>>& input_tables
    );
    
    //! Process morsels using task slot manager
    std::vector<std::shared_ptr<arrow::RecordBatch>> ProcessMorsels(
        const std::vector<std::shared_ptr<arrow::RecordBatch>>& morsels,
        const std::string& operator_type
    );
    
    //! Combine results into final table
    std::shared_ptr<arrow::Table> CombineResults(
        const std::vector<std::shared_ptr<arrow::RecordBatch>>& results
    );
};

//! Global local executor instance (singleton)
class LocalExecutorManager {
public:
    //! Get singleton instance
    static LocalExecutorManager& GetInstance();
    
    //! Get or create local executor
    std::shared_ptr<LocalExecutor> GetOrCreateExecutor(const std::string& executor_id = "default");
    
    //! Shutdown all executors
    void ShutdownAll();
    
private:
    LocalExecutorManager() = default;
    ~LocalExecutorManager();
    
    std::unordered_map<std::string, std::shared_ptr<LocalExecutor>> executors_;
    std::mutex mutex_;
};

} // namespace sabot
