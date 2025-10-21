//===----------------------------------------------------------------------===//
//                         Sabot
//
// sabot/_c/agent.hpp
//
// C++ Agent - Headless Worker Node
//
// High-performance C++ agent for streaming SQL execution with embedded MarbleDB.
// No Python dependencies - pure C++/Cython for maximum performance.
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

// Forward declarations
namespace sabot {
    class TaskSlotManager;
    class ShuffleTransport;
    class MarbleDBIntegration;
    class TaskExecutor;
}

namespace sabot {

//! Agent configuration
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

    AgentConfig() = default;
    
    AgentConfig(const std::string& id, const std::string& h, int p, 
                int mem, int slots, int workers, bool local = true)
        : agent_id(id), host(h), port(p), memory_mb(mem), 
          num_slots(slots), workers_per_slot(workers), is_local_mode(local) {
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
    
    TaskSpec() = default;
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
    
    AgentStatus() : running(false), active_tasks(0), available_slots(0),
                   total_morsels_processed(0), total_bytes_shuffled(0),
                   marbledb_initialized(false) {}
};

//! C++ Agent - Headless Worker Node
//!
//! Architecture:
//! - Embedded MarbleDB for streaming SQL state
//! - TaskSlotManager for morsel parallelism
//! - ShuffleTransport for network communication
//! - Direct C++ execution (no Python overhead)
//!
//! Key features:
//! - High-performance morsel processing
//! - Embedded MarbleDB with RAFT replication
//! - Lock-free network transport
//! - Dynamic task deployment
//! - Zero Python dependencies
class Agent {
public:
    //! Constructor
    explicit Agent(const AgentConfig& config);
    
    //! Destructor
    ~Agent();
    
    //! Start agent services
    arrow::Status Start();
    
    //! Stop agent services
    arrow::Status Stop();
    
    //! Deploy and start a task
    arrow::Status DeployTask(const TaskSpec& task);
    
    //! Stop a task
    arrow::Status StopTask(const std::string& task_id);
    
    //! Get agent status
    AgentStatus GetStatus() const;
    
    //! Get embedded MarbleDB instance
    std::shared_ptr<MarbleDBIntegration> GetMarbleDB() const;
    
    //! Get task slot manager
    std::shared_ptr<TaskSlotManager> GetTaskSlotManager() const;
    
    //! Get shuffle transport
    std::shared_ptr<ShuffleTransport> GetShuffleTransport() const;
    
    //! Check if agent is running
    bool IsRunning() const { return running_.load(std::memory_order_relaxed); }
    
    //! Get agent ID
    const std::string& GetAgentId() const { return config_.agent_id; }

private:
    //! Configuration
    AgentConfig config_;
    
    //! Running state
    std::atomic<bool> running_;
    
    //! Core components
    std::shared_ptr<TaskSlotManager> task_slot_manager_;
    std::shared_ptr<ShuffleTransport> shuffle_transport_;
    std::shared_ptr<MarbleDBIntegration> marbledb_;
    
    //! Active tasks
    std::unordered_map<std::string, std::shared_ptr<TaskExecutor>> active_tasks_;
    mutable std::mutex tasks_mutex_;
    
    //! Statistics
    mutable std::atomic<int64_t> total_morsels_processed_;
    mutable std::atomic<int64_t> total_bytes_shuffled_;
    
    //! Background threads
    std::thread heartbeat_thread_;
    std::atomic<bool> shutdown_requested_;
    
    //! Initialize embedded MarbleDB
    arrow::Status InitializeMarbleDB();
    
    //! Shutdown embedded MarbleDB
    arrow::Status ShutdownMarbleDB();
    
    //! Initialize task slot manager
    arrow::Status InitializeTaskSlotManager();
    
    //! Initialize shuffle transport
    arrow::Status InitializeShuffleTransport();
    
    //! Start HTTP server for task deployment (distributed mode)
    arrow::Status StartHTTPServer();
    
    //! Stop HTTP server
    arrow::Status StopHTTPServer();
    
    //! Register with job manager (distributed mode)
    arrow::Status RegisterWithJobManager();
    
    //! Heartbeat loop
    void HeartbeatLoop();
    
    //! Allocate slot for task
    int AllocateSlot();
    
    //! Release slot
    void ReleaseSlot(int slot_id);
};

//! Task Executor - C++ implementation
//!
//! Executes a single task using embedded MarbleDB and morsel parallelism.
class TaskExecutor {
public:
    TaskExecutor(const TaskSpec& task, int slot_id, 
                 std::shared_ptr<TaskSlotManager> slot_manager,
                 std::shared_ptr<ShuffleTransport> shuffle_transport,
                 std::shared_ptr<MarbleDBIntegration> marbledb);
    
    ~TaskExecutor();
    
    //! Start task execution
    arrow::Status Start();
    
    //! Stop task execution
    arrow::Status Stop();
    
    //! Check if running
    bool IsRunning() const { return running_.load(std::memory_order_relaxed); }
    
    //! Get task ID
    const std::string& GetTaskId() const { return task_.task_id; }

private:
    //! Task specification
    TaskSpec task_;
    
    //! Slot ID
    int slot_id_;
    
    //! Components
    std::shared_ptr<TaskSlotManager> slot_manager_;
    std::shared_ptr<ShuffleTransport> shuffle_transport_;
    std::shared_ptr<MarbleDBIntegration> marbledb_;
    
    //! Running state
    std::atomic<bool> running_;
    
    //! Execution thread
    std::thread execution_thread_;
    
    //! Execution loop
    void ExecutionLoop();
    
    //! Process batch
    std::shared_ptr<arrow::RecordBatch> ProcessBatch(
        std::shared_ptr<arrow::RecordBatch> batch);
    
    //! Send result downstream
    arrow::Status SendDownstream(std::shared_ptr<arrow::RecordBatch> batch);
};

} // namespace sabot
