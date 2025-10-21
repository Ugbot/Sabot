//===----------------------------------------------------------------------===//
//                         Sabot
//
// sabot/_c/local_executor.cpp
//
// Local Executor Implementation - Automatic C++ Agent Management
//
// When running locally, automatically creates and manages a C++ agent
// behind the scenes for high-performance execution.
//
//===----------------------------------------------------------------------===//

#include "local_executor.hpp"
#include "agent.hpp"
#include "task_slot_manager.hpp"
#include <arrow/status.h>
#include <arrow/result.h>
#include <arrow/table.h>
#include <arrow/record_batch.h>
#include <iostream>
#include <thread>
#include <chrono>
#include <mutex>
#include <atomic>
#include <sstream>

namespace sabot {

//! LocalExecutor Constructor
LocalExecutor::LocalExecutor(const LocalExecutorConfig& config) 
    : config_(config), initialized_(false) {
    
    std::cout << "LocalExecutor created: " << config_.executor_id << std::endl;
}

//! LocalExecutor Destructor
LocalExecutor::~LocalExecutor() {
    if (initialized_.load(std::memory_order_relaxed)) {
        Shutdown();
    }
}

//! Initialize local executor
arrow::Status LocalExecutor::Initialize() {
    if (initialized_.load(std::memory_order_relaxed)) {
        return arrow::Status::OK();
    }
    
    std::lock_guard<std::mutex> lock(mutex_);
    
    // Initialize C++ agent
    ARROW_RETURN_NOT_OK(InitializeAgent());
    
    initialized_.store(true, std::memory_order_release);
    
    std::cout << "LocalExecutor initialized: " << config_.executor_id << std::endl;
    
    return arrow::Status::OK();
}

//! Shutdown local executor
arrow::Status LocalExecutor::Shutdown() {
    if (!initialized_.load(std::memory_order_relaxed)) {
        return arrow::Status::OK();
    }
    
    std::lock_guard<std::mutex> lock(mutex_);
    
    // Shutdown C++ agent
    ARROW_RETURN_NOT_OK(ShutdownAgent());
    
    // Clear registered tables and sources
    dimension_tables_.clear();
    streaming_sources_.clear();
    
    initialized_.store(false, std::memory_order_release);
    
    std::cout << "LocalExecutor shutdown: " << config_.executor_id << std::endl;
    
    return arrow::Status::OK();
}

//! Execute a streaming SQL query locally
arrow::Status LocalExecutor::ExecuteStreamingSQL(
    const std::string& query,
    const std::unordered_map<std::string, std::shared_ptr<arrow::Table>>& input_tables,
    std::function<void(std::shared_ptr<arrow::RecordBatch>)> output_callback) {
    
    if (!initialized_.load(std::memory_order_relaxed)) {
        return arrow::Status::Invalid("LocalExecutor not initialized");
    }
    
    std::lock_guard<std::mutex> lock(mutex_);
    
    try {
        // Parse SQL query
        ARROW_ASSIGN_OR_RAISE(auto operator_type, ParseSQLQuery(query));
        
        // Create morsels from input tables
        auto morsels = CreateMorsels(input_tables);
        
        // Process morsels using task slot manager
        auto results = ProcessMorsels(morsels, operator_type);
        
        // Send results to callback
        for (const auto& result : results) {
            if (result) {
                output_callback(result);
            }
        }
        
        return arrow::Status::OK();
        
    } catch (const std::exception& e) {
        return arrow::Status::Invalid("Streaming SQL execution failed: " + std::string(e.what()));
    }
}

//! Execute a batch SQL query locally
arrow::Result<std::shared_ptr<arrow::Table>> LocalExecutor::ExecuteBatchSQL(
    const std::string& query,
    const std::unordered_map<std::string, std::shared_ptr<arrow::Table>>& input_tables) {
    
    if (!initialized_.load(std::memory_order_relaxed)) {
        return arrow::Status::Invalid("LocalExecutor not initialized");
    }
    
    std::lock_guard<std::mutex> lock(mutex_);
    
    try {
        // Parse SQL query
        ARROW_ASSIGN_OR_RAISE(auto operator_type, ParseSQLQuery(query));
        
        // Create morsels from input tables
        auto morsels = CreateMorsels(input_tables);
        
        // Process morsels using task slot manager
        auto results = ProcessMorsels(morsels, operator_type);
        
        // Combine results into final table
        return CombineResults(results);
        
    } catch (const std::exception& e) {
        return arrow::Status::Invalid("Batch SQL execution failed: " + std::string(e.what()));
    }
}

//! Register a dimension table
arrow::Status LocalExecutor::RegisterDimensionTable(
    const std::string& table_name,
    std::shared_ptr<arrow::Table> table,
    bool is_raft_replicated) {
    
    if (!initialized_.load(std::memory_order_relaxed)) {
        return arrow::Status::Invalid("LocalExecutor not initialized");
    }
    
    std::lock_guard<std::mutex> lock(mutex_);
    
    // Store table
    dimension_tables_[table_name] = table;
    
    // Store in embedded MarbleDB if available
    if (marbledb_) {
        // In real implementation, this would call marbledb_->CreateTable()
        std::cout << "Registered dimension table: " << table_name 
                  << " (RAFT=" << is_raft_replicated << ")" << std::endl;
    }
    
    return arrow::Status::OK();
}

//! Register a streaming source
arrow::Status LocalExecutor::RegisterStreamingSource(
    const std::string& source_name,
    const std::string& connector_type,
    const std::unordered_map<std::string, std::string>& config) {
    
    if (!initialized_.load(std::memory_order_relaxed)) {
        return arrow::Status::Invalid("LocalExecutor not initialized");
    }
    
    std::lock_guard<std::mutex> lock(mutex_);
    
    // Store source configuration
    streaming_sources_[source_name] = config;
    streaming_sources_[source_name]["connector_type"] = connector_type;
    
    std::cout << "Registered streaming source: " << source_name 
              << " (type=" << connector_type << ")" << std::endl;
    
    return arrow::Status::OK();
}

//! Get embedded MarbleDB instance
std::shared_ptr<MarbleDBIntegration> LocalExecutor::GetMarbleDB() const {
    return marbledb_;
}

//! Get task slot manager
std::shared_ptr<TaskSlotManager> LocalExecutor::GetTaskSlotManager() const {
    return task_slot_manager_;
}

//! Initialize C++ agent
arrow::Status LocalExecutor::InitializeAgent() {
    try {
        // Create agent configuration
        AgentConfig agent_config(
            config_.executor_id,
            "localhost",
            8819,  // Use different port for local executor
            config_.memory_mb,
            config_.num_slots,
            config_.workers_per_slot,
            true   // is_local_mode
        );
        
        // Create C++ agent
        agent_ = std::make_unique<Agent>(agent_config);
        
        // Start agent
        ARROW_RETURN_NOT_OK(agent_->Start());
        
        // Get components from agent
        marbledb_ = agent_->GetMarbleDB();
        task_slot_manager_ = agent_->GetTaskSlotManager();
        shuffle_transport_ = agent_->GetShuffleTransport();
        
        std::cout << "C++ agent initialized for local execution" << std::endl;
        
        return arrow::Status::OK();
        
    } catch (const std::exception& e) {
        return arrow::Status::Invalid("Failed to initialize C++ agent: " + std::string(e.what()));
    }
}

//! Shutdown C++ agent
arrow::Status LocalExecutor::ShutdownAgent() {
    if (agent_) {
        try {
            ARROW_RETURN_NOT_OK(agent_->Stop());
            agent_.reset();
            std::cout << "C++ agent shutdown for local execution" << std::endl;
        } catch (const std::exception& e) {
            return arrow::Status::Invalid("Failed to shutdown C++ agent: " + std::string(e.what()));
        }
    }
    
    return arrow::Status::OK();
}

//! Parse SQL query and create execution plan
arrow::Result<std::string> LocalExecutor::ParseSQLQuery(const std::string& query) {
    // In real implementation, this would parse the SQL query and determine
    // the appropriate operator type (e.g., "window_aggregate", "join", "filter")
    
    // For now, return a simple operator type based on query content
    if (query.find("SELECT") != std::string::npos) {
        if (query.find("GROUP BY") != std::string::npos) {
            return "window_aggregate";
        } else if (query.find("JOIN") != std::string::npos) {
            return "join";
        } else {
            return "filter";
        }
    }
    
    return arrow::Status::Invalid("Unsupported SQL query: " + query);
}

//! Create morsels from input tables
std::vector<std::shared_ptr<arrow::RecordBatch>> LocalExecutor::CreateMorsels(
    const std::unordered_map<std::string, std::shared_ptr<arrow::Table>>& input_tables) {
    
    std::vector<std::shared_ptr<arrow::RecordBatch>> morsels;
    
    for (const auto& [table_name, table] : input_tables) {
        if (table) {
            // Convert table to record batches (morsels)
            auto batches = table->ToRecordBatches();
            for (const auto& batch : batches.ValueOrDie()) {
                morsels.push_back(batch);
            }
        }
    }
    
    return morsels;
}

//! Process morsels using task slot manager
std::vector<std::shared_ptr<arrow::RecordBatch>> LocalExecutor::ProcessMorsels(
    const std::vector<std::shared_ptr<arrow::RecordBatch>>& morsels,
    const std::string& operator_type) {
    
    std::vector<std::shared_ptr<arrow::RecordBatch>> results;
    
    if (!task_slot_manager_) {
        // Fallback to simple processing
        for (const auto& morsel : morsels) {
            results.push_back(morsel);
        }
        return results;
    }
    
    // In real implementation, this would use the task slot manager
    // to process morsels in parallel using the appropriate operator
    
    // For now, just return the input morsels
    return morsels;
}

//! Combine results into final table
std::shared_ptr<arrow::Table> LocalExecutor::CombineResults(
    const std::vector<std::shared_ptr<arrow::RecordBatch>>& results) {
    
    if (results.empty()) {
        return nullptr;
    }
    
    // In real implementation, this would combine the record batches
    // into a single table using Arrow's table concatenation
    
    // For now, just return the first result as a table
    if (results[0]) {
        return arrow::Table::FromRecordBatches({results[0]}).ValueOrDie();
    }
    
    return nullptr;
}

//! LocalExecutorManager Implementation

//! Get singleton instance
LocalExecutorManager& LocalExecutorManager::GetInstance() {
    static LocalExecutorManager instance;
    return instance;
}

//! Get or create local executor
std::shared_ptr<LocalExecutor> LocalExecutorManager::GetOrCreateExecutor(const std::string& executor_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    auto it = executors_.find(executor_id);
    if (it != executors_.end()) {
        return it->second;
    }
    
    // Create new executor
    LocalExecutorConfig config(executor_id);
    auto executor = std::make_shared<LocalExecutor>(config);
    
    // Initialize executor
    auto status = executor->Initialize();
    if (!status.ok()) {
        std::cerr << "Failed to initialize local executor: " << status.message() << std::endl;
        return nullptr;
    }
    
    executors_[executor_id] = executor;
    return executor;
}

//! Shutdown all executors
void LocalExecutorManager::ShutdownAll() {
    std::lock_guard<std::mutex> lock(mutex_);
    
    for (auto& [executor_id, executor] : executors_) {
        executor->Shutdown();
    }
    
    executors_.clear();
}

//! Destructor
LocalExecutorManager::~LocalExecutorManager() {
    ShutdownAll();
}

} // namespace sabot
