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
#include "sabot_sql/streaming/morsel_plan_extensions.h"

namespace sabot_sql {
namespace streaming {

// Forward declarations
class StreamingMorselPlan;
class MarbleDBIntegration;
class CheckpointBarrierInjector;

/**
 * StreamingAgentDistributor - Manages multi-partition parallelism for streaming execution
 * 
 * This class handles the distribution of streaming tasks across multiple agents:
 * 1. Analyzes streaming plan to identify partitionable sources
 * 2. Assigns Kafka partitions to available agents
 * 3. Manages agent lifecycle and task deployment
 * 4. Coordinates partition rebalancing on agent failures
 * 5. Handles result collection from distributed agents
 */
class StreamingAgentDistributor {
public:
    struct AgentConfig {
        std::string agent_id;
        std::string host;
        int port;
        int available_slots;
        int cpu_percent;
        int memory_percent;
        std::string status;  // "alive", "busy", "failed"
        int64_t last_heartbeat;
        std::vector<std::string> assigned_tasks;
    };

    struct PartitionAssignment {
        std::string partition_id;
        std::string source_type;  // "kafka", "file", etc.
        std::string assigned_agent_id;
        std::unordered_map<std::string, std::string> source_config;
        int64_t assigned_timestamp;
        bool is_active;
    };

    struct DistributionPlan {
        std::string execution_id;
        std::string plan_id;
        int total_partitions;
        int assigned_agents;
        std::unordered_map<std::string, PartitionAssignment> partition_assignments;
        std::unordered_map<std::string, std::vector<std::string>> agent_partitions;
        int64_t created_timestamp;
        int64_t last_updated;
    };

    struct ExecutionMetrics {
        std::string execution_id;
        int64_t total_rows_processed;
        int64_t total_bytes_processed;
        int64_t execution_time_ms;
        std::unordered_map<std::string, int64_t> agent_metrics;
        std::unordered_map<std::string, int64_t> partition_metrics;
        int64_t last_updated;
    };

public:
    StreamingAgentDistributor();
    ~StreamingAgentDistributor();

    // Lifecycle
    arrow::Status Initialize(
        std::shared_ptr<MarbleDBIntegration> marbledb,
        std::shared_ptr<CheckpointBarrierInjector> barrier_injector
    );
    arrow::Status Shutdown();

    // Agent management
    arrow::Status RegisterAgent(const AgentConfig& config);
    arrow::Status UnregisterAgent(const std::string& agent_id);
    arrow::Status UpdateAgentStatus(const std::string& agent_id, const std::string& status);
    arrow::Result<std::vector<AgentConfig>> GetAvailableAgents();
    arrow::Result<AgentConfig> GetAgent(const std::string& agent_id);

    // Distribution planning
    arrow::Result<DistributionPlan> CreateDistributionPlan(
        const StreamingMorselPlan& plan,
        const std::string& execution_id
    );
    arrow::Status UpdateDistributionPlan(const DistributionPlan& plan);
    arrow::Result<DistributionPlan> GetDistributionPlan(const std::string& execution_id);

    // Task deployment
    arrow::Status DeployTasksToAgents(const DistributionPlan& plan);
    arrow::Status StartExecution(const std::string& execution_id);
    arrow::Status StopExecution(const std::string& execution_id);
    arrow::Status CancelExecution(const std::string& execution_id);

    // Partition management
    arrow::Status AssignPartitionToAgent(
        const std::string& execution_id,
        const std::string& partition_id,
        const std::string& agent_id
    );
    arrow::Status ReassignPartition(
        const std::string& execution_id,
        const std::string& partition_id,
        const std::string& new_agent_id
    );
    arrow::Status RebalancePartitions(const std::string& execution_id);

    // Result collection
    arrow::Result<std::shared_ptr<arrow::Table>> CollectResults(
        const std::string& execution_id,
        const std::string& output_operator_id
    );
    arrow::Status AggregateResults(
        const std::string& execution_id,
        const std::string& operator_id,
        const std::shared_ptr<arrow::RecordBatch>& batch
    );

    // Monitoring
    arrow::Result<ExecutionMetrics> GetExecutionMetrics(const std::string& execution_id);
    arrow::Status UpdateExecutionMetrics(const std::string& execution_id, const ExecutionMetrics& metrics);
    arrow::Result<std::vector<std::string>> GetActiveExecutions();

    // Failure handling
    arrow::Status HandleAgentFailure(const std::string& agent_id);
    arrow::Status RecoverFromAgentFailure(const std::string& execution_id, const std::string& failed_agent_id);
    arrow::Status ReassignFailedTasks(const std::string& execution_id, const std::string& failed_agent_id);

private:
    // Internal state
    std::shared_ptr<MarbleDBIntegration> marbledb_;
    std::shared_ptr<CheckpointBarrierInjector> barrier_injector_;
    
    // Agent management
    std::unordered_map<std::string, AgentConfig> agents_;
    std::mutex agents_mutex_;
    
    // Distribution plans
    std::unordered_map<std::string, DistributionPlan> distribution_plans_;
    std::mutex plans_mutex_;
    
    // Execution metrics
    std::unordered_map<std::string, ExecutionMetrics> execution_metrics_;
    std::mutex metrics_mutex_;
    
    // Result collection
    std::unordered_map<std::string, std::vector<std::shared_ptr<arrow::RecordBatch>>> execution_results_;
    std::mutex results_mutex_;
    
    // Background thread
    std::atomic<bool> running_;
    std::thread monitoring_thread_;
    std::condition_variable monitoring_cv_;
    
    // Internal methods
    void MonitoringThreadLoop();
    arrow::Status MonitorAgentHealth();
    arrow::Status MonitorExecutionProgress();
    arrow::Status CleanupCompletedExecutions();
    
    // Distribution algorithms
    arrow::Result<std::string> SelectBestAgent(const std::vector<AgentConfig>& available_agents);
    arrow::Status BalancePartitionLoad(const std::string& execution_id);
    arrow::Status OptimizePartitionPlacement(const std::string& execution_id);
    
    // Task deployment
    arrow::Status DeployTaskToAgent(
        const std::string& agent_id,
        const std::string& task_id,
        const std::unordered_map<std::string, std::string>& task_config
    );
    arrow::Status StartTaskOnAgent(const std::string& agent_id, const std::string& task_id);
    arrow::Status StopTaskOnAgent(const std::string& agent_id, const std::string& task_id);
    
    // Result aggregation
    arrow::Result<std::shared_ptr<arrow::Table>> AggregateBatches(
        const std::vector<std::shared_ptr<arrow::RecordBatch>>& batches
    );
    arrow::Status MergeExecutionResults(const std::string& execution_id);
    
    // Persistence
    arrow::Status PersistDistributionPlan(const DistributionPlan& plan);
    arrow::Status LoadDistributionPlan(const std::string& execution_id);
    arrow::Status PersistExecutionMetrics(const ExecutionMetrics& metrics);
    arrow::Status LoadExecutionMetrics(const std::string& execution_id);
};

/**
 * PartitionRebalancer - Handles dynamic partition rebalancing
 * 
 * This class manages the rebalancing of partitions when agents fail or new agents join:
 * 1. Detects agent failures and partition unavailability
 * 2. Calculates optimal partition reassignment
 * 3. Coordinates partition migration between agents
 * 4. Ensures exactly-once semantics during rebalancing
 */
class PartitionRebalancer {
public:
    struct RebalancePlan {
        std::string execution_id;
        std::vector<std::string> failed_agents;
        std::vector<std::string> available_agents;
        std::unordered_map<std::string, std::string> partition_reassignments;
        int64_t created_timestamp;
        bool is_executed;
    };

    struct MigrationStep {
        std::string partition_id;
        std::string from_agent_id;
        std::string to_agent_id;
        int64_t migration_timestamp;
        bool is_completed;
        std::string error_message;
    };

public:
    PartitionRebalancer();
    ~PartitionRebalancer();

    // Lifecycle
    arrow::Status Initialize(std::shared_ptr<MarbleDBIntegration> marbledb);
    arrow::Status Shutdown();

    // Rebalancing
    arrow::Result<RebalancePlan> CreateRebalancePlan(
        const std::string& execution_id,
        const std::vector<std::string>& failed_agents,
        const std::vector<std::string>& available_agents
    );
    arrow::Status ExecuteRebalancePlan(const RebalancePlan& plan);
    arrow::Status CancelRebalancePlan(const std::string& execution_id);

    // Migration
    arrow::Status MigratePartition(
        const std::string& partition_id,
        const std::string& from_agent_id,
        const std::string& to_agent_id
    );
    arrow::Status WaitForMigrationCompletion(
        const std::string& partition_id,
        int64_t timeout_ms = 30000
    );

    // Status tracking
    arrow::Result<RebalancePlan> GetRebalancePlan(const std::string& execution_id);
    arrow::Result<std::vector<MigrationStep>> GetMigrationSteps(const std::string& execution_id);
    arrow::Result<bool> IsRebalancingComplete(const std::string& execution_id);

private:
    std::shared_ptr<MarbleDBIntegration> marbledb_;
    
    // Rebalancing state
    std::unordered_map<std::string, RebalancePlan> active_rebalances_;
    std::unordered_map<std::string, std::vector<MigrationStep>> migration_steps_;
    std::mutex rebalance_mutex_;
    
    // Internal methods
    arrow::Status CalculateOptimalAssignment(
        const std::vector<std::string>& partitions,
        const std::vector<std::string>& available_agents,
        std::unordered_map<std::string, std::string>& assignments
    );
    arrow::Status ValidateRebalancePlan(const RebalancePlan& plan);
    arrow::Status PersistRebalancePlan(const RebalancePlan& plan);
    arrow::Status LoadRebalancePlan(const std::string& execution_id);
};

} // namespace streaming
} // namespace sabot_sql
