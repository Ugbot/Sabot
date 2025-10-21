#pragma once

#include <arrow/api.h>
#include <string>
#include <vector>
#include <memory>
#include <unordered_map>
#include <functional>
#include "sabot_sql/streaming/morsel_plan_extensions.h"

// Forward declarations
namespace sabot_sql {
namespace streaming {
class SabotOrchestratorClient;
}
}

namespace sabot_sql {
namespace streaming {

// Forward declarations
class StreamingMorselPlan;
class MarbleDBIntegration;
class CheckpointCoordinator;

/**
 * SabotExecutionCoordinator - Integrates streaming SQL with Sabot's distributed execution framework
 * 
 * This class bridges the gap between streaming SQL plans and Sabot's execution infrastructure:
 * 1. Converts StreamingMorselPlan to Sabot JobGraph
 * 2. Manages agent provisioning and task distribution
 * 3. Coordinates checkpoint barriers across streaming operators
 * 4. Handles result collection and aggregation
 * 5. Integrates with MarbleDB for state management
 */
class SabotExecutionCoordinator {
public:
    struct ExecutionConfig {
        std::string orchestrator_host = "localhost";
        int orchestrator_port = 8080;
        int max_parallelism = 8;
        std::string checkpoint_interval = "60s";
        std::string state_backend = "marbledb";
        std::string timer_backend = "marbledb";
        bool enable_checkpointing = true;
        bool enable_watermarks = true;
        int64_t watermark_idle_timeout_ms = 30000;
    };

    struct AgentConfig {
        std::string agent_id;
        std::string host;
        int port;
        int available_slots;
        int cpu_percent;
        int memory_percent;
        std::string status;
        int64_t last_heartbeat;
    };

    struct AgentAssignment {
        std::string agent_id;
        std::string host;
        int port;
        std::vector<std::string> assigned_tasks;
        int available_slots;
    };

    struct ExecutionResult {
        std::string execution_id;
        bool success;
        std::string error_message;
        int64_t total_rows_processed;
        int64_t execution_time_ms;
        std::vector<std::string> completed_tasks;
        std::unordered_map<std::string, int64_t> task_metrics;
    };

public:
    SabotExecutionCoordinator();
    ~SabotExecutionCoordinator();

    // Lifecycle
    arrow::Status Initialize(const ExecutionConfig& config);
    arrow::Status Shutdown();

    // Execution coordination
    arrow::Result<std::string> SubmitStreamingJob(
        const StreamingMorselPlan& plan,
        const std::string& job_name = ""
    );
    
    arrow::Result<ExecutionResult> WaitForCompletion(
        const std::string& execution_id,
        int64_t timeout_ms = 300000  // 5 minutes default
    );

    arrow::Status CancelExecution(const std::string& execution_id);

    // Agent management
    arrow::Result<std::vector<AgentAssignment>> GetAvailableAgents();
    arrow::Status RegisterAgent(const AgentConfig& agent);
    arrow::Status ProvisionAgents(const std::vector<std::string>& agent_ids);
    arrow::Status ReleaseAgents(const std::vector<std::string>& agent_ids);

    // Checkpoint coordination
    arrow::Result<int64_t> TriggerCheckpoint(const std::string& execution_id);
    arrow::Status WaitForCheckpointCompletion(
        const std::string& execution_id,
        int64_t checkpoint_id,
        int64_t timeout_ms = 60000
    );

    // Result collection
    arrow::Result<std::shared_ptr<arrow::Table>> CollectResults(
        const std::string& execution_id,
        const std::string& output_operator_id
    );

    // Monitoring
    arrow::Result<std::unordered_map<std::string, std::string>> GetExecutionStatus(
        const std::string& execution_id
    );
    
    arrow::Result<std::vector<std::string>> GetActiveExecutions();

private:
    // Internal state
    ExecutionConfig config_;
    std::shared_ptr<MarbleDBIntegration> marbledb_;
    std::shared_ptr<CheckpointCoordinator> checkpoint_coordinator_;
    
    // Execution tracking
    std::unordered_map<std::string, std::string> execution_to_job_id_;
    std::unordered_map<std::string, StreamingMorselPlan> active_plans_;
    std::unordered_map<std::string, std::vector<AgentAssignment>> execution_agents_;
    
    // Agent management
    std::unordered_map<std::string, AgentAssignment> available_agents_;
    std::unordered_map<std::string, std::string> agent_to_execution_;
    
    // Result collection
    std::unordered_map<std::string, std::vector<std::shared_ptr<arrow::RecordBatch>>> execution_results_;
    
    // Sabot orchestrator integration
    std::shared_ptr<sabot_sql::streaming::SabotOrchestratorClient> orchestrator_client_;
    
    // Execution state
    std::unordered_map<std::string, std::string> execution_status_;
    std::unordered_map<std::string, int64_t> execution_start_time_;
    
    // Internal methods
    arrow::Status InitializeOrchestratorClient();
    arrow::Result<std::string> CreateJobGraphFromPlan(const StreamingMorselPlan& plan);
    arrow::Result<std::string> CreateJobGraph(const StreamingMorselPlan& plan);
    arrow::Result<std::vector<AgentAssignment>> AssignTasksToAgents(
        const StreamingMorselPlan& plan,
        const std::vector<AgentAssignment>& agents
    );
    
    arrow::Status DeployTasksToAgents(
        const std::string& execution_id,
        const std::vector<AgentAssignment>& assignments
    );
    
    arrow::Status StartExecution(const std::string& execution_id);
    arrow::Status StopExecution(const std::string& execution_id);
    
    // Checkpoint integration
    arrow::Status RegisterCheckpointParticipants(const std::string& execution_id);
    arrow::Status InjectCheckpointBarriers(const std::string& execution_id, int64_t checkpoint_id);
    
    // Result aggregation
    arrow::Result<std::shared_ptr<arrow::Table>> AggregateResults(
        const std::string& execution_id,
        const std::string& output_operator_id
    );
    
    // Monitoring
    arrow::Status UpdateExecutionMetrics(const std::string& execution_id);
    arrow::Status CleanupExecution(const std::string& execution_id);
};

/**
 * StreamingJobGraph - Represents a streaming job in Sabot's execution framework
 * 
 * Converts StreamingMorselPlan to Sabot's JobGraph format for distributed execution.
 * Handles operator placement, parallelism, and state management.
 */
class StreamingJobGraph {
public:
    struct OperatorNode {
        std::string operator_id;
        std::string operator_type;
        std::unordered_map<std::string, std::string> parameters;
        std::vector<std::string> input_operators;
        std::vector<std::string> output_operators;
        bool is_stateful;
        bool requires_shuffle;
        int parallelism;
        std::string state_backend;
    };

    struct TaskVertex {
        std::string task_id;
        std::string operator_id;
        int task_index;
        std::string assigned_agent_id;
        std::unordered_map<std::string, std::string> parameters;
    };

public:
    StreamingJobGraph();
    ~StreamingJobGraph();

    // Graph construction
    arrow::Status BuildFromPlan(const StreamingMorselPlan& plan);
    arrow::Status AddOperator(const OperatorNode& operator_node);
    arrow::Status AddTask(const TaskVertex& task);
    
    // Graph properties
    std::string GetJobId() const { return job_id_; }
    std::string GetJobName() const { return job_name_; }
    int GetTaskCount() const { return tasks_.size(); }
    int GetOperatorCount() const { return operators_.size(); }
    
    // Graph traversal
    std::vector<std::string> GetSourceTasks() const;
    std::vector<std::string> GetSinkTasks() const;
    std::vector<std::string> GetTasksForOperator(const std::string& operator_id) const;
    
    // Serialization
    arrow::Result<std::string> ToJson() const;
    arrow::Status FromJson(const std::string& json);

private:
    std::string job_id_;
    std::string job_name_;
    std::unordered_map<std::string, OperatorNode> operators_;
    std::unordered_map<std::string, TaskVertex> tasks_;
    std::unordered_map<std::string, std::vector<std::string>> operator_to_tasks_;
    
    // Internal methods
    arrow::Status ValidateGraph();
    arrow::Status ExpandParallelism(const StreamingMorselPlan& plan);
    arrow::Status CreateTasksForOperator(const OperatorNode& operator_node);
    arrow::Status ConnectTasks();
};

} // namespace streaming
} // namespace sabot_sql
