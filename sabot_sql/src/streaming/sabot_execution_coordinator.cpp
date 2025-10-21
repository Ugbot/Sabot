#include "sabot_sql/streaming/sabot_execution_coordinator.h"
#include "sabot_sql/streaming/marbledb_integration.h"
#include "sabot_sql/streaming/checkpoint_coordinator.h"
#include "sabot_sql/streaming/sabot_orchestrator_client.h"
#include <arrow/table.h>
#include <arrow/array.h>
#include <arrow/builder.h>
#include <arrow/ipc/writer.h>
#include <arrow/ipc/reader.h>
#include <nlohmann/json.hpp>
#include <sstream>
#include <chrono>
#include <thread>
#include <random>

namespace sabot_sql {
namespace streaming {

// ============================================================================
// SabotExecutionCoordinator Implementation
// ============================================================================

SabotExecutionCoordinator::SabotExecutionCoordinator() {
    // Initialize with default config
    config_.orchestrator_host = "localhost";
    config_.orchestrator_port = 8080;
    config_.max_parallelism = 8;
    config_.checkpoint_interval = "60s";
    config_.state_backend = "marbledb";
    config_.timer_backend = "marbledb";
    config_.enable_checkpointing = true;
    config_.enable_watermarks = true;
    config_.watermark_idle_timeout_ms = 30000;
}

SabotExecutionCoordinator::~SabotExecutionCoordinator() {
    // Cleanup will be handled by Shutdown()
}

arrow::Status SabotExecutionCoordinator::Initialize(const ExecutionConfig& config) {
    config_ = config;
    
        // Initialize embedded MarbleDB integration
        marbledb_ = std::make_shared<MarbleDBIntegration>();
        ARROW_RETURN_NOT_OK(marbledb_->Initialize("sabot_streaming", "./sabot_streaming_marbledb", true));
    
    // Initialize checkpoint coordinator
    checkpoint_coordinator_ = std::make_shared<CheckpointCoordinator>();
    ARROW_RETURN_NOT_OK(checkpoint_coordinator_->Initialize(
        "sabot_streaming_coordinator",
        30000,  // 30 seconds
        nullptr
    ));
    
    // Initialize Sabot orchestrator client
    ARROW_RETURN_NOT_OK(InitializeOrchestratorClient());
    
    return arrow::Status::OK();
}

arrow::Status SabotExecutionCoordinator::Shutdown() {
    // Cancel all active executions
    for (const auto& [execution_id, job_id] : execution_to_job_id_) {
        ARROW_RETURN_NOT_OK(CancelExecution(execution_id));
    }
    
    // Cleanup MarbleDB
    if (marbledb_) {
        ARROW_RETURN_NOT_OK(marbledb_->Shutdown());
    }
    
    // Cleanup checkpoint coordinator
    if (checkpoint_coordinator_) {
        ARROW_RETURN_NOT_OK(checkpoint_coordinator_->Shutdown());
    }
    
    return arrow::Status::OK();
}

arrow::Status SabotExecutionCoordinator::InitializeOrchestratorClient() {
    orchestrator_client_ = std::make_shared<SabotOrchestratorClient>();
    
    std::unordered_map<std::string, std::string> client_config;
    client_config["connection_timeout_ms"] = "30000";
    client_config["max_retries"] = "3";
    client_config["retry_delay_ms"] = "1000";
    
    ARROW_RETURN_NOT_OK(orchestrator_client_->Initialize(
        config_.orchestrator_host,
        config_.orchestrator_port,
        client_config
    ));
    
    return arrow::Status::OK();
}

arrow::Result<std::string> SabotExecutionCoordinator::SubmitStreamingJob(
    const StreamingMorselPlan& plan,
    const std::string& job_name
) {
    // Generate execution ID
    std::string execution_id = "exec_" + std::to_string(
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()
        ).count()
    );
    
    // Store plan
    active_plans_[execution_id] = plan;
    
    // Create job graph from plan
    ARROW_ASSIGN_OR_RAISE(std::string job_graph_json, CreateJobGraphFromPlan(plan));
    
    // Submit job to Sabot orchestrator
    ARROW_ASSIGN_OR_RAISE(
        auto submission_result,
        orchestrator_client_->SubmitJob(job_graph_json, job_name)
    );
    
    if (!submission_result.success) {
        return arrow::Status::Invalid("Failed to submit job to orchestrator: " + submission_result.error_message);
    }
    
    execution_to_job_id_[execution_id] = submission_result.job_id;
    
    // Get available agents from orchestrator
    ARROW_ASSIGN_OR_RAISE(auto agent_infos, orchestrator_client_->GetAvailableAgents());
    
    // Convert to AgentAssignment format
    std::vector<AgentAssignment> assignments;
    for (const auto& agent_info : agent_infos) {
        AgentAssignment assignment;
        assignment.agent_id = agent_info.agent_id;
        assignment.host = agent_info.host;
        assignment.port = agent_info.port;
        assignment.available_slots = agent_info.available_slots;
        assignments.push_back(assignment);
    }
    
    execution_agents_[execution_id] = assignments;
    
    // Start execution
    ARROW_RETURN_NOT_OK(StartExecution(execution_id));
    
    return execution_id;
}

arrow::Result<SabotExecutionCoordinator::ExecutionResult> SabotExecutionCoordinator::WaitForCompletion(
    const std::string& execution_id,
    int64_t timeout_ms
) {
    ExecutionResult result;
    result.execution_id = execution_id;
    
    auto start_time = std::chrono::steady_clock::now();
    auto timeout_duration = std::chrono::milliseconds(timeout_ms);
    
    while (std::chrono::steady_clock::now() - start_time < timeout_duration) {
        // Check execution status
        auto exec_status_result = GetExecutionStatus(execution_id);
        if (!exec_status_result.ok()) {
            result.success = false;
            result.error_message = exec_status_result.status().message();
            break;
        }
        auto exec_status = exec_status_result.ValueOrDie();
        
        if (exec_status["status"] == "completed") {
            result.success = true;
            result.total_rows_processed = std::stoll(exec_status["total_rows"]);
            result.execution_time_ms = std::stoll(exec_status["execution_time_ms"]);
            for (const auto& assignment : execution_agents_[execution_id]) {
                result.completed_tasks.push_back(assignment.agent_id);
            }
            break;
        } else if (exec_status["status"] == "failed") {
            result.success = false;
            result.error_message = exec_status["error_message"];
            break;
        }
        
        // Wait a bit before checking again
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    
    if (result.success == false && result.error_message.empty()) {
        result.error_message = "Execution timed out";
    }
    
    return result;
}

arrow::Status SabotExecutionCoordinator::CancelExecution(const std::string& execution_id) {
    // Stop execution
    ARROW_RETURN_NOT_OK(StopExecution(execution_id));
    
    // Release agents
    if (execution_agents_.find(execution_id) != execution_agents_.end()) {
        std::vector<std::string> agent_ids;
        for (const auto& assignment : execution_agents_[execution_id]) {
            agent_ids.push_back(assignment.agent_id);
        }
        ARROW_RETURN_NOT_OK(ReleaseAgents(agent_ids));
    }
    
    // Cleanup execution
    ARROW_RETURN_NOT_OK(CleanupExecution(execution_id));
    
    return arrow::Status::OK();
}

arrow::Result<std::vector<SabotExecutionCoordinator::AgentAssignment>> SabotExecutionCoordinator::GetAvailableAgents() {
    if (!orchestrator_client_) {
        return arrow::Status::Invalid("Orchestrator client not initialized");
    }
    
    // Get available agents from orchestrator
    ARROW_ASSIGN_OR_RAISE(auto agent_infos, orchestrator_client_->GetAvailableAgents());
    
    // Convert to AgentAssignment format
    std::vector<AgentAssignment> assignments;
    for (const auto& agent_info : agent_infos) {
        AgentAssignment assignment;
        assignment.agent_id = agent_info.agent_id;
        assignment.host = agent_info.host;
        assignment.port = agent_info.port;
        assignment.available_slots = agent_info.available_slots;
        assignments.push_back(assignment);
    }
    
    return assignments;
}

arrow::Status SabotExecutionCoordinator::RegisterAgent(const AgentConfig& agent) {
    if (!orchestrator_client_) {
        return arrow::Status::Invalid("Orchestrator client not initialized");
    }
    
    // Convert to AgentInfo format for orchestrator
    SabotOrchestratorClient::AgentInfo agent_info;
    agent_info.agent_id = agent.agent_id;
    agent_info.host = agent.host;
    agent_info.port = agent.port;
    agent_info.available_slots = agent.available_slots;
    agent_info.cpu_percent = agent.cpu_percent;
    agent_info.memory_percent = agent.memory_percent;
    agent_info.status = agent.status;
    agent_info.last_heartbeat = agent.last_heartbeat;
    
    // Register with orchestrator
    ARROW_RETURN_NOT_OK(orchestrator_client_->RegisterAgent(agent_info));
    
    // Also update local state
    AgentAssignment assignment;
    assignment.agent_id = agent.agent_id;
    assignment.host = agent.host;
    assignment.port = agent.port;
    assignment.available_slots = agent.available_slots;
    
    available_agents_[agent.agent_id] = assignment;
    
    return arrow::Status::OK();
}

arrow::Status SabotExecutionCoordinator::ProvisionAgents(const std::vector<std::string>& agent_ids) {
    for (const auto& agent_id : agent_ids) {
        if (available_agents_.find(agent_id) != available_agents_.end()) {
            available_agents_[agent_id].available_slots = 4;  // Reset to full capacity
        }
    }
    return arrow::Status::OK();
}

arrow::Status SabotExecutionCoordinator::ReleaseAgents(const std::vector<std::string>& agent_ids) {
    for (const auto& agent_id : agent_ids) {
        if (available_agents_.find(agent_id) != available_agents_.end()) {
            available_agents_[agent_id].available_slots = 4;  // Release slots
            agent_to_execution_.erase(agent_id);
        }
    }
    return arrow::Status::OK();
}

arrow::Result<int64_t> SabotExecutionCoordinator::TriggerCheckpoint(const std::string& execution_id) {
    if (!config_.enable_checkpointing) {
        return arrow::Status::Invalid("Checkpointing is disabled");
    }
    
    // Register participants if not already done
    ARROW_RETURN_NOT_OK(RegisterCheckpointParticipants(execution_id));
    
    // Trigger checkpoint
    ARROW_ASSIGN_OR_RAISE(int64_t checkpoint_id, checkpoint_coordinator_->TriggerCheckpoint());
    
    // Inject barriers
    ARROW_RETURN_NOT_OK(InjectCheckpointBarriers(execution_id, checkpoint_id));
    
    return checkpoint_id;
}

arrow::Status SabotExecutionCoordinator::WaitForCheckpointCompletion(
    const std::string& execution_id,
    int64_t checkpoint_id,
    int64_t timeout_ms
) {
    auto start_time = std::chrono::steady_clock::now();
    auto timeout_duration = std::chrono::milliseconds(timeout_ms);
    
    while (std::chrono::steady_clock::now() - start_time < timeout_duration) {
        // Check checkpoint status
        ARROW_ASSIGN_OR_RAISE(
            bool is_completed,
            checkpoint_coordinator_->IsCheckpointCompleted(checkpoint_id)
        );
        
        if (is_completed) {
            return arrow::Status::OK();
        }
        
        // Wait a bit before checking again
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    
    return arrow::Status::Invalid("Checkpoint completion timed out");
}

arrow::Result<std::shared_ptr<arrow::Table>> SabotExecutionCoordinator::CollectResults(
    const std::string& execution_id,
    const std::string& output_operator_id
) {
    // Aggregate results from all agents
    ARROW_ASSIGN_OR_RAISE(
        std::shared_ptr<arrow::Table> results,
        AggregateResults(execution_id, output_operator_id)
    );
    
    return results;
}


arrow::Result<std::vector<std::string>> SabotExecutionCoordinator::GetActiveExecutions() {
    std::vector<std::string> active;
    for (const auto& [execution_id, plan] : active_plans_) {
        active.push_back(execution_id);
    }
    return active;
}

// ============================================================================
// Private Methods
// ============================================================================

arrow::Result<std::string> SabotExecutionCoordinator::CreateJobGraph(const StreamingMorselPlan& plan) {
    StreamingJobGraph job_graph;
    ARROW_RETURN_NOT_OK(job_graph.BuildFromPlan(plan));
    
    // Generate job ID
    std::string job_id = "job_" + std::to_string(
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()
        ).count()
    );
    
    return job_id;
}

arrow::Result<std::vector<SabotExecutionCoordinator::AgentAssignment>> SabotExecutionCoordinator::AssignTasksToAgents(
    const StreamingMorselPlan& plan,
    const std::vector<AgentAssignment>& agents
) {
    std::vector<AgentAssignment> assignments;
    
    // Simple round-robin assignment
    int agent_index = 0;
    for (const auto& operator_desc : plan.operators) {
        if (agents.empty()) {
            return arrow::Status::Invalid("No available agents");
        }
        
        AgentAssignment assignment = agents[agent_index % agents.size()];
        assignment.assigned_tasks.push_back(operator_desc.operator_id);
        assignments.push_back(assignment);
        
        agent_index++;
    }
    
    return assignments;
}

arrow::Status SabotExecutionCoordinator::DeployTasksToAgents(
    const std::string& execution_id,
    const std::vector<AgentAssignment>& assignments
) {
    // Mock deployment - in real implementation, this would send tasks to agents
    for (const auto& assignment : assignments) {
        agent_to_execution_[assignment.agent_id] = execution_id;
        available_agents_[assignment.agent_id].available_slots--;
    }
    
    return arrow::Status::OK();
}

arrow::Status SabotExecutionCoordinator::StartExecution(const std::string& execution_id) {
    // Mock start - in real implementation, this would start tasks on agents
    return arrow::Status::OK();
}

arrow::Status SabotExecutionCoordinator::StopExecution(const std::string& execution_id) {
    // Mock stop - in real implementation, this would stop tasks on agents
    return arrow::Status::OK();
}

arrow::Status SabotExecutionCoordinator::RegisterCheckpointParticipants(const std::string& execution_id) {
    // Register all agents as checkpoint participants
    for (const auto& assignment : execution_agents_[execution_id]) {
        ARROW_RETURN_NOT_OK(checkpoint_coordinator_->RegisterParticipant(
            assignment.agent_id,
            assignment.agent_id
        ));
    }
    
    return arrow::Status::OK();
}

arrow::Status SabotExecutionCoordinator::InjectCheckpointBarriers(
    const std::string& execution_id,
    int64_t checkpoint_id
) {
    // Mock barrier injection - in real implementation, this would send barriers to agents
    return arrow::Status::OK();
}

arrow::Result<std::shared_ptr<arrow::Table>> SabotExecutionCoordinator::AggregateResults(
    const std::string& execution_id,
    const std::string& output_operator_id
) {
    // Mock result aggregation - in real implementation, this would collect results from agents
    arrow::StringBuilder key_builder;
    arrow::Int64Builder count_builder;
    arrow::DoubleBuilder sum_builder;
    
    // Add some mock data
    ARROW_RETURN_NOT_OK(key_builder.Append("AAPL"));
    ARROW_RETURN_NOT_OK(count_builder.Append(1000));
    ARROW_RETURN_NOT_OK(sum_builder.Append(150000.0));
    
    ARROW_ASSIGN_OR_RAISE(auto key_array, key_builder.Finish());
    ARROW_ASSIGN_OR_RAISE(auto count_array, count_builder.Finish());
    ARROW_ASSIGN_OR_RAISE(auto sum_array, sum_builder.Finish());
    
    return arrow::Table::Make(
        arrow::schema({
            arrow::field("key", arrow::utf8()),
            arrow::field("count", arrow::int64()),
            arrow::field("sum", arrow::float64())
        }),
        {key_array, count_array, sum_array}
    );
}

arrow::Status SabotExecutionCoordinator::UpdateExecutionMetrics(const std::string& execution_id) {
    // Mock metrics update - in real implementation, this would collect metrics from agents
    return arrow::Status::OK();
}

arrow::Status SabotExecutionCoordinator::CleanupExecution(const std::string& execution_id) {
    // Cleanup execution state
    active_plans_.erase(execution_id);
    execution_to_job_id_.erase(execution_id);
    execution_agents_.erase(execution_id);
    execution_results_.erase(execution_id);
    
    return arrow::Status::OK();
}

// ============================================================================
// StreamingJobGraph Implementation
// ============================================================================

StreamingJobGraph::StreamingJobGraph() {
    // Generate job ID
    job_id_ = "job_" + std::to_string(
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()
        ).count()
    );
    job_name_ = "streaming_job";
}

StreamingJobGraph::~StreamingJobGraph() {
    // Cleanup
}

arrow::Status StreamingJobGraph::BuildFromPlan(const StreamingMorselPlan& plan) {
    // Clear existing graph
    operators_.clear();
    tasks_.clear();
    operator_to_tasks_.clear();
    
    // Add operators from plan
    for (const auto& operator_desc : plan.operators) {
        OperatorNode operator_node;
        operator_node.operator_id = operator_desc.operator_id;
        operator_node.operator_type = operator_desc.operator_type;
        operator_node.parameters = operator_desc.parameters;
        operator_node.is_stateful = operator_desc.is_stateful;
        operator_node.requires_shuffle = operator_desc.requires_shuffle;
        operator_node.parallelism = plan.max_parallelism;
        operator_node.state_backend = plan.state_backend;
        
        ARROW_RETURN_NOT_OK(AddOperator(operator_node));
    }
    
    // Expand parallelism and create tasks
    ARROW_RETURN_NOT_OK(ExpandParallelism(plan));
    
    // Connect tasks
    ARROW_RETURN_NOT_OK(ConnectTasks());
    
    // Validate graph
    ARROW_RETURN_NOT_OK(ValidateGraph());
    
    return arrow::Status::OK();
}

arrow::Status StreamingJobGraph::AddOperator(const OperatorNode& operator_node) {
    operators_[operator_node.operator_id] = operator_node;
    return arrow::Status::OK();
}

arrow::Status StreamingJobGraph::AddTask(const TaskVertex& task) {
    tasks_[task.task_id] = task;
    operator_to_tasks_[task.operator_id].push_back(task.task_id);
    return arrow::Status::OK();
}

std::vector<std::string> StreamingJobGraph::GetSourceTasks() const {
    std::vector<std::string> source_tasks;
    for (const auto& [task_id, task] : tasks_) {
        if (task.parameters.find("is_source") != task.parameters.end()) {
            source_tasks.push_back(task_id);
        }
    }
    return source_tasks;
}

std::vector<std::string> StreamingJobGraph::GetSinkTasks() const {
    std::vector<std::string> sink_tasks;
    for (const auto& [task_id, task] : tasks_) {
        if (task.parameters.find("is_sink") != task.parameters.end()) {
            sink_tasks.push_back(task_id);
        }
    }
    return sink_tasks;
}

std::vector<std::string> StreamingJobGraph::GetTasksForOperator(const std::string& operator_id) const {
    auto it = operator_to_tasks_.find(operator_id);
    if (it != operator_to_tasks_.end()) {
        return it->second;
    }
    return {};
}

arrow::Result<std::string> StreamingJobGraph::ToJson() const {
    nlohmann::json j;
    j["job_id"] = job_id_;
    j["job_name"] = job_name_;
    j["operators"] = nlohmann::json::array();
    j["tasks"] = nlohmann::json::array();
    
    // Serialize operators
    for (const auto& [operator_id, operator_node] : operators_) {
        nlohmann::json op_json;
        op_json["operator_id"] = operator_node.operator_id;
        op_json["operator_type"] = operator_node.operator_type;
        op_json["parameters"] = operator_node.parameters;
        op_json["is_stateful"] = operator_node.is_stateful;
        op_json["requires_shuffle"] = operator_node.requires_shuffle;
        op_json["parallelism"] = operator_node.parallelism;
        op_json["state_backend"] = operator_node.state_backend;
        j["operators"].push_back(op_json);
    }
    
    // Serialize tasks
    for (const auto& [task_id, task] : tasks_) {
        nlohmann::json task_json;
        task_json["task_id"] = task.task_id;
        task_json["operator_id"] = task.operator_id;
        task_json["task_index"] = task.task_index;
        task_json["assigned_agent_id"] = task.assigned_agent_id;
        task_json["parameters"] = task.parameters;
        j["tasks"].push_back(task_json);
    }
    
    return j.dump(2);
}

arrow::Status StreamingJobGraph::FromJson(const std::string& json) {
    try {
        nlohmann::json j = nlohmann::json::parse(json);
        
        job_id_ = j["job_id"];
        job_name_ = j["job_name"];
        
        // Deserialize operators
        for (const auto& op_json : j["operators"]) {
            OperatorNode operator_node;
            operator_node.operator_id = op_json["operator_id"];
            operator_node.operator_type = op_json["operator_type"];
            operator_node.parameters = op_json["parameters"];
            operator_node.is_stateful = op_json["is_stateful"];
            operator_node.requires_shuffle = op_json["requires_shuffle"];
            operator_node.parallelism = op_json["parallelism"];
            operator_node.state_backend = op_json["state_backend"];
            
            ARROW_RETURN_NOT_OK(AddOperator(operator_node));
        }
        
        // Deserialize tasks
        for (const auto& task_json : j["tasks"]) {
            TaskVertex task;
            task.task_id = task_json["task_id"];
            task.operator_id = task_json["operator_id"];
            task.task_index = task_json["task_index"];
            task.assigned_agent_id = task_json["assigned_agent_id"];
            task.parameters = task_json["parameters"];
            
            ARROW_RETURN_NOT_OK(AddTask(task));
        }
        
        return arrow::Status::OK();
    } catch (const std::exception& e) {
        return arrow::Status::Invalid("Failed to parse JSON: " + std::string(e.what()));
    }
}

arrow::Status StreamingJobGraph::ValidateGraph() {
    // Basic validation
    if (operators_.empty()) {
        return arrow::Status::Invalid("No operators in graph");
    }
    
    if (tasks_.empty()) {
        return arrow::Status::Invalid("No tasks in graph");
    }
    
    return arrow::Status::OK();
}

arrow::Status StreamingJobGraph::ExpandParallelism(const StreamingMorselPlan& plan) {
    // Create tasks for each operator based on parallelism
    for (const auto& [operator_id, operator_node] : operators_) {
        ARROW_RETURN_NOT_OK(CreateTasksForOperator(operator_node));
    }
    
    return arrow::Status::OK();
}

arrow::Status StreamingJobGraph::CreateTasksForOperator(const OperatorNode& operator_node) {
    // Create tasks based on parallelism
    for (int i = 0; i < operator_node.parallelism; ++i) {
        TaskVertex task;
        task.task_id = operator_node.operator_id + "_task_" + std::to_string(i);
        task.operator_id = operator_node.operator_id;
        task.task_index = i;
        task.assigned_agent_id = "";  // Will be assigned later
        task.parameters = operator_node.parameters;
        
        ARROW_RETURN_NOT_OK(AddTask(task));
    }
    
    return arrow::Status::OK();
}

arrow::Status StreamingJobGraph::ConnectTasks() {
    // Connect tasks based on operator dependencies
    // This is a simplified implementation
    return arrow::Status::OK();
}

// ========== SabotExecutionCoordinator Private Methods ==========

arrow::Result<std::string> SabotExecutionCoordinator::CreateJobGraphFromPlan(const StreamingMorselPlan& plan) {
    StreamingJobGraph job_graph;
    ARROW_RETURN_NOT_OK(job_graph.BuildFromPlan(plan));
    
    // Convert to JSON for orchestrator submission
    ARROW_ASSIGN_OR_RAISE(std::string job_graph_json, job_graph.ToJson());
    
    return job_graph_json;
}

arrow::Result<std::unordered_map<std::string, std::string>> SabotExecutionCoordinator::GetExecutionStatus(const std::string& execution_id) {
    auto job_id_it = execution_to_job_id_.find(execution_id);
    if (job_id_it == execution_to_job_id_.end()) {
        return arrow::Status::Invalid("Execution not found: " + execution_id);
    }
    
    std::string job_id = job_id_it->second;
    
    // Get job status from orchestrator
    ARROW_ASSIGN_OR_RAISE(auto job_status, orchestrator_client_->GetJobStatus(job_id));
    
    std::unordered_map<std::string, std::string> status;
    status["execution_id"] = execution_id;
    status["job_id"] = job_id;
    status["status"] = job_status.status;
    status["total_tasks"] = std::to_string(job_status.total_tasks);
    status["completed_tasks"] = std::to_string(job_status.completed_tasks);
    status["failed_tasks"] = std::to_string(job_status.failed_tasks);
    
    if (!job_status.error_message.empty()) {
        status["error_message"] = job_status.error_message;
    }
    
    return status;
}

} // namespace streaming
} // namespace sabot_sql
