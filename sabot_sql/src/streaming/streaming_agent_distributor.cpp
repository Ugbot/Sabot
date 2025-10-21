#include "sabot_sql/streaming/streaming_agent_distributor.h"
#include "sabot_sql/streaming/marbledb_integration.h"
#include "sabot_sql/streaming/checkpoint_barrier_injector.h"
#include <arrow/table.h>
#include <arrow/array.h>
#include <arrow/builder.h>
#include <arrow/compute/api.h>
#include <nlohmann/json.hpp>
#include <sstream>
#include <chrono>
#include <thread>
#include <algorithm>
#include <random>

namespace sabot_sql {
namespace streaming {

// ============================================================================
// StreamingAgentDistributor Implementation
// ============================================================================

StreamingAgentDistributor::StreamingAgentDistributor() {
    running_.store(false);
}

StreamingAgentDistributor::~StreamingAgentDistributor() {
    Shutdown();
}

arrow::Status StreamingAgentDistributor::Initialize(
    std::shared_ptr<MarbleDBIntegration> marbledb,
    std::shared_ptr<CheckpointBarrierInjector> barrier_injector
) {
    marbledb_ = marbledb;
    barrier_injector_ = barrier_injector;
    
    running_.store(true);
    
    // Start monitoring thread
    monitoring_thread_ = std::thread(&StreamingAgentDistributor::MonitoringThreadLoop, this);
    
    return arrow::Status::OK();
}

arrow::Status StreamingAgentDistributor::Shutdown() {
    running_.store(false);
    
    // Notify monitoring thread
    monitoring_cv_.notify_all();
    
    // Wait for thread to finish
    if (monitoring_thread_.joinable()) {
        monitoring_thread_.join();
    }
    
    return arrow::Status::OK();
}

arrow::Status StreamingAgentDistributor::RegisterAgent(const AgentConfig& config) {
    std::lock_guard<std::mutex> lock(agents_mutex_);
    
    agents_[config.agent_id] = config;
    
    return arrow::Status::OK();
}

arrow::Status StreamingAgentDistributor::UnregisterAgent(const std::string& agent_id) {
    std::lock_guard<std::mutex> lock(agents_mutex_);
    
    agents_.erase(agent_id);
    
    return arrow::Status::OK();
}

arrow::Status StreamingAgentDistributor::UpdateAgentStatus(
    const std::string& agent_id,
    const std::string& status
) {
    std::lock_guard<std::mutex> lock(agents_mutex_);
    
    auto it = agents_.find(agent_id);
    if (it != agents_.end()) {
        it->second.status = status;
        it->second.last_heartbeat = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()
        ).count();
    }
    
    return arrow::Status::OK();
}

arrow::Result<std::vector<StreamingAgentDistributor::AgentConfig>> StreamingAgentDistributor::GetAvailableAgents() {
    std::lock_guard<std::mutex> lock(agents_mutex_);
    
    std::vector<AgentConfig> available;
    for (const auto& [agent_id, config] : agents_) {
        if (config.status == "alive" && config.available_slots > 0) {
            available.push_back(config);
        }
    }
    
    return available;
}

arrow::Result<StreamingAgentDistributor::AgentConfig> StreamingAgentDistributor::GetAgent(const std::string& agent_id) {
    std::lock_guard<std::mutex> lock(agents_mutex_);
    
    auto it = agents_.find(agent_id);
    if (it == agents_.end()) {
        return arrow::Status::Invalid("Agent not found");
    }
    
    return it->second;
}

arrow::Result<StreamingAgentDistributor::DistributionPlan> StreamingAgentDistributor::CreateDistributionPlan(
    const StreamingMorselPlan& plan,
    const std::string& execution_id
) {
    DistributionPlan distribution_plan;
    distribution_plan.execution_id = execution_id;
    distribution_plan.plan_id = plan.query_id;
    distribution_plan.created_timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()
    ).count();
    distribution_plan.last_updated = distribution_plan.created_timestamp;
    
    // Get available agents
    ARROW_ASSIGN_OR_RAISE(std::vector<AgentConfig> available_agents, GetAvailableAgents());
    
    if (available_agents.empty()) {
        return arrow::Status::Invalid("No available agents");
    }
    
    // Analyze plan to identify partitionable sources
    int partition_count = 0;
    for (const auto& source : plan.streaming_sources) {
        if (source.source_type == "kafka") {
            // For Kafka, create one partition per partition
            for (int i = 0; i < source.max_parallelism; ++i) {
                PartitionAssignment assignment;
                assignment.partition_id = source.topic + "_partition_" + std::to_string(i);
                assignment.source_type = "kafka";
                assignment.source_config = source.properties;
                assignment.source_config["topic"] = source.topic;
                assignment.source_config["partition"] = std::to_string(i);
                assignment.assigned_timestamp = distribution_plan.created_timestamp;
                assignment.is_active = true;
                
                // Assign to best available agent
                ARROW_ASSIGN_OR_RAISE(
                    std::string best_agent,
                    SelectBestAgent(available_agents)
                );
                assignment.assigned_agent_id = best_agent;
                
                distribution_plan.partition_assignments[assignment.partition_id] = assignment;
                distribution_plan.agent_partitions[best_agent].push_back(assignment.partition_id);
                
                partition_count++;
            }
        } else if (source.source_type == "file") {
            // For file sources, create one partition per file
            PartitionAssignment assignment;
            assignment.partition_id = source.topic + "_file";
            assignment.source_type = "file";
            assignment.source_config = source.properties;
            assignment.source_config["file_path"] = source.topic;
            assignment.assigned_timestamp = distribution_plan.created_timestamp;
            assignment.is_active = true;
            
            // Assign to best available agent
            ARROW_ASSIGN_OR_RAISE(
                std::string best_agent,
                SelectBestAgent(available_agents)
            );
            assignment.assigned_agent_id = best_agent;
            
            distribution_plan.partition_assignments[assignment.partition_id] = assignment;
            distribution_plan.agent_partitions[best_agent].push_back(assignment.partition_id);
            
            partition_count++;
        }
    }
    
    distribution_plan.total_partitions = partition_count;
    distribution_plan.assigned_agents = available_agents.size();
    
    // Store distribution plan
    {
        std::lock_guard<std::mutex> lock(plans_mutex_);
        distribution_plans_[execution_id] = distribution_plan;
    }
    
    // Persist distribution plan
    ARROW_RETURN_NOT_OK(PersistDistributionPlan(distribution_plan));
    
    return distribution_plan;
}

arrow::Status StreamingAgentDistributor::UpdateDistributionPlan(const DistributionPlan& plan) {
    std::lock_guard<std::mutex> lock(plans_mutex_);
    
    distribution_plans_[plan.execution_id] = plan;
    
    // Persist updated plan
    ARROW_RETURN_NOT_OK(PersistDistributionPlan(plan));
    
    return arrow::Status::OK();
}

arrow::Result<StreamingAgentDistributor::DistributionPlan> StreamingAgentDistributor::GetDistributionPlan(
    const std::string& execution_id
) {
    std::lock_guard<std::mutex> lock(plans_mutex_);
    
    auto it = distribution_plans_.find(execution_id);
    if (it == distribution_plans_.end()) {
        return arrow::Status::Invalid("Distribution plan not found");
    }
    
    return it->second;
}

arrow::Status StreamingAgentDistributor::DeployTasksToAgents(const DistributionPlan& plan) {
    // Deploy tasks to assigned agents
    for (const auto& [partition_id, assignment] : plan.partition_assignments) {
        ARROW_RETURN_NOT_OK(DeployTaskToAgent(
            assignment.assigned_agent_id,
            partition_id,
            assignment.source_config
        ));
    }
    
    return arrow::Status::OK();
}

arrow::Status StreamingAgentDistributor::StartExecution(const std::string& execution_id) {
    ARROW_ASSIGN_OR_RAISE(DistributionPlan plan, GetDistributionPlan(execution_id));
    
    // Start tasks on all assigned agents
    for (const auto& [agent_id, partitions] : plan.agent_partitions) {
        for (const auto& partition_id : partitions) {
            ARROW_RETURN_NOT_OK(StartTaskOnAgent(agent_id, partition_id));
        }
    }
    
    return arrow::Status::OK();
}

arrow::Status StreamingAgentDistributor::StopExecution(const std::string& execution_id) {
    ARROW_ASSIGN_OR_RAISE(DistributionPlan plan, GetDistributionPlan(execution_id));
    
    // Stop tasks on all assigned agents
    for (const auto& [agent_id, partitions] : plan.agent_partitions) {
        for (const auto& partition_id : partitions) {
            ARROW_RETURN_NOT_OK(StopTaskOnAgent(agent_id, partition_id));
        }
    }
    
    return arrow::Status::OK();
}

arrow::Status StreamingAgentDistributor::CancelExecution(const std::string& execution_id) {
    // Stop execution
    ARROW_RETURN_NOT_OK(StopExecution(execution_id));
    
    // Cleanup distribution plan
    {
        std::lock_guard<std::mutex> lock(plans_mutex_);
        distribution_plans_.erase(execution_id);
    }
    
    // Cleanup execution metrics
    {
        std::lock_guard<std::mutex> lock(metrics_mutex_);
        execution_metrics_.erase(execution_id);
    }
    
    // Cleanup execution results
    {
        std::lock_guard<std::mutex> lock(results_mutex_);
        execution_results_.erase(execution_id);
    }
    
    return arrow::Status::OK();
}

arrow::Status StreamingAgentDistributor::AssignPartitionToAgent(
    const std::string& execution_id,
    const std::string& partition_id,
    const std::string& agent_id
) {
    ARROW_ASSIGN_OR_RAISE(DistributionPlan plan, GetDistributionPlan(execution_id));
    
    // Update partition assignment
    auto it = plan.partition_assignments.find(partition_id);
    if (it != plan.partition_assignments.end()) {
        it->second.assigned_agent_id = agent_id;
        it->second.assigned_timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()
        ).count();
    }
    
    // Update agent partitions
    plan.agent_partitions[agent_id].push_back(partition_id);
    
    // Update distribution plan
    ARROW_RETURN_NOT_OK(UpdateDistributionPlan(plan));
    
    return arrow::Status::OK();
}

arrow::Status StreamingAgentDistributor::ReassignPartition(
    const std::string& execution_id,
    const std::string& partition_id,
    const std::string& new_agent_id
) {
    ARROW_ASSIGN_OR_RAISE(DistributionPlan plan, GetDistributionPlan(execution_id));
    
    // Find current assignment
    auto it = plan.partition_assignments.find(partition_id);
    if (it == plan.partition_assignments.end()) {
        return arrow::Status::Invalid("Partition not found");
    }
    
    std::string old_agent_id = it->second.assigned_agent_id;
    
    // Update partition assignment
    it->second.assigned_agent_id = new_agent_id;
    it->second.assigned_timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()
    ).count();
    
    // Update agent partitions
    auto old_agent_it = plan.agent_partitions.find(old_agent_id);
    if (old_agent_it != plan.agent_partitions.end()) {
        auto partition_it = std::find(
            old_agent_it->second.begin(),
            old_agent_it->second.end(),
            partition_id
        );
        if (partition_it != old_agent_it->second.end()) {
            old_agent_it->second.erase(partition_it);
        }
    }
    
    plan.agent_partitions[new_agent_id].push_back(partition_id);
    
    // Update distribution plan
    ARROW_RETURN_NOT_OK(UpdateDistributionPlan(plan));
    
    return arrow::Status::OK();
}

arrow::Status StreamingAgentDistributor::RebalancePartitions(const std::string& execution_id) {
    ARROW_ASSIGN_OR_RAISE(DistributionPlan plan, GetDistributionPlan(execution_id));
    
    // Get available agents
    ARROW_ASSIGN_OR_RAISE(std::vector<AgentConfig> available_agents, GetAvailableAgents());
    
    if (available_agents.empty()) {
        return arrow::Status::Invalid("No available agents for rebalancing");
    }
    
    // Rebalance partitions
    ARROW_RETURN_NOT_OK(BalancePartitionLoad(execution_id));
    ARROW_RETURN_NOT_OK(OptimizePartitionPlacement(execution_id));
    
    return arrow::Status::OK();
}

arrow::Result<std::shared_ptr<arrow::Table>> StreamingAgentDistributor::CollectResults(
    const std::string& execution_id,
    const std::string& output_operator_id
) {
    std::lock_guard<std::mutex> lock(results_mutex_);
    
    auto it = execution_results_.find(execution_id);
    if (it == execution_results_.end()) {
        return arrow::Status::Invalid("Execution results not found");
    }
    
    // Aggregate batches
    ARROW_ASSIGN_OR_RAISE(
        std::shared_ptr<arrow::Table> results,
        AggregateBatches(it->second)
    );
    
    return results;
}

arrow::Status StreamingAgentDistributor::AggregateResults(
    const std::string& execution_id,
    const std::string& operator_id,
    const std::shared_ptr<arrow::RecordBatch>& batch
) {
    std::lock_guard<std::mutex> lock(results_mutex_);
    
    execution_results_[execution_id].push_back(batch);
    
    return arrow::Status::OK();
}

arrow::Result<StreamingAgentDistributor::ExecutionMetrics> StreamingAgentDistributor::GetExecutionMetrics(
    const std::string& execution_id
) {
    std::lock_guard<std::mutex> lock(metrics_mutex_);
    
    auto it = execution_metrics_.find(execution_id);
    if (it == execution_metrics_.end()) {
        return arrow::Status::Invalid("Execution metrics not found");
    }
    
    return it->second;
}

arrow::Status StreamingAgentDistributor::UpdateExecutionMetrics(
    const std::string& execution_id,
    const ExecutionMetrics& metrics
) {
    std::lock_guard<std::mutex> lock(metrics_mutex_);
    
    execution_metrics_[execution_id] = metrics;
    
    // Persist metrics
    ARROW_RETURN_NOT_OK(PersistExecutionMetrics(metrics));
    
    return arrow::Status::OK();
}

arrow::Result<std::vector<std::string>> StreamingAgentDistributor::GetActiveExecutions() {
    std::lock_guard<std::mutex> lock(plans_mutex_);
    
    std::vector<std::string> active;
    for (const auto& [execution_id, plan] : distribution_plans_) {
        active.push_back(execution_id);
    }
    
    return active;
}

arrow::Status StreamingAgentDistributor::HandleAgentFailure(const std::string& agent_id) {
    // Update agent status
    ARROW_RETURN_NOT_OK(UpdateAgentStatus(agent_id, "failed"));
    
    // Find affected executions
    std::vector<std::string> affected_executions;
    {
        std::lock_guard<std::mutex> lock(plans_mutex_);
        for (const auto& [execution_id, plan] : distribution_plans_) {
            if (plan.agent_partitions.find(agent_id) != plan.agent_partitions.end()) {
                affected_executions.push_back(execution_id);
            }
        }
    }
    
    // Recover from agent failure for each affected execution
    for (const auto& execution_id : affected_executions) {
        ARROW_RETURN_NOT_OK(RecoverFromAgentFailure(execution_id, agent_id));
    }
    
    return arrow::Status::OK();
}

arrow::Status StreamingAgentDistributor::RecoverFromAgentFailure(
    const std::string& execution_id,
    const std::string& failed_agent_id
) {
    // Reassign failed tasks
    ARROW_RETURN_NOT_OK(ReassignFailedTasks(execution_id, failed_agent_id));
    
    // Rebalance partitions
    ARROW_RETURN_NOT_OK(RebalancePartitions(execution_id));
    
    return arrow::Status::OK();
}

arrow::Status StreamingAgentDistributor::ReassignFailedTasks(
    const std::string& execution_id,
    const std::string& failed_agent_id
) {
    ARROW_ASSIGN_OR_RAISE(DistributionPlan plan, GetDistributionPlan(execution_id));
    
    // Get available agents
    ARROW_ASSIGN_OR_RAISE(std::vector<AgentConfig> available_agents, GetAvailableAgents());
    
    if (available_agents.empty()) {
        return arrow::Status::Invalid("No available agents for reassignment");
    }
    
    // Reassign partitions from failed agent
    auto it = plan.agent_partitions.find(failed_agent_id);
    if (it != plan.agent_partitions.end()) {
        for (const auto& partition_id : it->second) {
            ARROW_ASSIGN_OR_RAISE(
                std::string best_agent,
                SelectBestAgent(available_agents)
            );
            ARROW_RETURN_NOT_OK(ReassignPartition(execution_id, partition_id, best_agent));
        }
    }
    
    return arrow::Status::OK();
}

// ============================================================================
// Private Methods
// ============================================================================

void StreamingAgentDistributor::MonitoringThreadLoop() {
    while (running_.load()) {
        std::unique_lock<std::mutex> lock(agents_mutex_);
        
        // Wait for monitoring signal
        monitoring_cv_.wait_for(lock, std::chrono::seconds(10), [this] { 
            return !running_.load(); 
        });
        
        if (!running_.load()) {
            break;
        }
        
        lock.unlock();
        
        // Monitor agent health
        auto status1 = MonitorAgentHealth();
        if (!status1.ok()) {
            // Log error but continue
        }
        
        // Monitor execution progress
        auto status2 = MonitorExecutionProgress();
        if (!status2.ok()) {
            // Log error but continue
        }
        
        // Cleanup completed executions
        auto status3 = CleanupCompletedExecutions();
        if (!status3.ok()) {
            // Log error but continue
        }
    }
}

arrow::Status StreamingAgentDistributor::MonitorAgentHealth() {
    std::lock_guard<std::mutex> lock(agents_mutex_);
    
    int64_t current_time = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()
    ).count();
    
    // Check for failed agents (no heartbeat for 30 seconds)
    for (auto it = agents_.begin(); it != agents_.end();) {
        if (current_time - it->second.last_heartbeat > 30000) {
            std::string failed_agent_id = it->first;
            it = agents_.erase(it);
            
            // Handle agent failure
            ARROW_RETURN_NOT_OK(HandleAgentFailure(failed_agent_id));
        } else {
            ++it;
        }
    }
    
    return arrow::Status::OK();
}

arrow::Status StreamingAgentDistributor::MonitorExecutionProgress() {
    // Mock monitoring - in real implementation, this would collect metrics from agents
    return arrow::Status::OK();
}

arrow::Status StreamingAgentDistributor::CleanupCompletedExecutions() {
    std::lock_guard<std::mutex> lock(plans_mutex_);
    
    // Cleanup executions that have been running for more than 1 hour
    int64_t current_time = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()
    ).count();
    
    for (auto it = distribution_plans_.begin(); it != distribution_plans_.end();) {
        if (current_time - it->second.created_timestamp > 3600000) {  // 1 hour
            std::string execution_id = it->first;
            it = distribution_plans_.erase(it);
            
            // Cleanup related data
            {
                std::lock_guard<std::mutex> metrics_lock(metrics_mutex_);
                execution_metrics_.erase(execution_id);
            }
            {
                std::lock_guard<std::mutex> results_lock(results_mutex_);
                execution_results_.erase(execution_id);
            }
        } else {
            ++it;
        }
    }
    
    return arrow::Status::OK();
}

arrow::Result<std::string> StreamingAgentDistributor::SelectBestAgent(
    const std::vector<AgentConfig>& available_agents
) {
    if (available_agents.empty()) {
        return arrow::Status::Invalid("No available agents");
    }
    
    // Simple selection: choose agent with most available slots
    auto best_agent = std::max_element(
        available_agents.begin(),
        available_agents.end(),
        [](const AgentConfig& a, const AgentConfig& b) {
            return a.available_slots < b.available_slots;
        }
    );
    
    return best_agent->agent_id;
}

arrow::Status StreamingAgentDistributor::BalancePartitionLoad(const std::string& execution_id) {
    ARROW_ASSIGN_OR_RAISE(DistributionPlan plan, GetDistributionPlan(execution_id));
    
    // Calculate load per agent
    std::unordered_map<std::string, int> agent_load;
    for (const auto& [agent_id, partitions] : plan.agent_partitions) {
        agent_load[agent_id] = partitions.size();
    }
    
    // Find overloaded and underloaded agents
    std::vector<std::string> overloaded_agents;
    std::vector<std::string> underloaded_agents;
    
    int avg_load = plan.total_partitions / plan.assigned_agents;
    for (const auto& [agent_id, load] : agent_load) {
        if (load > avg_load + 1) {
            overloaded_agents.push_back(agent_id);
        } else if (load < avg_load - 1) {
            underloaded_agents.push_back(agent_id);
        }
    }
    
    // Rebalance partitions
    for (const auto& overloaded_agent : overloaded_agents) {
        for (const auto& underloaded_agent : underloaded_agents) {
            if (agent_load[overloaded_agent] > agent_load[underloaded_agent] + 1) {
                // Move one partition from overloaded to underloaded
                auto& overloaded_partitions = plan.agent_partitions[overloaded_agent];
                if (!overloaded_partitions.empty()) {
                    std::string partition_id = overloaded_partitions.back();
                    overloaded_partitions.pop_back();
                    
                    ARROW_RETURN_NOT_OK(ReassignPartition(execution_id, partition_id, underloaded_agent));
                    
                    agent_load[overloaded_agent]--;
                    agent_load[underloaded_agent]++;
                }
            }
        }
    }
    
    return arrow::Status::OK();
}

arrow::Status StreamingAgentDistributor::OptimizePartitionPlacement(const std::string& execution_id) {
    // Mock optimization - in real implementation, this would optimize based on network topology, data locality, etc.
    return arrow::Status::OK();
}

arrow::Status StreamingAgentDistributor::DeployTaskToAgent(
    const std::string& agent_id,
    const std::string& task_id,
    const std::unordered_map<std::string, std::string>& task_config
) {
    // Mock deployment - in real implementation, this would send task configuration to the agent
    return arrow::Status::OK();
}

arrow::Status StreamingAgentDistributor::StartTaskOnAgent(
    const std::string& agent_id,
    const std::string& task_id
) {
    // Mock start - in real implementation, this would start the task on the agent
    return arrow::Status::OK();
}

arrow::Status StreamingAgentDistributor::StopTaskOnAgent(
    const std::string& agent_id,
    const std::string& task_id
) {
    // Mock stop - in real implementation, this would stop the task on the agent
    return arrow::Status::OK();
}

arrow::Result<std::shared_ptr<arrow::Table>> StreamingAgentDistributor::AggregateBatches(
    const std::vector<std::shared_ptr<arrow::RecordBatch>>& batches
) {
    if (batches.empty()) {
        return arrow::Status::Invalid("No batches to aggregate");
    }
    
    // Simple aggregation: concatenate all batches
    std::vector<std::shared_ptr<arrow::RecordBatch>> valid_batches;
    for (const auto& batch : batches) {
        if (batch && batch->num_rows() > 0) {
            valid_batches.push_back(batch);
        }
    }
    
    if (valid_batches.empty()) {
        return arrow::Status::Invalid("No valid batches to aggregate");
    }
    
    // Use Arrow's concatenate function
    ARROW_ASSIGN_OR_RAISE(
        std::shared_ptr<arrow::Table> result,
        arrow::Table::FromRecordBatches(valid_batches)
    );
    
    return result;
}

arrow::Status StreamingAgentDistributor::MergeExecutionResults(const std::string& execution_id) {
    // Mock merge - in real implementation, this would merge results from multiple agents
    return arrow::Status::OK();
}

arrow::Status StreamingAgentDistributor::PersistDistributionPlan(const DistributionPlan& plan) {
    if (!marbledb_) {
        return arrow::Status::OK();  // No persistence if MarbleDB not available
    }
    
    nlohmann::json plan_json;
    plan_json["execution_id"] = plan.execution_id;
    plan_json["plan_id"] = plan.plan_id;
    plan_json["total_partitions"] = plan.total_partitions;
    plan_json["assigned_agents"] = plan.assigned_agents;
    plan_json["created_timestamp"] = plan.created_timestamp;
    plan_json["last_updated"] = plan.last_updated;
    
    // Serialize partition assignments
    plan_json["partition_assignments"] = nlohmann::json::object();
    for (const auto& [partition_id, assignment] : plan.partition_assignments) {
        nlohmann::json assignment_json;
        assignment_json["partition_id"] = assignment.partition_id;
        assignment_json["source_type"] = assignment.source_type;
        assignment_json["assigned_agent_id"] = assignment.assigned_agent_id;
        assignment_json["source_config"] = assignment.source_config;
        assignment_json["assigned_timestamp"] = assignment.assigned_timestamp;
        assignment_json["is_active"] = assignment.is_active;
        
        plan_json["partition_assignments"][partition_id] = assignment_json;
    }
    
    // Serialize agent partitions
    plan_json["agent_partitions"] = plan.agent_partitions;
    
    // Store in MarbleDB
    ARROW_RETURN_NOT_OK(marbledb_->WriteState(
        "distribution_plan_" + plan.execution_id,
        plan_json.dump()
    ));
    
    return arrow::Status::OK();
}

arrow::Status StreamingAgentDistributor::LoadDistributionPlan(const std::string& execution_id) {
    if (!marbledb_) {
        return arrow::Status::OK();  // No persistence if MarbleDB not available
    }
    
    // Load from MarbleDB
    ARROW_ASSIGN_OR_RAISE(
        std::string plan_json_str,
        marbledb_->ReadState("distribution_plan_" + execution_id)
    );
    
    try {
        nlohmann::json plan_json = nlohmann::json::parse(plan_json_str);
        
        DistributionPlan plan;
        plan.execution_id = plan_json["execution_id"];
        plan.plan_id = plan_json["plan_id"];
        plan.total_partitions = plan_json["total_partitions"];
        plan.assigned_agents = plan_json["assigned_agents"];
        plan.created_timestamp = plan_json["created_timestamp"];
        plan.last_updated = plan_json["last_updated"];
        
        // Deserialize partition assignments
        for (const auto& [partition_id, assignment_json] : plan_json["partition_assignments"].items()) {
            PartitionAssignment assignment;
            assignment.partition_id = assignment_json["partition_id"];
            assignment.source_type = assignment_json["source_type"];
            assignment.assigned_agent_id = assignment_json["assigned_agent_id"];
            assignment.source_config = assignment_json["source_config"];
            assignment.assigned_timestamp = assignment_json["assigned_timestamp"];
            assignment.is_active = assignment_json["is_active"];
            
            plan.partition_assignments[partition_id] = assignment;
        }
        
        // Deserialize agent partitions
        plan.agent_partitions = plan_json["agent_partitions"];
        
        // Store in memory
        {
            std::lock_guard<std::mutex> lock(plans_mutex_);
            distribution_plans_[execution_id] = plan;
        }
        
    } catch (const std::exception& e) {
        return arrow::Status::Invalid("Failed to parse distribution plan: " + std::string(e.what()));
    }
    
    return arrow::Status::OK();
}

arrow::Status StreamingAgentDistributor::PersistExecutionMetrics(const ExecutionMetrics& metrics) {
    if (!marbledb_) {
        return arrow::Status::OK();  // No persistence if MarbleDB not available
    }
    
    nlohmann::json metrics_json;
    metrics_json["execution_id"] = metrics.execution_id;
    metrics_json["total_rows_processed"] = metrics.total_rows_processed;
    metrics_json["total_bytes_processed"] = metrics.total_bytes_processed;
    metrics_json["execution_time_ms"] = metrics.execution_time_ms;
    metrics_json["agent_metrics"] = metrics.agent_metrics;
    metrics_json["partition_metrics"] = metrics.partition_metrics;
    metrics_json["last_updated"] = metrics.last_updated;
    
    // Store in MarbleDB
    ARROW_RETURN_NOT_OK(marbledb_->WriteState(
        "execution_metrics_" + metrics.execution_id,
        metrics_json.dump()
    ));
    
    return arrow::Status::OK();
}

arrow::Status StreamingAgentDistributor::LoadExecutionMetrics(const std::string& execution_id) {
    if (!marbledb_) {
        return arrow::Status::OK();  // No persistence if MarbleDB not available
    }
    
    // Load from MarbleDB
    ARROW_ASSIGN_OR_RAISE(
        std::string metrics_json_str,
        marbledb_->ReadState("execution_metrics_" + execution_id)
    );
    
    try {
        nlohmann::json metrics_json = nlohmann::json::parse(metrics_json_str);
        
        ExecutionMetrics metrics;
        metrics.execution_id = metrics_json["execution_id"];
        metrics.total_rows_processed = metrics_json["total_rows_processed"];
        metrics.total_bytes_processed = metrics_json["total_bytes_processed"];
        metrics.execution_time_ms = metrics_json["execution_time_ms"];
        metrics.agent_metrics = metrics_json["agent_metrics"];
        metrics.partition_metrics = metrics_json["partition_metrics"];
        metrics.last_updated = metrics_json["last_updated"];
        
        // Store in memory
        {
            std::lock_guard<std::mutex> lock(metrics_mutex_);
            execution_metrics_[execution_id] = metrics;
        }
        
    } catch (const std::exception& e) {
        return arrow::Status::Invalid("Failed to parse execution metrics: " + std::string(e.what()));
    }
    
    return arrow::Status::OK();
}

// ============================================================================
// PartitionRebalancer Implementation
// ============================================================================

PartitionRebalancer::PartitionRebalancer() {
    // Initialize
}

PartitionRebalancer::~PartitionRebalancer() {
    Shutdown();
}

arrow::Status PartitionRebalancer::Initialize(std::shared_ptr<MarbleDBIntegration> marbledb) {
    marbledb_ = marbledb;
    return arrow::Status::OK();
}

arrow::Status PartitionRebalancer::Shutdown() {
    return arrow::Status::OK();
}

arrow::Result<PartitionRebalancer::RebalancePlan> PartitionRebalancer::CreateRebalancePlan(
    const std::string& execution_id,
    const std::vector<std::string>& failed_agents,
    const std::vector<std::string>& available_agents
) {
    RebalancePlan plan;
    plan.execution_id = execution_id;
    plan.failed_agents = failed_agents;
    plan.available_agents = available_agents;
    plan.created_timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()
    ).count();
    plan.is_executed = false;
    
    // Calculate optimal assignment
    ARROW_RETURN_NOT_OK(CalculateOptimalAssignment(
        failed_agents,
        available_agents,
        plan.partition_reassignments
    ));
    
    // Validate plan
    ARROW_RETURN_NOT_OK(ValidateRebalancePlan(plan));
    
    // Store plan
    {
        std::lock_guard<std::mutex> lock(rebalance_mutex_);
        active_rebalances_[execution_id] = plan;
    }
    
    // Persist plan
    ARROW_RETURN_NOT_OK(PersistRebalancePlan(plan));
    
    return plan;
}

arrow::Status PartitionRebalancer::ExecuteRebalancePlan(const RebalancePlan& plan) {
    // Execute partition reassignments
    for (const auto& [partition_id, new_agent_id] : plan.partition_reassignments) {
        ARROW_RETURN_NOT_OK(MigratePartition(partition_id, "", new_agent_id));
    }
    
    // Mark plan as executed
    {
        std::lock_guard<std::mutex> lock(rebalance_mutex_);
        auto it = active_rebalances_.find(plan.execution_id);
        if (it != active_rebalances_.end()) {
            it->second.is_executed = true;
        }
    }
    
    return arrow::Status::OK();
}

arrow::Status PartitionRebalancer::CancelRebalancePlan(const std::string& execution_id) {
    std::lock_guard<std::mutex> lock(rebalance_mutex_);
    
    active_rebalances_.erase(execution_id);
    migration_steps_.erase(execution_id);
    
    return arrow::Status::OK();
}

arrow::Status PartitionRebalancer::MigratePartition(
    const std::string& partition_id,
    const std::string& from_agent_id,
    const std::string& to_agent_id
) {
    // Mock migration - in real implementation, this would coordinate partition migration
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    
    return arrow::Status::OK();
}

arrow::Status PartitionRebalancer::WaitForMigrationCompletion(
    const std::string& partition_id,
    int64_t timeout_ms
) {
    // Mock wait - in real implementation, this would wait for migration completion
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    
    return arrow::Status::OK();
}

arrow::Result<PartitionRebalancer::RebalancePlan> PartitionRebalancer::GetRebalancePlan(
    const std::string& execution_id
) {
    std::lock_guard<std::mutex> lock(rebalance_mutex_);
    
    auto it = active_rebalances_.find(execution_id);
    if (it == active_rebalances_.end()) {
        return arrow::Status::Invalid("Rebalance plan not found");
    }
    
    return it->second;
}

arrow::Result<std::vector<PartitionRebalancer::MigrationStep>> PartitionRebalancer::GetMigrationSteps(
    const std::string& execution_id
) {
    std::lock_guard<std::mutex> lock(rebalance_mutex_);
    
    auto it = migration_steps_.find(execution_id);
    if (it == migration_steps_.end()) {
        return std::vector<MigrationStep>();
    }
    
    return it->second;
}

arrow::Result<bool> PartitionRebalancer::IsRebalancingComplete(const std::string& execution_id) {
    ARROW_ASSIGN_OR_RAISE(RebalancePlan plan, GetRebalancePlan(execution_id));
    return plan.is_executed;
}

arrow::Status PartitionRebalancer::CalculateOptimalAssignment(
    const std::vector<std::string>& partitions,
    const std::vector<std::string>& available_agents,
    std::unordered_map<std::string, std::string>& assignments
) {
    if (available_agents.empty()) {
        return arrow::Status::Invalid("No available agents for assignment");
    }
    
    // Simple round-robin assignment
    int agent_index = 0;
    for (const auto& partition_id : partitions) {
        assignments[partition_id] = available_agents[agent_index % available_agents.size()];
        agent_index++;
    }
    
    return arrow::Status::OK();
}

arrow::Status PartitionRebalancer::ValidateRebalancePlan(const RebalancePlan& plan) {
    if (plan.execution_id.empty()) {
        return arrow::Status::Invalid("Execution ID is empty");
    }
    
    if (plan.available_agents.empty()) {
        return arrow::Status::Invalid("No available agents");
    }
    
    return arrow::Status::OK();
}

arrow::Status PartitionRebalancer::PersistRebalancePlan(const RebalancePlan& plan) {
    if (!marbledb_) {
        return arrow::Status::OK();  // No persistence if MarbleDB not available
    }
    
    nlohmann::json plan_json;
    plan_json["execution_id"] = plan.execution_id;
    plan_json["failed_agents"] = plan.failed_agents;
    plan_json["available_agents"] = plan.available_agents;
    plan_json["partition_reassignments"] = plan.partition_reassignments;
    plan_json["created_timestamp"] = plan.created_timestamp;
    plan_json["is_executed"] = plan.is_executed;
    
    // Store in MarbleDB
    ARROW_RETURN_NOT_OK(marbledb_->WriteState(
        "rebalance_plan_" + plan.execution_id,
        plan_json.dump()
    ));
    
    return arrow::Status::OK();
}

arrow::Status PartitionRebalancer::LoadRebalancePlan(const std::string& execution_id) {
    if (!marbledb_) {
        return arrow::Status::OK();  // No persistence if MarbleDB not available
    }
    
    // Load from MarbleDB
    ARROW_ASSIGN_OR_RAISE(
        std::string plan_json_str,
        marbledb_->ReadState("rebalance_plan_" + execution_id)
    );
    
    try {
        nlohmann::json plan_json = nlohmann::json::parse(plan_json_str);
        
        RebalancePlan plan;
        plan.execution_id = plan_json["execution_id"];
        plan.failed_agents = plan_json["failed_agents"];
        plan.available_agents = plan_json["available_agents"];
        plan.partition_reassignments = plan_json["partition_reassignments"];
        plan.created_timestamp = plan_json["created_timestamp"];
        plan.is_executed = plan_json["is_executed"];
        
        // Store in memory
        {
            std::lock_guard<std::mutex> lock(rebalance_mutex_);
            active_rebalances_[execution_id] = plan;
        }
        
    } catch (const std::exception& e) {
        return arrow::Status::Invalid("Failed to parse rebalance plan: " + std::string(e.what()));
    }
    
    return arrow::Status::OK();
}

} // namespace streaming
} // namespace sabot_sql
