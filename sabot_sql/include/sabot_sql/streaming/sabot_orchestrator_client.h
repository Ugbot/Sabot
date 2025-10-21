#pragma once

#include <arrow/api.h>
#include <string>
#include <vector>
#include <memory>
#include <unordered_map>
#include <functional>

namespace sabot_sql {
namespace streaming {

/**
 * Sabot Orchestrator Client - Production integration with Sabot's orchestrator
 * 
 * Provides C++ interface to Sabot's production orchestrator APIs:
 * - UnifiedCoordinator: Job submission and management
 * - Agent management: Agent registration and discovery
 * - Task assignment: Distributed task execution
 * - Health monitoring: Agent health and status tracking
 */
class SabotOrchestratorClient {
public:
    /**
     * Agent information from Sabot orchestrator
     */
    struct AgentInfo {
        std::string agent_id;
        std::string host;
        int port;
        int available_slots;
        int cpu_percent;
        int memory_percent;
        std::string status;
        int64_t last_heartbeat;
        
        AgentInfo() = default;
        AgentInfo(const std::string& id, const std::string& h, int p, int slots)
            : agent_id(id), host(h), port(p), available_slots(slots), cpu_percent(0), memory_percent(0), status("alive"), last_heartbeat(0) {}
    };
    
    /**
     * Job submission result
     */
    struct JobSubmissionResult {
        std::string job_id;
        bool success;
        std::string error_message;
        int64_t submission_time;
        
        JobSubmissionResult() = default;
        JobSubmissionResult(const std::string& id, bool s)
            : job_id(id), success(s), submission_time(0) {}
    };
    
    /**
     * Job status information
     */
    struct JobStatus {
        std::string job_id;
        std::string status;  // "submitted", "running", "completed", "failed"
        std::string error_message;
        int64_t total_tasks;
        int64_t completed_tasks;
        int64_t failed_tasks;
        int64_t start_time;
        int64_t end_time;
        
        JobStatus() = default;
        JobStatus(const std::string& id, const std::string& s)
            : job_id(id), status(s), total_tasks(0), completed_tasks(0), failed_tasks(0), start_time(0), end_time(0) {}
    };
    
    /**
     * Task assignment information
     */
    struct TaskAssignment {
        std::string task_id;
        std::string agent_id;
        std::string operator_type;
        std::unordered_map<std::string, std::string> parameters;
        
        TaskAssignment() = default;
        TaskAssignment(const std::string& tid, const std::string& aid, const std::string& op_type)
            : task_id(tid), agent_id(aid), operator_type(op_type) {}
    };
    
    SabotOrchestratorClient();
    ~SabotOrchestratorClient();
    
    // ========== Lifecycle ==========
    
    /**
     * Initialize orchestrator client
     * 
     * @param orchestrator_host Hostname of Sabot orchestrator
     * @param orchestrator_port Port of Sabot orchestrator
     * @param config Additional configuration parameters
     * @return Status indicating success/failure
     */
    arrow::Status Initialize(
        const std::string& orchestrator_host = "localhost",
        int orchestrator_port = 8080,
        const std::unordered_map<std::string, std::string>& config = {}
    );
    
    /**
     * Shutdown orchestrator client
     */
    arrow::Status Shutdown();
    
    // ========== Job Management ==========
    
    /**
     * Submit a streaming job to the orchestrator
     * 
     * @param job_graph_json Serialized job graph
     * @param job_name Human-readable job name
     * @return Job submission result
     */
    arrow::Result<JobSubmissionResult> SubmitJob(
        const std::string& job_graph_json,
        const std::string& job_name = "streaming_job"
    );
    
    /**
     * Get job status
     * 
     * @param job_id Job identifier
     * @return Job status information
     */
    arrow::Result<JobStatus> GetJobStatus(const std::string& job_id);
    
    /**
     * Cancel a running job
     * 
     * @param job_id Job identifier
     * @return Status indicating success/failure
     */
    arrow::Status CancelJob(const std::string& job_id);
    
    /**
     * Wait for job completion
     * 
     * @param job_id Job identifier
     * @param timeout_ms Maximum time to wait
     * @return Final job status
     */
    arrow::Result<JobStatus> WaitForJobCompletion(
        const std::string& job_id,
        int64_t timeout_ms = 300000
    );
    
    // ========== Agent Management ==========
    
    /**
     * Get available agents
     * 
     * @return List of available agents
     */
    arrow::Result<std::vector<AgentInfo>> GetAvailableAgents();
    
    /**
     * Register an agent with the orchestrator
     * 
     * @param agent_info Agent information
     * @return Status indicating success/failure
     */
    arrow::Status RegisterAgent(const AgentInfo& agent_info);
    
    /**
     * Unregister an agent
     * 
     * @param agent_id Agent identifier
     * @return Status indicating success/failure
     */
    arrow::Status UnregisterAgent(const std::string& agent_id);
    
    /**
     * Update agent status
     * 
     * @param agent_id Agent identifier
     * @param status New status
     * @return Status indicating success/failure
     */
    arrow::Status UpdateAgentStatus(const std::string& agent_id, const std::string& status);
    
    // ========== Task Management ==========
    
    /**
     * Get task assignments for a job
     * 
     * @param job_id Job identifier
     * @return List of task assignments
     */
    arrow::Result<std::vector<TaskAssignment>> GetTaskAssignments(const std::string& job_id);
    
    /**
     * Deploy tasks to agents
     * 
     * @param assignments Task assignments
     * @return Status indicating success/failure
     */
    arrow::Status DeployTasks(const std::vector<TaskAssignment>& assignments);
    
    // ========== Health Monitoring ==========
    
    /**
     * Get orchestrator health status
     * 
     * @return Health status information
     */
    arrow::Result<std::unordered_map<std::string, std::string>> GetHealthStatus();
    
    /**
     * Get cluster statistics
     * 
     * @return Cluster statistics
     */
    arrow::Result<std::unordered_map<std::string, int64_t>> GetClusterStats();
    
    // ========== Configuration ==========
    
    /**
     * Set connection timeout
     */
    void SetConnectionTimeout(int64_t timeout_ms);
    
    /**
     * Set retry configuration
     */
    void SetRetryConfig(int max_retries, int64_t retry_delay_ms);
    
    /**
     * Enable/disable SSL
     */
    void SetSSLEnabled(bool enabled);
    
private:
    // Configuration
    std::string orchestrator_host_;
    int orchestrator_port_;
    std::unordered_map<std::string, std::string> config_;
    
    // Connection settings
    int64_t connection_timeout_ms_;
    int max_retries_;
    int64_t retry_delay_ms_;
    bool ssl_enabled_;
    
    // Internal state
    bool initialized_;
    std::string base_url_;
    
    // HTTP client (would be implemented with actual HTTP library)
    void* http_client_;  // Placeholder for HTTP client
    
    // Helper methods
    arrow::Status MakeHTTPRequest(
        const std::string& method,
        const std::string& endpoint,
        const std::string& body,
        std::string& response
    );
    
    static size_t WriteCallback(void* contents, size_t size, size_t nmemb, std::string* response);
    
    arrow::Status ParseJobStatus(const std::string& json_response, JobStatus& status);
    arrow::Status ParseAgentList(const std::string& json_response, std::vector<AgentInfo>& agents);
    arrow::Status ParseTaskAssignments(const std::string& json_response, std::vector<TaskAssignment>& assignments);
    
    std::string SerializeJobGraph(const std::unordered_map<std::string, std::string>& job_graph);
    std::string SerializeAgentInfo(const AgentInfo& agent_info);
    std::string SerializeTaskAssignments(const std::vector<TaskAssignment>& assignments);
};

} // namespace streaming
} // namespace sabot_sql
