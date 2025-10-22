// Unified State Store
//
// MarbleDB-backed unified state store for graphs, agents, workflows, and system state.

#pragma once

#include <arrow/api.h>
#include <memory>
#include <string>
#include <vector>
#include <unordered_map>
#include <chrono>

namespace marble {
    class MarbleDB;
    class ColumnFamilyHandle;
    struct DBOptions;
    struct ReadOptions;
    struct WriteOptions;
    class Iterator;
}

namespace sabot_cypher {
namespace state {

using Timestamp = std::chrono::system_clock::time_point;

/// Vertex in the graph
struct Vertex {
    int64_t id;
    std::string name;
    std::string type;
    std::unordered_map<std::string, std::string> properties;
    Timestamp timestamp;
};

/// Edge in the graph
struct Edge {
    int64_t id;
    int64_t source;
    int64_t target;
    std::string type;
    std::unordered_map<std::string, std::string> properties;
    Timestamp timestamp;
};

/// Agent memory entry
struct AgentMemory {
    std::string memory_id;
    std::string agent_id;
    std::string memory_type;  // "short_term", "long_term", "episodic"
    std::string content;
    std::vector<float> embedding;
    std::unordered_map<std::string, std::string> metadata;
    Timestamp timestamp;
};

/// Agent conversation
struct AgentConversation {
    std::string conversation_id;
    std::string agent_id;
    std::string user_id;
    std::vector<std::string> messages;
    std::unordered_map<std::string, std::string> context;
    Timestamp timestamp;
};

/// Agent tool call
struct AgentToolCall {
    std::string tool_call_id;
    std::string agent_id;
    std::string tool_name;
    std::string arguments;
    std::string result;
    double duration_ms;
    bool success;
    Timestamp timestamp;
};

/// Workflow task
enum class TaskStatus {
    PENDING,
    RUNNING,
    COMPLETED,
    FAILED,
    CANCELLED
};

struct Task {
    std::string task_id;
    std::string workflow_id;
    std::string description;
    TaskStatus status;
    std::vector<std::string> dependencies;
    std::string result;
    Timestamp created_at;
    Timestamp updated_at;
};

/// Workflow checkpoint
struct Checkpoint {
    std::string checkpoint_id;
    std::string workflow_id;
    std::string state;  // Serialized state
    Timestamp timestamp;
};

/// System metric
struct Metric {
    std::string metric_name;
    double value;
    std::unordered_map<std::string, std::string> labels;
    Timestamp timestamp;
};

/// System event
struct Event {
    std::string event_id;
    std::string event_type;
    std::string payload;
    Timestamp timestamp;
};

/// Unified state store backed by MarbleDB
///
/// Features:
/// - Graph state (vertices, edges, indexes, counters)
/// - Agent state (memory, conversations, tool calls)
/// - Workflow state (tasks, checkpoints)
/// - System state (metrics, events)
/// - Fast lookups (5-10 μs)
/// - Zero-copy Arrow reads
/// - ACID transactions
/// - Distributed replication (NuRaft)
class UnifiedStateStore {
public:
    /// Constructor
    /// @param db_path Path to MarbleDB database
    UnifiedStateStore(const std::string& db_path);
    
    ~UnifiedStateStore();
    
    /// Open/create database
    /// @return Status
    arrow::Status Open();
    
    /// Close database
    /// @return Status
    arrow::Status Close();
    
    // ============================================================
    // GRAPH STATE OPERATIONS
    // ============================================================
    
    /// Insert vertex
    /// @param vertex Vertex to insert
    /// @return Status
    arrow::Status InsertVertex(const Vertex& vertex);
    
    /// Insert vertices (batch)
    /// @param vertices Arrow table of vertices
    /// @return Status
    arrow::Status InsertVertices(std::shared_ptr<arrow::Table> vertices);
    
    /// Get vertex by ID (fast: 5-10 μs)
    /// @param vertex_id Vertex ID
    /// @return Vertex data
    arrow::Result<Vertex> GetVertex(int64_t vertex_id);
    
    /// Insert edge
    /// @param edge Edge to insert
    /// @return Status
    arrow::Status InsertEdge(const Edge& edge);
    
    /// Insert edges (batch)
    /// @param edges Arrow table of edges
    /// @return Status
    arrow::Status InsertEdges(std::shared_ptr<arrow::Table> edges);
    
    /// Get edges for vertex (fast: 5-10 μs)
    /// @param vertex_id Vertex ID
    /// @param direction Direction ("out", "in", "both")
    /// @return Edges as Arrow table
    arrow::Result<std::shared_ptr<arrow::Table>> GetEdges(int64_t vertex_id, const std::string& direction);
    
    /// Query graph with Cypher
    /// @param query Cypher query string
    /// @return Result as Arrow table
    arrow::Result<std::shared_ptr<arrow::Table>> QueryGraph(const std::string& query);
    
    /// Increment vertex counter (merge operator)
    /// @param vertex_id Vertex ID
    /// @param counter_name Counter name (e.g., "followers", "degree")
    /// @param delta Increment amount
    /// @return Status
    arrow::Status IncrementCounter(int64_t vertex_id, const std::string& counter_name, int64_t delta);
    
    // ============================================================
    // AGENT STATE OPERATIONS
    // ============================================================
    
    /// Store agent memory
    /// @param memory Agent memory entry
    /// @return Status
    arrow::Status StoreAgentMemory(const AgentMemory& memory);
    
    /// Get agent memories
    /// @param agent_id Agent ID
    /// @param limit Maximum number of memories to return
    /// @return Memories as Arrow table
    arrow::Result<std::shared_ptr<arrow::Table>> GetAgentMemories(const std::string& agent_id, int64_t limit = 100);
    
    /// Store conversation
    /// @param conversation Conversation entry
    /// @return Status
    arrow::Status StoreConversation(const AgentConversation& conversation);
    
    /// Get conversation history
    /// @param agent_id Agent ID
    /// @param limit Maximum number of conversations
    /// @return Conversations as Arrow table
    arrow::Result<std::shared_ptr<arrow::Table>> GetConversationHistory(const std::string& agent_id, int64_t limit = 10);
    
    /// Log tool call
    /// @param tool_call Tool call entry
    /// @return Status
    arrow::Status LogToolCall(const AgentToolCall& tool_call);
    
    /// Get tool call history
    /// @param agent_id Agent ID
    /// @param limit Maximum number of tool calls
    /// @return Tool calls as Arrow table
    arrow::Result<std::shared_ptr<arrow::Table>> GetToolCallHistory(const std::string& agent_id, int64_t limit = 100);
    
    // ============================================================
    // WORKFLOW STATE OPERATIONS
    // ============================================================
    
    /// Create task
    /// @param task Task to create
    /// @return Status
    arrow::Status CreateTask(const Task& task);
    
    /// Update task status
    /// @param task_id Task ID
    /// @param status New status
    /// @param result Optional result
    /// @return Status
    arrow::Status UpdateTaskStatus(const std::string& task_id, TaskStatus status, const std::string& result = "");
    
    /// Get ready tasks (no pending dependencies)
    /// @param workflow_id Workflow ID
    /// @return Ready tasks as Arrow table
    arrow::Result<std::shared_ptr<arrow::Table>> GetReadyTasks(const std::string& workflow_id);
    
    /// Save checkpoint
    /// @param checkpoint Checkpoint to save
    /// @return Status
    arrow::Status SaveCheckpoint(const Checkpoint& checkpoint);
    
    /// Load latest checkpoint
    /// @param workflow_id Workflow ID
    /// @return Latest checkpoint
    arrow::Result<Checkpoint> LoadLatestCheckpoint(const std::string& workflow_id);
    
    // ============================================================
    // SYSTEM STATE OPERATIONS
    // ============================================================
    
    /// Record metric
    /// @param metric Metric to record
    /// @return Status
    arrow::Status RecordMetric(const Metric& metric);
    
    /// Query metrics
    /// @param metric_name Metric name
    /// @param start_time Start of time range
    /// @param end_time End of time range
    /// @return Metrics as Arrow table
    arrow::Result<std::shared_ptr<arrow::Table>> QueryMetrics(
        const std::string& metric_name,
        Timestamp start_time,
        Timestamp end_time);
    
    /// Record event
    /// @param event Event to record
    /// @return Status
    arrow::Status RecordEvent(const Event& event);
    
    /// Query events
    /// @param event_type Event type filter (optional)
    /// @param start_time Start of time range
    /// @param end_time End of time range
    /// @return Events as Arrow table
    arrow::Result<std::shared_ptr<arrow::Table>> QueryEvents(
        const std::string& event_type,
        Timestamp start_time,
        Timestamp end_time);
    
    // ============================================================
    // UNIFIED OPERATIONS
    // ============================================================
    
    /// Unified query across all domains
    /// @param query Cypher query string
    /// @return Result as Arrow table
    arrow::Result<std::shared_ptr<arrow::Table>> Query(const std::string& query);
    
    /// Begin transaction
    /// @return Transaction handle
    arrow::Status BeginTransaction();
    
    /// Commit transaction
    /// @return Status
    arrow::Status CommitTransaction();
    
    /// Rollback transaction
    /// @return Status
    arrow::Status RollbackTransaction();
    
    /// Compact database
    /// @return Status
    arrow::Status Compact();
    
    /// Get statistics
    struct Stats {
        // Graph stats
        int64_t total_vertices;
        int64_t total_edges;
        
        // Agent stats
        int64_t total_memories;
        int64_t total_conversations;
        int64_t total_tool_calls;
        
        // Workflow stats
        int64_t total_tasks;
        int64_t total_checkpoints;
        
        // System stats
        int64_t total_metrics;
        int64_t total_events;
        
        // Storage stats
        int64_t disk_size_bytes;
        int64_t memory_size_bytes;
        int64_t cache_hits;
        int64_t cache_misses;
    };
    
    Stats GetStats() const;

private:
    /// Create column families
    arrow::Status CreateColumnFamilies();
    
    /// Serialize vertex to Arrow record batch
    arrow::Result<std::shared_ptr<arrow::RecordBatch>> SerializeVertex(const Vertex& vertex);
    
    /// Deserialize vertex from Arrow record batch
    arrow::Result<Vertex> DeserializeVertex(std::shared_ptr<arrow::RecordBatch> batch);

private:
    std::string db_path_;
    std::unique_ptr<marble::MarbleDB> db_;
    
    // Column family handles
    marble::ColumnFamilyHandle* graph_vertices_cf_;
    marble::ColumnFamilyHandle* graph_edges_cf_;
    marble::ColumnFamilyHandle* graph_indexes_cf_;
    marble::ColumnFamilyHandle* graph_counters_cf_;
    
    marble::ColumnFamilyHandle* agent_memory_cf_;
    marble::ColumnFamilyHandle* agent_conversations_cf_;
    marble::ColumnFamilyHandle* agent_tool_calls_cf_;
    marble::ColumnFamilyHandle* agent_embeddings_cf_;
    
    marble::ColumnFamilyHandle* workflow_tasks_cf_;
    marble::ColumnFamilyHandle* workflow_checkpoints_cf_;
    marble::ColumnFamilyHandle* workflow_state_cf_;
    
    marble::ColumnFamilyHandle* system_metrics_cf_;
    marble::ColumnFamilyHandle* system_events_cf_;
    marble::ColumnFamilyHandle* system_logs_cf_;
};

} // namespace state
} // namespace sabot_cypher

