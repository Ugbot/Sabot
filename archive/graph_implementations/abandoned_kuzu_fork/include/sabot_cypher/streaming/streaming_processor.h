// Streaming Graph Processor
//
// High-performance streaming graph query processor with continuous query execution.

#pragma once

#include "sabot_cypher/streaming/temporal_graph_store.h"
#include "sabot_cypher/execution/arrow_executor.h"
#include <arrow/api.h>
#include <memory>
#include <vector>
#include <functional>
#include <string>

namespace sabot_cypher {
namespace streaming {

/// Callback function for continuous query results
using ResultCallback = std::function<void(std::shared_ptr<arrow::Table>)>;

/// Continuous query registration
struct ContinuousQuery {
    std::string query_id;
    std::string query_string;
    ResultCallback callback;
    
    // Statistics
    int64_t execution_count = 0;
    double total_time_ms = 0.0;
    int64_t error_count = 0;
};

/// Streaming graph processor with continuous query execution
class StreamingGraphProcessor {
public:
    /// Constructor
    /// @param window_size Size of query time window in milliseconds
    /// @param slide_interval How often to slide window in milliseconds
    /// @param ttl Time-to-live for graph data in milliseconds
    StreamingGraphProcessor(
        TimeDelta window_size = std::chrono::minutes(5),
        TimeDelta slide_interval = std::chrono::seconds(30),
        TimeDelta ttl = std::chrono::hours(1));
    
    ~StreamingGraphProcessor() = default;
    
    /// Ingest a batch of vertices and edges
    /// @param vertices Arrow table of vertices (can be nullptr)
    /// @param edges Arrow table of edges (can be nullptr)
    /// @return Status
    arrow::Status IngestBatch(std::shared_ptr<arrow::Table> vertices,
                              std::shared_ptr<arrow::Table> edges);
    
    /// Register a continuous query
    /// @param query_id Unique identifier for the query
    /// @param query Cypher query string
    /// @param callback Function to call with results
    /// @return Status
    arrow::Status RegisterContinuousQuery(
        const std::string& query_id,
        const std::string& query,
        ResultCallback callback);
    
    /// Execute query on current window
    /// @param query Cypher query string
    /// @return Query result table
    arrow::Result<std::shared_ptr<arrow::Table>> QueryCurrentWindow(const std::string& query);
    
    /// Get current windowed graph
    /// @return Pair of (vertices, edges) in current window
    arrow::Result<std::pair<std::shared_ptr<arrow::Table>, std::shared_ptr<arrow::Table>>>
    GetCurrentGraph();
    
    /// Get processor statistics
    struct Stats {
        TemporalGraphStore::Stats store_stats;
        int64_t window_count;
        int64_t continuous_queries;
        TimeDelta window_size;
        TimeDelta slide_interval;
    };
    
    Stats GetStats() const;

private:
    /// Check if window should slide
    bool ShouldSlide() const;
    
    /// Slide window and execute continuous queries
    void OnWindowSlide();
    
    /// Execute continuous queries on current window
    void ExecuteContinuousQueries();

private:
    std::shared_ptr<TemporalGraphStore> temporal_store_;
    std::shared_ptr<execution::ArrowExecutor> executor_;
    
    // Window management
    TimeDelta window_size_;
    TimeDelta slide_interval_;
    Timestamp current_window_end_;
    int64_t window_count_;
    
    // Continuous queries
    std::vector<ContinuousQuery> continuous_queries_;
};

} // namespace streaming
} // namespace sabot_cypher

