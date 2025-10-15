// MarbleDB Graph Store
//
// High-performance persistent graph store using MarbleDB for streaming queries.

#pragma once

#include <arrow/api.h>
#include <memory>
#include <string>
#include <chrono>

// Forward declare MarbleDB types to avoid circular dependencies
namespace marble {
    class MarbleDB;
    struct DBOptions;
    struct ReadOptions;
    struct WriteOptions;
    class ColumnFamilyHandle;
}

namespace sabot_cypher {
namespace streaming {

using Timestamp = std::chrono::system_clock::time_point;
using TimeDelta = std::chrono::milliseconds;

/// High-performance graph store using MarbleDB
///
/// Features:
/// - Fast vertex/edge lookups (5-10 μs)
/// - Zero-copy Arrow reads
/// - Time-range queries with zone map pruning
/// - Merge operators for incremental updates
/// - Column families for vertices/edges/indexes
/// - Persistent storage with WAL
class MarbleDBGraphStore {
public:
    /// Constructor
    /// @param db_path Path to MarbleDB database
    /// @param ttl Time-to-live for graph data
    MarbleDBGraphStore(const std::string& db_path, 
                       TimeDelta ttl = std::chrono::hours(24));
    
    ~MarbleDBGraphStore();
    
    /// Open/create database
    /// @return Status
    arrow::Status Open();
    
    /// Insert vertices (batched)
    /// @param vertices Arrow table with 'id', 'timestamp', and property columns
    /// @return Status
    arrow::Status InsertVertices(std::shared_ptr<arrow::Table> vertices);
    
    /// Insert edges (batched)
    /// @param edges Arrow table with 'source', 'target', 'timestamp', and property columns
    /// @return Status
    arrow::Status InsertEdges(std::shared_ptr<arrow::Table> edges);
    
    /// Get vertex by ID (fast point lookup)
    /// @param vertex_id Vertex ID
    /// @return Vertex properties as Arrow record
    arrow::Result<std::shared_ptr<arrow::RecordBatch>> GetVertex(int64_t vertex_id);
    
    /// Get edges for vertex (fast lookup)
    /// @param vertex_id Vertex ID
    /// @param direction Direction ('out', 'in', 'both')
    /// @return Edges as Arrow table
    arrow::Result<std::shared_ptr<arrow::Table>> GetEdges(int64_t vertex_id, const std::string& direction);
    
    /// Query vertices in time range (zone map pruning)
    /// @param start_time Start of time range
    /// @param end_time End of time range
    /// @return Vertices table
    arrow::Result<std::shared_ptr<arrow::Table>> QueryVertices(Timestamp start_time, Timestamp end_time);
    
    /// Query edges in time range (zone map pruning)
    /// @param start_time Start of time range
    /// @param end_time End of time range
    /// @return Edges table
    arrow::Result<std::shared_ptr<arrow::Table>> QueryEdges(Timestamp start_time, Timestamp end_time);
    
    /// Increment vertex counter using merge operator
    /// @param vertex_id Vertex ID
    /// @param counter_name Counter name (e.g., "followers")
    /// @param delta Increment amount
    /// @return Status
    arrow::Status IncrementCounter(int64_t vertex_id, const std::string& counter_name, int64_t delta);
    
    /// Compact database
    /// @return Status
    arrow::Status Compact();
    
    /// Get storage statistics
    struct Stats {
        int64_t total_vertices;
        int64_t total_edges;
        int64_t disk_size_bytes;
        int64_t memory_size_bytes;
        int64_t cache_hits;
        int64_t cache_misses;
    };
    
    Stats GetStats() const;

private:
    /// Expire old data beyond TTL
    /// @return Number of expired records
    int64_t ExpireOldData();
    
    /// Create column families
    /// @return Status
    arrow::Status CreateColumnFamilies();

private:
    std::string db_path_;
    TimeDelta ttl_;
    
    // MarbleDB instance
    std::unique_ptr<marble::MarbleDB> db_;
    
    // Column families
    marble::ColumnFamilyHandle* vertices_cf_;
    marble::ColumnFamilyHandle* edges_cf_;
    marble::ColumnFamilyHandle* indexes_cf_;
    marble::ColumnFamilyHandle* counters_cf_;
};

} // namespace streaming
} // namespace sabot_cypher

