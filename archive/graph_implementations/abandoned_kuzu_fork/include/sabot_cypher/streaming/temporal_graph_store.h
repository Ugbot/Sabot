// Temporal Graph Store
//
// High-performance in-memory temporal graph store with time-based indexing.

#pragma once

#include <arrow/api.h>
#include <arrow/compute/api.h>
#include <memory>
#include <unordered_map>
#include <vector>
#include <chrono>
#include <cstdint>

namespace sabot_cypher {
namespace streaming {

using Timestamp = std::chrono::system_clock::time_point;
using TimeDelta = std::chrono::milliseconds;

// Time bucket ID (seconds since epoch / bucket_size)
using BucketID = int64_t;

/// Temporal graph store with time-bucketed storage
class TemporalGraphStore {
public:
    /// Constructor
    /// @param ttl Time-to-live for graph data
    /// @param bucket_size Size of time buckets in milliseconds
    TemporalGraphStore(TimeDelta ttl = std::chrono::hours(1), 
                       TimeDelta bucket_size = std::chrono::minutes(5));
    
    ~TemporalGraphStore() = default;
    
    /// Insert vertices with timestamps
    /// @param vertices Arrow table with 'id' and 'timestamp' columns
    /// @return Status
    arrow::Status InsertVertices(std::shared_ptr<arrow::Table> vertices);
    
    /// Insert edges with timestamps
    /// @param edges Arrow table with 'source', 'target', and 'timestamp' columns
    /// @return Status
    arrow::Status InsertEdges(std::shared_ptr<arrow::Table> edges);
    
    /// Query graph within time range
    /// @param start_time Start of time range
    /// @param end_time End of time range
    /// @return Pair of (vertices, edges) tables
    arrow::Result<std::pair<std::shared_ptr<arrow::Table>, std::shared_ptr<arrow::Table>>>
    Query(Timestamp start_time, Timestamp end_time);
    
    /// Expire old buckets beyond TTL
    /// @return Number of expired buckets
    int64_t ExpireOldBuckets();
    
    /// Get storage statistics
    struct Stats {
        int64_t num_vertex_buckets;
        int64_t num_edge_buckets;
        int64_t total_vertices;
        int64_t total_edges;
        int64_t indexed_vertices;
    };
    
    Stats GetStats() const;

private:
    /// Get bucket ID for timestamp
    BucketID GetBucketID(Timestamp timestamp) const;
    
    /// Get bucket IDs in time range
    std::vector<BucketID> GetBucketRange(Timestamp start_time, Timestamp end_time) const;
    
    /// Concatenate tables from multiple buckets
    arrow::Result<std::shared_ptr<arrow::Table>> 
    ConcatenateTables(const std::vector<BucketID>& bucket_ids, 
                      const std::unordered_map<BucketID, std::shared_ptr<arrow::Table>>& buckets) const;
    
    /// Filter table by time range
    arrow::Result<std::shared_ptr<arrow::Table>>
    FilterByTimeRange(std::shared_ptr<arrow::Table> table, 
                      Timestamp start_time, 
                      Timestamp end_time) const;

private:
    TimeDelta ttl_;
    TimeDelta bucket_size_;
    
    // Time-bucketed storage
    std::unordered_map<BucketID, std::shared_ptr<arrow::Table>> vertices_;
    std::unordered_map<BucketID, std::shared_ptr<arrow::Table>> edges_;
    
    // Vertex ID -> Bucket IDs index
    std::unordered_map<int64_t, std::vector<BucketID>> vertex_index_;
};

} // namespace streaming
} // namespace sabot_cypher

