// Temporal Graph Store Implementation

#include "sabot_cypher/streaming/temporal_graph_store.h"
#include <arrow/compute/api.h>
#include <algorithm>

namespace sabot_cypher {
namespace streaming {

TemporalGraphStore::TemporalGraphStore(TimeDelta ttl, TimeDelta bucket_size)
    : ttl_(ttl), bucket_size_(bucket_size) {}

BucketID TemporalGraphStore::GetBucketID(Timestamp timestamp) const {
    auto epoch = std::chrono::system_clock::from_time_t(0);
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(timestamp - epoch);
    int64_t bucket_size_ms = bucket_size_.count();
    return duration.count() / bucket_size_ms;
}

std::vector<BucketID> TemporalGraphStore::GetBucketRange(Timestamp start_time, Timestamp end_time) const {
    BucketID start_bucket = GetBucketID(start_time);
    BucketID end_bucket = GetBucketID(end_time);
    
    std::vector<BucketID> buckets;
    for (BucketID b = start_bucket; b <= end_bucket; ++b) {
        buckets.push_back(b);
    }
    return buckets;
}

arrow::Status TemporalGraphStore::InsertVertices(std::shared_ptr<arrow::Table> vertices) {
    if (!vertices) {
        return arrow::Status::Invalid("Vertices table is null");
    }
    
    // Check for timestamp column
    auto timestamp_col = vertices->GetColumnByName("timestamp");
    if (!timestamp_col) {
        return arrow::Status::Invalid("Vertices table must have 'timestamp' column");
    }
    
    // Partition by time bucket
    // For now, insert entire table into current bucket
    // TODO: Optimize by partitioning rows by bucket
    Timestamp now = std::chrono::system_clock::now();
    BucketID bucket_id = GetBucketID(now);
    
    if (vertices_.count(bucket_id) > 0) {
        // Concatenate with existing data
        ARROW_ASSIGN_OR_RAISE(auto concatenated,
            arrow::ConcatenateTables({vertices_[bucket_id], vertices}));
        vertices_[bucket_id] = concatenated;
    } else {
        vertices_[bucket_id] = vertices;
    }
    
    // Update vertex index
    auto id_col = vertices->GetColumnByName("id");
    if (id_col) {
        auto id_array = std::static_pointer_cast<arrow::Int64Array>(id_col->chunk(0));
        for (int64_t i = 0; i < id_array->length(); ++i) {
            if (!id_array->IsNull(i)) {
                int64_t vertex_id = id_array->Value(i);
                vertex_index_[vertex_id].push_back(bucket_id);
            }
        }
    }
    
    return arrow::Status::OK();
}

arrow::Status TemporalGraphStore::InsertEdges(std::shared_ptr<arrow::Table> edges) {
    if (!edges) {
        return arrow::Status::Invalid("Edges table is null");
    }
    
    // Check for timestamp column
    auto timestamp_col = edges->GetColumnByName("timestamp");
    if (!timestamp_col) {
        return arrow::Status::Invalid("Edges table must have 'timestamp' column");
    }
    
    // Partition by time bucket
    // For now, insert entire table into current bucket
    Timestamp now = std::chrono::system_clock::now();
    BucketID bucket_id = GetBucketID(now);
    
    if (edges_.count(bucket_id) > 0) {
        // Concatenate with existing data
        ARROW_ASSIGN_OR_RAISE(auto concatenated,
            arrow::ConcatenateTables({edges_[bucket_id], edges}));
        edges_[bucket_id] = concatenated;
    } else {
        edges_[bucket_id] = edges;
    }
    
    return arrow::Status::OK();
}

arrow::Result<std::shared_ptr<arrow::Table>> 
TemporalGraphStore::ConcatenateTables(
    const std::vector<BucketID>& bucket_ids,
    const std::unordered_map<BucketID, std::shared_ptr<arrow::Table>>& buckets) const {
    
    std::vector<std::shared_ptr<arrow::Table>> tables;
    
    for (BucketID bucket_id : bucket_ids) {
        auto it = buckets.find(bucket_id);
        if (it != buckets.end()) {
            tables.push_back(it->second);
        }
    }
    
    if (tables.empty()) {
        // Return empty table
        return nullptr;
    }
    
    return arrow::ConcatenateTables(tables);
}

arrow::Result<std::shared_ptr<arrow::Table>>
TemporalGraphStore::FilterByTimeRange(
    std::shared_ptr<arrow::Table> table,
    Timestamp start_time,
    Timestamp end_time) const {
    
    if (!table || table->num_rows() == 0) {
        return table;
    }
    
    auto timestamp_col = table->GetColumnByName("timestamp");
    if (!timestamp_col) {
        return table; // No filtering if no timestamp column
    }
    
    // Convert timestamps to int64 for comparison
    auto start_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        start_time.time_since_epoch()).count();
    auto end_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        end_time.time_since_epoch()).count();
    
    // Filter using Arrow Compute
    // For now, return unfiltered (filtering logic can be added)
    return table;
}

arrow::Result<std::pair<std::shared_ptr<arrow::Table>, std::shared_ptr<arrow::Table>>>
TemporalGraphStore::Query(Timestamp start_time, Timestamp end_time) {
    // Get bucket range
    auto bucket_ids = GetBucketRange(start_time, end_time);
    
    // Gather vertices
    ARROW_ASSIGN_OR_RAISE(auto vertices, ConcatenateTables(bucket_ids, vertices_));
    
    // Gather edges
    ARROW_ASSIGN_OR_RAISE(auto edges, ConcatenateTables(bucket_ids, edges_));
    
    // Filter by exact time range
    if (vertices) {
        ARROW_ASSIGN_OR_RAISE(vertices, FilterByTimeRange(vertices, start_time, end_time));
    }
    
    if (edges) {
        ARROW_ASSIGN_OR_RAISE(edges, FilterByTimeRange(edges, start_time, end_time));
    }
    
    // Create empty tables if null
    if (!vertices) {
        auto schema = arrow::schema({
            arrow::field("id", arrow::int64()),
            arrow::field("timestamp", arrow::timestamp(arrow::TimeUnit::MILLI))
        });
        std::vector<std::shared_ptr<arrow::Array>> arrays;
        vertices = arrow::Table::Make(schema, arrays);
    }
    
    if (!edges) {
        auto schema = arrow::schema({
            arrow::field("source", arrow::int64()),
            arrow::field("target", arrow::int64()),
            arrow::field("timestamp", arrow::timestamp(arrow::TimeUnit::MILLI))
        });
        std::vector<std::shared_ptr<arrow::Array>> arrays;
        edges = arrow::Table::Make(schema, arrays);
    }
    
    return std::make_pair(vertices, edges);
}

int64_t TemporalGraphStore::ExpireOldBuckets() {
    Timestamp now = std::chrono::system_clock::now();
    Timestamp expiry_time = now - ttl_;
    BucketID expiry_bucket = GetBucketID(expiry_time);
    
    int64_t expired_count = 0;
    
    // Remove old vertex buckets
    for (auto it = vertices_.begin(); it != vertices_.end();) {
        if (it->first < expiry_bucket) {
            it = vertices_.erase(it);
            ++expired_count;
        } else {
            ++it;
        }
    }
    
    // Remove old edge buckets
    for (auto it = edges_.begin(); it != edges_.end();) {
        if (it->first < expiry_bucket) {
            it = edges_.erase(it);
            ++expired_count;
        } else {
            ++it;
        }
    }
    
    // Clean up vertex index
    for (auto it = vertex_index_.begin(); it != vertex_index_.end();) {
        auto& buckets = it->second;
        buckets.erase(
            std::remove_if(buckets.begin(), buckets.end(),
                [expiry_bucket](BucketID b) { return b < expiry_bucket; }),
            buckets.end()
        );
        
        if (buckets.empty()) {
            it = vertex_index_.erase(it);
        } else {
            ++it;
        }
    }
    
    return expired_count;
}

TemporalGraphStore::Stats TemporalGraphStore::GetStats() const {
    Stats stats;
    stats.num_vertex_buckets = vertices_.size();
    stats.num_edge_buckets = edges_.size();
    
    stats.total_vertices = 0;
    for (const auto& [bucket_id, table] : vertices_) {
        stats.total_vertices += table->num_rows();
    }
    
    stats.total_edges = 0;
    for (const auto& [bucket_id, table] : edges_) {
        stats.total_edges += table->num_rows();
    }
    
    stats.indexed_vertices = vertex_index_.size();
    
    return stats;
}

} // namespace streaming
} // namespace sabot_cypher

