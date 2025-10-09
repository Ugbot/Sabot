#pragma once

#include <arrow/api.h>
#include <arrow/result.h>
#include <memory>
#include <cstdint>

namespace sabot {
namespace graph {
namespace traversal {

/**
 * Betweenness centrality result.
 */
struct BetweennessCentralityResult {
    std::shared_ptr<arrow::DoubleArray> centrality;  // Betweenness scores for each vertex
    int64_t num_vertices;                            // Total vertices
    bool normalized;                                 // True if scores are normalized
};

/**
 * Compute betweenness centrality using Brandes' algorithm.
 *
 * Betweenness centrality measures how often a vertex lies on shortest paths
 * between other vertices. High betweenness indicates important connectors/bridges.
 *
 * Formula:
 *   BC(v) = sum_{s≠v≠t} (σ_st(v) / σ_st)
 * where:
 *   σ_st = number of shortest paths from s to t
 *   σ_st(v) = number of those paths that pass through v
 *
 * Args:
 *   indptr: CSR indptr array (neighbor offsets)
 *   indices: CSR indices array (neighbor IDs)
 *   num_vertices: Total number of vertices
 *   normalized: If true, normalize scores by (n-1)(n-2)/2 for undirected graphs
 *
 * Returns:
 *   BetweennessCentralityResult with centrality scores
 *
 * Performance: O(VE) time, O(V + E) space
 *
 * Note: This is the unweighted version using BFS. For weighted graphs, use
 *       Dijkstra-based betweenness (more expensive: O(VE + V² log V)).
 */
arrow::Result<BetweennessCentralityResult> BetweennessCentrality(
    const std::shared_ptr<arrow::Int64Array>& indptr,
    const std::shared_ptr<arrow::Int64Array>& indices,
    int64_t num_vertices,
    bool normalized = true
);

/**
 * Compute weighted betweenness centrality (Dijkstra-based).
 *
 * Like betweenness centrality, but uses edge weights for shortest path calculations.
 *
 * Args:
 *   indptr: CSR indptr array
 *   indices: CSR indices array
 *   weights: Edge weights (parallel to indices, must be >= 0)
 *   num_vertices: Total number of vertices
 *   normalized: If true, normalize scores
 *
 * Returns:
 *   BetweennessCentralityResult with weighted centrality scores
 *
 * Performance: O(VE + V² log V) time, O(V + E) space
 */
arrow::Result<BetweennessCentralityResult> WeightedBetweennessCentrality(
    const std::shared_ptr<arrow::Int64Array>& indptr,
    const std::shared_ptr<arrow::Int64Array>& indices,
    const std::shared_ptr<arrow::DoubleArray>& weights,
    int64_t num_vertices,
    bool normalized = true
);

/**
 * Compute edge betweenness centrality.
 *
 * Measures how often each edge lies on shortest paths.
 * Useful for detecting community boundaries (edges with high betweenness
 * connect communities).
 *
 * Args:
 *   indptr: CSR indptr array
 *   indices: CSR indices array
 *   num_vertices: Total number of vertices
 *
 * Returns:
 *   RecordBatch with columns:
 *     - src: int64 (source vertex)
 *     - dst: int64 (destination vertex)
 *     - betweenness: double (edge betweenness score)
 *
 * Performance: O(VE) time, O(E) space
 */
arrow::Result<std::shared_ptr<arrow::RecordBatch>> EdgeBetweennessCentrality(
    const std::shared_ptr<arrow::Int64Array>& indptr,
    const std::shared_ptr<arrow::Int64Array>& indices,
    int64_t num_vertices
);

/**
 * Get top-K vertices by betweenness centrality.
 *
 * Returns the K vertices with highest betweenness scores.
 *
 * Args:
 *   centrality: Betweenness scores from BetweennessCentrality()
 *   k: Number of top vertices to return
 *
 * Returns:
 *   RecordBatch with columns:
 *     - vertex_id: int64 (vertex ID)
 *     - betweenness: double (betweenness score)
 */
arrow::Result<std::shared_ptr<arrow::RecordBatch>> TopKBetweenness(
    const std::shared_ptr<arrow::DoubleArray>& centrality,
    int64_t k
);

}  // namespace traversal
}  // namespace graph
}  // namespace sabot
