#pragma once

#include <arrow/api.h>
#include <arrow/result.h>
#include <memory>
#include <cstdint>

namespace sabot {
namespace graph {
namespace traversal {

/**
 * PageRank result containing vertex ranks and metadata.
 */
struct PageRankResult {
    std::shared_ptr<arrow::DoubleArray> ranks;        // PageRank scores for each vertex
    int64_t num_vertices;                             // Total vertices
    int64_t num_iterations;                           // Iterations until convergence
    double final_delta;                               // Final max delta (convergence metric)
    bool converged;                                   // True if converged before max iterations
};

/**
 * Compute PageRank using power iteration method.
 *
 * PageRank measures vertex importance based on incoming edges:
 *   rank[v] = (1-damping)/N + damping * sum(rank[u] / out_degree[u]) for all u→v
 *
 * Args:
 *   indptr: CSR indptr array (neighbor offsets)
 *   indices: CSR indices array (neighbor IDs)
 *   num_vertices: Total number of vertices
 *   damping: Damping factor (typically 0.85)
 *   max_iterations: Maximum iterations (default 100)
 *   tolerance: Convergence tolerance (default 1e-6)
 *
 * Returns:
 *   PageRankResult with ranks and convergence info
 *
 * Performance: O(E * iterations) time, O(V) space
 */
arrow::Result<PageRankResult> PageRank(
    const std::shared_ptr<arrow::Int64Array>& indptr,
    const std::shared_ptr<arrow::Int64Array>& indices,
    int64_t num_vertices,
    double damping = 0.85,
    int64_t max_iterations = 100,
    double tolerance = 1e-6
);

/**
 * Compute Personalized PageRank from a set of source vertices.
 *
 * Like PageRank, but random jumps go to source vertices instead of all vertices.
 * Useful for topic-specific importance or recommendations.
 *
 * Args:
 *   indptr: CSR indptr array
 *   indices: CSR indices array
 *   num_vertices: Total number of vertices
 *   source_vertices: Source vertex IDs for personalized jumps
 *   damping: Damping factor
 *   max_iterations: Maximum iterations
 *   tolerance: Convergence tolerance
 *
 * Returns:
 *   PageRankResult with personalized ranks
 *
 * Performance: O(E * iterations) time, O(V) space
 */
arrow::Result<PageRankResult> PersonalizedPageRank(
    const std::shared_ptr<arrow::Int64Array>& indptr,
    const std::shared_ptr<arrow::Int64Array>& indices,
    int64_t num_vertices,
    const std::shared_ptr<arrow::Int64Array>& source_vertices,
    double damping = 0.85,
    int64_t max_iterations = 100,
    double tolerance = 1e-6
);

/**
 * Compute weighted PageRank with edge weights.
 *
 * Edge weights influence how much rank flows along each edge.
 * Useful for graphs where edges have varying importance.
 *
 * Args:
 *   indptr: CSR indptr array
 *   indices: CSR indices array
 *   weights: Edge weights (parallel to indices)
 *   num_vertices: Total number of vertices
 *   damping: Damping factor
 *   max_iterations: Maximum iterations
 *   tolerance: Convergence tolerance
 *
 * Returns:
 *   PageRankResult with weighted ranks
 *
 * Performance: O(E * iterations) time, O(V) space
 */
arrow::Result<PageRankResult> WeightedPageRank(
    const std::shared_ptr<arrow::Int64Array>& indptr,
    const std::shared_ptr<arrow::Int64Array>& indices,
    const std::shared_ptr<arrow::DoubleArray>& weights,
    int64_t num_vertices,
    double damping = 0.85,
    int64_t max_iterations = 100,
    double tolerance = 1e-6
);

/**
 * Get top-K vertices by PageRank score.
 *
 * Returns the K highest-ranked vertices in descending order.
 *
 * Args:
 *   ranks: PageRank scores from PageRank()
 *   k: Number of top vertices to return
 *
 * Returns:
 *   RecordBatch with columns:
 *     - vertex_id: int64 (vertex ID)
 *     - rank: double (PageRank score)
 */
arrow::Result<std::shared_ptr<arrow::RecordBatch>> TopKPageRank(
    const std::shared_ptr<arrow::DoubleArray>& ranks,
    int64_t k
);

}  // namespace traversal
}  // namespace graph
}  // namespace sabot
