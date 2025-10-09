#pragma once

#include <arrow/api.h>
#include <arrow/result.h>
#include <memory>
#include <cstdint>

namespace sabot {
namespace graph {
namespace traversal {

/**
 * Result of triangle counting operation.
 */
struct TriangleCountResult {
    std::shared_ptr<arrow::Int64Array> per_vertex_triangles;  // Triangles per vertex
    std::shared_ptr<arrow::DoubleArray> clustering_coefficient;  // Local clustering coefficient
    int64_t total_triangles;  // Total number of triangles in graph
    double global_clustering_coefficient;  // Global clustering coefficient
    int64_t num_vertices;
};

/**
 * Count triangles in an undirected graph using node-iterator method.
 *
 * For each vertex v, counts triangles by checking if pairs of v's neighbors
 * are also connected. This is O(d^2 * V) where d is average degree.
 *
 * The graph is assumed to be undirected (edges in both directions).
 *
 * @param indptr CSR indptr array (length num_vertices + 1)
 * @param indices CSR indices array (neighbor lists)
 * @param num_vertices Number of vertices in graph
 * @return TriangleCountResult with per-vertex and global counts
 */
arrow::Result<TriangleCountResult> CountTriangles(
    const std::shared_ptr<arrow::Int64Array>& indptr,
    const std::shared_ptr<arrow::Int64Array>& indices,
    int64_t num_vertices
);

/**
 * Count triangles in a weighted graph (weights ignored, topology only).
 *
 * @param indptr CSR indptr array
 * @param indices CSR indices array
 * @param weights Edge weights (ignored for triangle counting)
 * @param num_vertices Number of vertices
 * @return TriangleCountResult
 */
arrow::Result<TriangleCountResult> CountTrianglesWeighted(
    const std::shared_ptr<arrow::Int64Array>& indptr,
    const std::shared_ptr<arrow::Int64Array>& indices,
    const std::shared_ptr<arrow::DoubleArray>& weights,
    int64_t num_vertices
);

/**
 * Get vertices with highest triangle counts.
 *
 * @param per_vertex_triangles Triangle counts per vertex
 * @param clustering_coefficient Clustering coefficient per vertex
 * @param k Number of top vertices to return
 * @return RecordBatch with columns: vertex_id, triangle_count, clustering_coefficient
 */
arrow::Result<std::shared_ptr<arrow::RecordBatch>> TopKTriangleCounts(
    const std::shared_ptr<arrow::Int64Array>& per_vertex_triangles,
    const std::shared_ptr<arrow::DoubleArray>& clustering_coefficient,
    int64_t k
);

/**
 * Compute transitivity (global clustering coefficient) of the graph.
 *
 * Transitivity = 3 * (number of triangles) / (number of connected triples)
 *
 * @param indptr CSR indptr array
 * @param indices CSR indices array
 * @param num_vertices Number of vertices
 * @return Transitivity score (0.0 to 1.0)
 */
arrow::Result<double> ComputeTransitivity(
    const std::shared_ptr<arrow::Int64Array>& indptr,
    const std::shared_ptr<arrow::Int64Array>& indices,
    int64_t num_vertices
);

}  // namespace traversal
}  // namespace graph
}  // namespace sabot
