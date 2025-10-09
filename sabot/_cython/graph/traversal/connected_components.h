#pragma once

#include <arrow/api.h>
#include <arrow/result.h>
#include <memory>
#include <cstdint>

namespace sabot {
namespace graph {
namespace traversal {

/**
 * Result of connected components computation.
 */
struct ConnectedComponentsResult {
    std::shared_ptr<arrow::Int64Array> component_ids;  // Component ID per vertex
    int64_t num_components;                            // Total number of components
    std::shared_ptr<arrow::Int64Array> component_sizes;  // Size of each component
    int64_t largest_component_size;                    // Size of largest component
    int64_t num_vertices;
};

/**
 * Find connected components in an undirected graph using union-find.
 *
 * Uses path compression and union by rank for O(α(n)) amortized time per operation,
 * where α is the inverse Ackermann function (effectively constant).
 *
 * @param indptr CSR indptr array (length num_vertices + 1)
 * @param indices CSR indices array (neighbor lists)
 * @param num_vertices Number of vertices in graph
 * @return ConnectedComponentsResult with component assignments
 */
arrow::Result<ConnectedComponentsResult> FindConnectedComponents(
    const std::shared_ptr<arrow::Int64Array>& indptr,
    const std::shared_ptr<arrow::Int64Array>& indices,
    int64_t num_vertices
);

/**
 * Find connected components using BFS (alternative to union-find).
 *
 * Useful for directed graphs treated as undirected.
 *
 * @param indptr CSR indptr array
 * @param indices CSR indices array
 * @param num_vertices Number of vertices
 * @return ConnectedComponentsResult
 */
arrow::Result<ConnectedComponentsResult> FindConnectedComponentsBFS(
    const std::shared_ptr<arrow::Int64Array>& indptr,
    const std::shared_ptr<arrow::Int64Array>& indices,
    int64_t num_vertices
);

/**
 * Get vertices in the largest connected component.
 *
 * @param component_ids Component ID per vertex
 * @param component_sizes Size of each component
 * @return Array of vertex IDs in largest component
 */
arrow::Result<std::shared_ptr<arrow::Int64Array>> GetLargestComponent(
    const std::shared_ptr<arrow::Int64Array>& component_ids,
    const std::shared_ptr<arrow::Int64Array>& component_sizes
);

/**
 * Get statistics about connected components.
 *
 * @param component_ids Component ID per vertex
 * @param num_components Number of components
 * @return RecordBatch with columns: component_id, size, isolated (bool)
 */
arrow::Result<std::shared_ptr<arrow::RecordBatch>> ComponentStatistics(
    const std::shared_ptr<arrow::Int64Array>& component_ids,
    int64_t num_components
);

/**
 * Count number of isolated vertices (components of size 1).
 *
 * @param component_sizes Size of each component
 * @return Number of isolated vertices
 */
arrow::Result<int64_t> CountIsolatedVertices(
    const std::shared_ptr<arrow::Int64Array>& component_sizes
);

/**
 * Find weakly connected components in a directed graph.
 *
 * Weakly connected components ignore edge direction - treat the graph
 * as undirected. This is done by symmetrizing the edge list.
 *
 * @param indptr CSR indptr array (directed edges)
 * @param indices CSR indices array
 * @param num_vertices Number of vertices
 * @return ConnectedComponentsResult with component assignments
 */
arrow::Result<ConnectedComponentsResult> WeaklyConnectedComponents(
    const std::shared_ptr<arrow::Int64Array>& indptr,
    const std::shared_ptr<arrow::Int64Array>& indices,
    int64_t num_vertices
);

/**
 * Symmetrize a directed graph by adding reverse edges.
 *
 * For each edge (u, v), ensures that (v, u) also exists.
 * Returns new CSR structure with bidirectional edges.
 *
 * @param indptr Original CSR indptr
 * @param indices Original CSR indices
 * @param num_vertices Number of vertices
 * @return Pair of (new_indptr, new_indices) for symmetrized graph
 */
arrow::Result<std::pair<std::shared_ptr<arrow::Int64Array>, std::shared_ptr<arrow::Int64Array>>>
SymmetrizeGraph(
    const std::shared_ptr<arrow::Int64Array>& indptr,
    const std::shared_ptr<arrow::Int64Array>& indices,
    int64_t num_vertices
);

}  // namespace traversal
}  // namespace graph
}  // namespace sabot
