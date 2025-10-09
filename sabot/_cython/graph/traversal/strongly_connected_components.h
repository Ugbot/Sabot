#pragma once

#include <arrow/api.h>
#include <arrow/result.h>
#include <memory>
#include <cstdint>

namespace sabot {
namespace graph {
namespace traversal {

/**
 * Result of strongly connected components computation.
 */
struct StronglyConnectedComponentsResult {
    std::shared_ptr<arrow::Int64Array> component_ids;  // Component ID per vertex
    int64_t num_components;                            // Total number of SCCs
    std::shared_ptr<arrow::Int64Array> component_sizes;  // Size of each SCC
    int64_t largest_component_size;                    // Size of largest SCC
    int64_t num_vertices;
};

/**
 * Find strongly connected components using Tarjan's algorithm.
 *
 * A strongly connected component (SCC) is a maximal set of vertices where
 * every vertex is reachable from every other vertex in the set.
 *
 * Tarjan's algorithm uses DFS with discovery time and low-link values.
 * Time complexity: O(V + E)
 * Space complexity: O(V)
 *
 * @param indptr CSR indptr array (length num_vertices + 1)
 * @param indices CSR indices array (neighbor lists)
 * @param num_vertices Number of vertices in graph
 * @return StronglyConnectedComponentsResult with SCC assignments
 */
arrow::Result<StronglyConnectedComponentsResult> FindStronglyConnectedComponents(
    const std::shared_ptr<arrow::Int64Array>& indptr,
    const std::shared_ptr<arrow::Int64Array>& indices,
    int64_t num_vertices
);

/**
 * Find strongly connected components and return them as a list of vertex sets.
 *
 * @param indptr CSR indptr array
 * @param indices CSR indices array
 * @param num_vertices Number of vertices
 * @return RecordBatch with columns: component_id, vertex_id
 */
arrow::Result<std::shared_ptr<arrow::RecordBatch>> GetSCCMembership(
    const std::shared_ptr<arrow::Int64Array>& indptr,
    const std::shared_ptr<arrow::Int64Array>& indices,
    int64_t num_vertices
);

/**
 * Get vertices in the largest strongly connected component.
 *
 * @param component_ids Component ID per vertex
 * @param component_sizes Size of each component
 * @return Array of vertex IDs in largest SCC
 */
arrow::Result<std::shared_ptr<arrow::Int64Array>> GetLargestSCC(
    const std::shared_ptr<arrow::Int64Array>& component_ids,
    const std::shared_ptr<arrow::Int64Array>& component_sizes
);

/**
 * Check if graph is strongly connected (single SCC).
 *
 * @param indptr CSR indptr array
 * @param indices CSR indices array
 * @param num_vertices Number of vertices
 * @return true if graph is strongly connected, false otherwise
 */
arrow::Result<bool> IsStronglyConnected(
    const std::shared_ptr<arrow::Int64Array>& indptr,
    const std::shared_ptr<arrow::Int64Array>& indices,
    int64_t num_vertices
);

/**
 * Condense graph into DAG of SCCs.
 *
 * Each SCC becomes a single vertex in the condensed graph.
 * Edges between SCCs are preserved.
 *
 * @param indptr CSR indptr array
 * @param indices CSR indices array
 * @param num_vertices Number of vertices
 * @return Pair of (condensed_indptr, condensed_indices) for SCC DAG
 */
arrow::Result<std::pair<std::shared_ptr<arrow::Int64Array>, std::shared_ptr<arrow::Int64Array>>>
CondenseGraph(
    const std::shared_ptr<arrow::Int64Array>& indptr,
    const std::shared_ptr<arrow::Int64Array>& indices,
    int64_t num_vertices
);

/**
 * Find source SCCs (no incoming edges from other SCCs).
 *
 * @param component_ids Component ID per vertex
 * @param indptr CSR indptr array
 * @param indices CSR indices array
 * @param num_vertices Number of vertices
 * @return Array of SCC IDs that are sources
 */
arrow::Result<std::shared_ptr<arrow::Int64Array>> FindSourceSCCs(
    const std::shared_ptr<arrow::Int64Array>& component_ids,
    const std::shared_ptr<arrow::Int64Array>& indptr,
    const std::shared_ptr<arrow::Int64Array>& indices,
    int64_t num_vertices
);

/**
 * Find sink SCCs (no outgoing edges to other SCCs).
 *
 * @param component_ids Component ID per vertex
 * @param indptr CSR indptr array
 * @param indices CSR indices array
 * @param num_vertices Number of vertices
 * @return Array of SCC IDs that are sinks
 */
arrow::Result<std::shared_ptr<arrow::Int64Array>> FindSinkSCCs(
    const std::shared_ptr<arrow::Int64Array>& component_ids,
    const std::shared_ptr<arrow::Int64Array>& indptr,
    const std::shared_ptr<arrow::Int64Array>& indices,
    int64_t num_vertices
);

}  // namespace traversal
}  // namespace graph
}  // namespace sabot
