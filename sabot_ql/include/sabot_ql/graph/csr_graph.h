/**
 * CSR (Compressed Sparse Row) Graph Representation
 *
 * Efficient graph storage format for property path queries.
 * Based on QLever's approach but implemented with Arrow for zero-copy.
 */

#pragma once

#include <arrow/api.h>
#include <arrow/compute/api.h>
#include <memory>
#include <string>

namespace sabot {
namespace graph {

/**
 * CSR (Compressed Sparse Row) Graph Format
 *
 * Memory-efficient sparse graph representation:
 * - offsets[i] = start index in targets for node i's edges
 * - offsets[i+1] = end index (exclusive)
 * - targets[offsets[i]..offsets[i+1]] = neighbors of node i
 * - edge_ids[offsets[i]..offsets[i+1]] = original edge indices
 *
 * Example:
 *   Graph: 0→1, 0→2, 1→2
 *   offsets:  [0, 2, 3, 3]
 *   targets:  [1, 2, 2]
 *   edge_ids: [0, 1, 2]
 *
 * Properties:
 * - O(1) successor lookup
 * - O(E) space
 * - Cache-friendly (sequential memory)
 * - Zero-copy from Arrow (uses slices)
 */
struct CSRGraph {
    std::shared_ptr<arrow::Int64Array> offsets;   // [num_nodes + 1]
    std::shared_ptr<arrow::Int64Array> targets;   // [num_edges]
    std::shared_ptr<arrow::Int64Array> edge_ids;  // [num_edges]
    int64_t num_nodes;
    int64_t num_edges;

    /**
     * Get successors of a node (zero-copy slice).
     *
     * @param node Node ID
     * @return Array of successor node IDs (empty if node has no edges)
     *
     * Time complexity: O(1)
     * Space complexity: O(1) - returns slice, no copy
     */
    arrow::Result<std::shared_ptr<arrow::Int64Array>> GetSuccessors(int64_t node) const;

    /**
     * Get edge IDs for a node's outgoing edges.
     *
     * @param node Node ID
     * @return Array of original edge indices
     */
    arrow::Result<std::shared_ptr<arrow::Int64Array>> GetEdgeIds(int64_t node) const;

    /**
     * Check if graph is empty.
     */
    bool empty() const {
        return num_nodes == 0 || num_edges == 0;
    }
};

/**
 * Build CSR graph from edge list.
 *
 * @param edges RecordBatch with edge data
 * @param source_col Column name for source nodes
 * @param target_col Column name for target nodes
 * @param pool Arrow memory pool (optional)
 *
 * @return CSR graph structure
 *
 * Time complexity: O(E + V)
 * Space complexity: O(E + V)
 *
 * Example:
 *   edges = RecordBatch{
 *     subject: [0, 0, 1],
 *     object: [1, 2, 2]
 *   }
 *   csr = BuildCSRGraph(edges, "subject", "object")
 */
arrow::Result<CSRGraph> BuildCSRGraph(
    const std::shared_ptr<arrow::RecordBatch>& edges,
    const std::string& source_col,
    const std::string& target_col,
    arrow::MemoryPool* pool = arrow::default_memory_pool()
);

/**
 * Build reverse CSR graph (swap source/target).
 *
 * Used for inverse property paths (^p).
 */
arrow::Result<CSRGraph> BuildReverseCSRGraph(
    const std::shared_ptr<arrow::RecordBatch>& edges,
    const std::string& source_col,
    const std::string& target_col,
    arrow::MemoryPool* pool = arrow::default_memory_pool()
);

}  // namespace graph
}  // namespace sabot
