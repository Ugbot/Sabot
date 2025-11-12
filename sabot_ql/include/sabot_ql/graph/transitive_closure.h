/**
 * Transitive Closure Algorithms for Property Paths
 *
 * Implements SPARQL 1.1 property path operators:
 * - p+ (one or more hops)
 * - p* (zero or more hops)
 * - p{m,n} (bounded paths)
 *
 * Algorithm borrowed from QLever (TransitivePathImpl.h):
 * - DFS with visited set for cycle detection
 * - Distance tracking for bounded paths
 * - O(V + E) time complexity
 */

#pragma once

#include "sabot_ql/graph/csr_graph.h"
#include <arrow/api.h>
#include <arrow/compute/api.h>
#include <memory>
#include <vector>
#include <stack>

namespace sabot {
namespace graph {

/**
 * Result of transitive closure query.
 *
 * RecordBatch with schema:
 *   start: int64 - Starting node
 *   end: int64   - Reachable node
 *   dist: int64  - Distance (number of hops)
 */
struct TransitiveClosureResult {
    std::shared_ptr<arrow::RecordBatch> result;

    int64_t num_pairs() const {
        return result ? result->num_rows() : 0;
    }
};

/**
 * Compute transitive closure using DFS.
 *
 * Algorithm from QLever (TransitivePathImpl.h:203-238):
 * - Stack-based DFS (no recursion, avoids stack overflow)
 * - Visited bitmap to prevent cycles
 * - Distance tracking for bounded paths
 *
 * @param graph CSR graph to traverse
 * @param start_nodes Array of starting node IDs
 * @param min_dist Minimum distance (1 for p+, 0 for p*)
 * @param max_dist Maximum distance (INT64_MAX for unbounded)
 * @param pool Arrow memory pool
 *
 * @return RecordBatch with (start, end, dist) tuples
 *
 * Time complexity: O(V + E) per start node
 * Space complexity: O(V) for visited set
 *
 * Example:
 *   Graph: 0 → 1 → 2 → 3
 *   Query: transitive_closure(graph, [0], min_dist=1, max_dist=100)
 *   Result: [(0, 1, 1), (0, 2, 2), (0, 3, 3)]
 */
arrow::Result<TransitiveClosureResult> TransitiveClosure(
    const CSRGraph& graph,
    const std::shared_ptr<arrow::Int64Array>& start_nodes,
    int64_t min_dist,
    int64_t max_dist,
    arrow::MemoryPool* pool = arrow::default_memory_pool()
);

/**
 * Optimized version for single start node.
 *
 * Avoids array overhead when querying from single node.
 */
arrow::Result<TransitiveClosureResult> TransitiveClosureSingle(
    const CSRGraph& graph,
    int64_t start_node,
    int64_t min_dist,
    int64_t max_dist,
    arrow::MemoryPool* pool = arrow::default_memory_pool()
);

}  // namespace graph
}  // namespace sabot
