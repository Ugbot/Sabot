// shortest_paths.h - Shortest Paths Algorithms Header
//
// Arrow-native shortest path algorithms for weighted and unweighted graphs.
//
// Performance: Dijkstra O((V + E) log V), BFS O(V + E) for unweighted
// Memory: Zero-copy Arrow arrays

#pragma once

#include <arrow/api.h>
#include <memory>

namespace sabot {
namespace graph {
namespace traversal {

// Shortest paths result structure
struct ShortestPathsResult {
  std::shared_ptr<arrow::DoubleArray> distances;     // Distance from source to each vertex
  std::shared_ptr<arrow::Int64Array> predecessors;   // Parent in shortest path tree
  int64_t source;                                    // Source vertex
  int64_t num_reached;                               // Number of reachable vertices
};

// ============================================================================
// Dijkstra's Algorithm (Weighted Graphs)
// ============================================================================

/**
 * Dijkstra's single-source shortest paths for weighted graphs.
 *
 * Computes shortest paths from source to all reachable vertices.
 * Uses min-priority queue (binary heap) for efficiency.
 *
 * @param indptr CSR indptr array (offsets for each vertex's neighbors)
 * @param indices CSR indices array (neighbor vertex IDs)
 * @param weights Edge weights (parallel to indices, must be >= 0)
 * @param num_vertices Total number of vertices in graph
 * @param source Source vertex ID to start from
 * @return ShortestPathsResult with distances and predecessors
 *
 * Requirements:
 * - All weights must be non-negative (>= 0)
 * - weights.length() == indices.length()
 *
 * Performance: O((V + E) log V) time, O(V) space
 *
 * Example:
 *     auto result = Dijkstra(indptr, indices, weights, num_vertices, 0);
 *     // result.distances[v] = shortest distance from source to v
 *     // result.predecessors[v] = parent of v in shortest path tree
 */
arrow::Result<ShortestPathsResult> Dijkstra(
    std::shared_ptr<arrow::Int64Array> indptr,
    std::shared_ptr<arrow::Int64Array> indices,
    std::shared_ptr<arrow::DoubleArray> weights,
    int64_t num_vertices,
    int64_t source);

/**
 * Unweighted shortest paths (BFS-based, all edges have weight 1).
 *
 * Faster than Dijkstra for unweighted graphs.
 *
 * @param indptr CSR indptr array
 * @param indices CSR indices array
 * @param num_vertices Total number of vertices
 * @param source Source vertex ID
 * @return ShortestPathsResult with distances (as doubles) and predecessors
 *
 * Performance: O(V + E) time, O(V) space
 */
arrow::Result<ShortestPathsResult> UnweightedShortestPaths(
    std::shared_ptr<arrow::Int64Array> indptr,
    std::shared_ptr<arrow::Int64Array> indices,
    int64_t num_vertices,
    int64_t source);

/**
 * Single shortest path reconstruction from source to target.
 *
 * Uses predecessors from ShortestPathsResult to reconstruct the actual path.
 *
 * @param predecessors Predecessor array from shortest paths result
 * @param source Source vertex ID
 * @param target Target vertex ID
 * @return Int64Array with path vertices [source, ..., target]
 *         Empty array if target is unreachable
 *
 * Performance: O(path_length) time, O(path_length) space
 */
arrow::Result<std::shared_ptr<arrow::Int64Array>> ReconstructPath(
    std::shared_ptr<arrow::Int64Array> predecessors,
    int64_t source,
    int64_t target);

/**
 * All-pairs shortest paths (Floyd-Warshall algorithm).
 *
 * Computes shortest paths between all pairs of vertices.
 * WARNING: O(V^3) time complexity - only suitable for small graphs (<500 vertices).
 *
 * @param indptr CSR indptr array
 * @param indices CSR indices array
 * @param weights Edge weights
 * @param num_vertices Total number of vertices
 * @return RecordBatch with schema:
 *         - src: int64 (source vertex)
 *         - dst: int64 (destination vertex)
 *         - distance: double (shortest distance)
 *         - via: int64 (intermediate vertex for path reconstruction)
 *
 * Performance: O(V^3) time, O(V^2) space
 * Memory usage: ~16 bytes per vertex pair (V^2)
 *
 * Not recommended for graphs with >500 vertices due to memory/time constraints.
 */
arrow::Result<std::shared_ptr<arrow::RecordBatch>> FloydWarshall(
    std::shared_ptr<arrow::Int64Array> indptr,
    std::shared_ptr<arrow::Int64Array> indices,
    std::shared_ptr<arrow::DoubleArray> weights,
    int64_t num_vertices);

/**
 * A* shortest path from source to target (heuristic-based).
 *
 * Uses heuristic function to guide search towards target.
 * More efficient than Dijkstra when target is known and heuristic is good.
 *
 * @param indptr CSR indptr array
 * @param indices CSR indices array
 * @param weights Edge weights
 * @param num_vertices Total number of vertices
 * @param source Source vertex ID
 * @param target Target vertex ID
 * @param heuristic Heuristic estimates (h[v] = estimated distance from v to target)
 * @return ShortestPathsResult with path from source to target only
 *
 * Requirements:
 * - heuristic must be admissible (never overestimate true distance)
 * - heuristic.length() == num_vertices
 *
 * Performance: O((V + E) log V) worst case, often much better with good heuristic
 */
arrow::Result<ShortestPathsResult> AStar(
    std::shared_ptr<arrow::Int64Array> indptr,
    std::shared_ptr<arrow::Int64Array> indices,
    std::shared_ptr<arrow::DoubleArray> weights,
    int64_t num_vertices,
    int64_t source,
    int64_t target,
    std::shared_ptr<arrow::DoubleArray> heuristic);

}  // namespace traversal
}  // namespace graph
}  // namespace sabot
