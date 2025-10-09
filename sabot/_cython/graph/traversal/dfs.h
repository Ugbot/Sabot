// dfs.h - Depth-First Search Header
//
// Arrow-native DFS algorithms for graph traversal.
//
// Performance: O(V + E) time, O(V) space
// Memory: Zero-copy Arrow arrays

#pragma once

#include <arrow/api.h>
#include <memory>

namespace sabot {
namespace graph {
namespace traversal {

// DFS result structure
struct DFSResult {
  std::shared_ptr<arrow::Int64Array> vertices;       // Visited vertices in DFS order
  std::shared_ptr<arrow::Int64Array> discovery_time; // Discovery time (pre-order)
  std::shared_ptr<arrow::Int64Array> finish_time;    // Finish time (post-order)
  std::shared_ptr<arrow::Int64Array> predecessors;   // Parent in DFS tree
  int64_t num_visited;                               // Total vertices visited
};

// ============================================================================
// DFS Functions
// ============================================================================

/**
 * Depth-First Search from a source vertex.
 *
 * Performs DFS traversal and records discovery/finish times.
 *
 * @param indptr CSR indptr array (offsets for each vertex's neighbors)
 * @param indices CSR indices array (neighbor vertex IDs)
 * @param num_vertices Total number of vertices in graph
 * @param source Source vertex ID to start traversal
 * @return DFSResult with vertices, times, and predecessors
 *
 * Performance: O(V + E) time, O(V) space (call stack + visited array)
 *
 * Example:
 *     auto result = DFS(indptr, indices, num_vertices, 0);
 *     // result.discovery_time[v] = when v was first visited
 *     // result.finish_time[v] = when v's subtree was fully explored
 */
arrow::Result<DFSResult> DFS(
    std::shared_ptr<arrow::Int64Array> indptr,
    std::shared_ptr<arrow::Int64Array> indices,
    int64_t num_vertices,
    int64_t source);

/**
 * Full DFS traversal (all connected components).
 *
 * Performs DFS from all unvisited vertices to handle disconnected graphs.
 *
 * @param indptr CSR indptr array
 * @param indices CSR indices array
 * @param num_vertices Total number of vertices
 * @return DFSResult covering all vertices
 *
 * Performance: O(V + E) time, O(V) space
 */
arrow::Result<DFSResult> FullDFS(
    std::shared_ptr<arrow::Int64Array> indptr,
    std::shared_ptr<arrow::Int64Array> indices,
    int64_t num_vertices);

/**
 * Topological sort using DFS.
 *
 * Returns vertices in topological order (reverse post-order).
 * Only valid for Directed Acyclic Graphs (DAGs).
 *
 * @param indptr CSR indptr array
 * @param indices CSR indices array
 * @param num_vertices Total number of vertices
 * @return Int64Array of vertices in topological order
 *
 * Returns error if graph contains cycles.
 * Performance: O(V + E) time, O(V) space
 *
 * Example:
 *     auto topo = TopologicalSort(indptr, indices, num_vertices);
 *     // For DAG: topo[i] comes before topo[j] if edge i -> j exists
 */
arrow::Result<std::shared_ptr<arrow::Int64Array>> TopologicalSort(
    std::shared_ptr<arrow::Int64Array> indptr,
    std::shared_ptr<arrow::Int64Array> indices,
    int64_t num_vertices);

/**
 * Detect cycles in a directed graph using DFS.
 *
 * @param indptr CSR indptr array
 * @param indices CSR indices array
 * @param num_vertices Total number of vertices
 * @return Boolean - true if graph contains at least one cycle
 *
 * Performance: O(V + E) time, O(V) space
 */
arrow::Result<bool> HasCycle(
    std::shared_ptr<arrow::Int64Array> indptr,
    std::shared_ptr<arrow::Int64Array> indices,
    int64_t num_vertices);

/**
 * Find all back edges (edges creating cycles).
 *
 * Back edges are edges (u -> v) where v is an ancestor of u in the DFS tree.
 *
 * @param indptr CSR indptr array
 * @param indices CSR indices array
 * @param num_vertices Total number of vertices
 * @return RecordBatch with columns 'src' and 'dst' for back edges
 *
 * Performance: O(V + E) time, O(V) space
 */
arrow::Result<std::shared_ptr<arrow::RecordBatch>> FindBackEdges(
    std::shared_ptr<arrow::Int64Array> indptr,
    std::shared_ptr<arrow::Int64Array> indices,
    int64_t num_vertices);

}  // namespace traversal
}  // namespace graph
}  // namespace sabot
