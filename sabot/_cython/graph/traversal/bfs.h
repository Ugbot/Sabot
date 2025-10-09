// bfs.h - Breadth-First Search Algorithms
//
// Arrow-native BFS implementation for graph traversal.
// Uses CSR adjacency structure for O(V + E) traversal.

#ifndef SABOT_GRAPH_BFS_H
#define SABOT_GRAPH_BFS_H

#include <arrow/api.h>
#include <arrow/array.h>
#include <memory>
#include <vector>
#include <queue>

namespace sabot {
namespace graph {
namespace traversal {

/// BFS traversal result containing visited vertices and metadata
struct BFSResult {
  std::shared_ptr<arrow::Int64Array> vertices;    // Visited vertex IDs in BFS order
  std::shared_ptr<arrow::Int64Array> distances;   // Distance from source
  std::shared_ptr<arrow::Int64Array> predecessors; // Parent vertex in BFS tree
  int64_t num_visited;                             // Number of vertices visited
};

/// BFS traversal from a single source vertex
///
/// Args:
///   indptr: CSR indptr array (offsets for each vertex's neighbors)
///   indices: CSR indices array (neighbor vertex IDs)
///   num_vertices: Total number of vertices in graph
///   source: Source vertex ID to start traversal
///   max_depth: Maximum depth to traverse (-1 = unlimited)
///
/// Returns:
///   BFSResult with visited vertices, distances, and predecessors
///
/// Performance: O(V + E) time, O(V) space
arrow::Result<BFSResult> BFS(
    std::shared_ptr<arrow::Int64Array> indptr,
    std::shared_ptr<arrow::Int64Array> indices,
    int64_t num_vertices,
    int64_t source,
    int64_t max_depth = -1);

/// K-hop neighbors from a source vertex
///
/// Args:
///   indptr: CSR indptr array
///   indices: CSR indices array
///   num_vertices: Total number of vertices
///   source: Source vertex ID
///   k: Number of hops
///
/// Returns:
///   Int64Array of all vertices exactly k hops away from source
///
/// Performance: O(V + E) time (worst case), typically much faster
arrow::Result<std::shared_ptr<arrow::Int64Array>> KHopNeighbors(
    std::shared_ptr<arrow::Int64Array> indptr,
    std::shared_ptr<arrow::Int64Array> indices,
    int64_t num_vertices,
    int64_t source,
    int64_t k);

/// Multi-source BFS (breadth-first from multiple sources simultaneously)
///
/// Args:
///   indptr: CSR indptr array
///   indices: CSR indices array
///   num_vertices: Total number of vertices
///   sources: Array of source vertex IDs
///   max_depth: Maximum depth to traverse (-1 = unlimited)
///
/// Returns:
///   BFSResult with vertices and distances (predecessors = closest source)
///
/// Performance: O(V + E) time, O(V) space
arrow::Result<BFSResult> MultiBFS(
    std::shared_ptr<arrow::Int64Array> indptr,
    std::shared_ptr<arrow::Int64Array> indices,
    int64_t num_vertices,
    std::shared_ptr<arrow::Int64Array> sources,
    int64_t max_depth = -1);

/// Connected component containing a source vertex (using BFS)
///
/// Args:
///   indptr: CSR indptr array
///   indices: CSR indices array
///   num_vertices: Total number of vertices
///   source: Source vertex ID
///
/// Returns:
///   Int64Array of all vertices in the same connected component
///
/// Performance: O(V + E) time for the component
arrow::Result<std::shared_ptr<arrow::Int64Array>> ComponentBFS(
    std::shared_ptr<arrow::Int64Array> indptr,
    std::shared_ptr<arrow::Int64Array> indices,
    int64_t num_vertices,
    int64_t source);

}  // namespace traversal
}  // namespace graph
}  // namespace sabot

#endif  // SABOT_GRAPH_BFS_H
