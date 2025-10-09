// bfs.cpp - Breadth-First Search Implementation

#include "bfs.h"
#include <arrow/builder.h>
#include <algorithm>
#include <queue>
#include <vector>

namespace sabot {
namespace graph {
namespace traversal {

arrow::Result<BFSResult> BFS(
    std::shared_ptr<arrow::Int64Array> indptr,
    std::shared_ptr<arrow::Int64Array> indices,
    int64_t num_vertices,
    int64_t source,
    int64_t max_depth) {

  // Validate input
  if (source < 0 || source >= num_vertices) {
    return arrow::Status::Invalid("Source vertex out of range");
  }

  // Initialize result builders
  arrow::Int64Builder vertex_builder;
  arrow::Int64Builder distance_builder;
  arrow::Int64Builder predecessor_builder;

  // BFS state: visited bitmap and distance array
  std::vector<bool> visited(num_vertices, false);
  std::vector<int64_t> distance(num_vertices, -1);
  std::vector<int64_t> predecessor(num_vertices, -1);

  // BFS queue: (vertex, depth)
  std::queue<std::pair<int64_t, int64_t>> queue;
  queue.push({source, 0});
  visited[source] = true;
  distance[source] = 0;

  int64_t num_visited = 0;

  // BFS traversal
  while (!queue.empty()) {
    auto [v, depth] = queue.front();
    queue.pop();

    // Add to result
    ARROW_RETURN_NOT_OK(vertex_builder.Append(v));
    ARROW_RETURN_NOT_OK(distance_builder.Append(distance[v]));
    ARROW_RETURN_NOT_OK(predecessor_builder.Append(predecessor[v]));
    num_visited++;

    // Check max depth
    if (max_depth >= 0 && depth >= max_depth) {
      continue;
    }

    // Explore neighbors
    int64_t start = indptr->Value(v);
    int64_t end = indptr->Value(v + 1);

    for (int64_t i = start; i < end; ++i) {
      int64_t neighbor = indices->Value(i);

      if (!visited[neighbor]) {
        visited[neighbor] = true;
        distance[neighbor] = depth + 1;
        predecessor[neighbor] = v;
        queue.push({neighbor, depth + 1});
      }
    }
  }

  // Build result arrays
  std::shared_ptr<arrow::Int64Array> vertices_array;
  std::shared_ptr<arrow::Int64Array> distances_array;
  std::shared_ptr<arrow::Int64Array> predecessors_array;

  ARROW_RETURN_NOT_OK(vertex_builder.Finish(&vertices_array));
  ARROW_RETURN_NOT_OK(distance_builder.Finish(&distances_array));
  ARROW_RETURN_NOT_OK(predecessor_builder.Finish(&predecessors_array));

  return BFSResult{vertices_array, distances_array, predecessors_array, num_visited};
}

arrow::Result<std::shared_ptr<arrow::Int64Array>> KHopNeighbors(
    std::shared_ptr<arrow::Int64Array> indptr,
    std::shared_ptr<arrow::Int64Array> indices,
    int64_t num_vertices,
    int64_t source,
    int64_t k) {

  if (k < 0) {
    return arrow::Status::Invalid("k must be non-negative");
  }

  if (k == 0) {
    // 0-hop neighbors = just the source vertex
    arrow::Int64Builder builder;
    ARROW_RETURN_NOT_OK(builder.Append(source));
    std::shared_ptr<arrow::Int64Array> result;
    ARROW_RETURN_NOT_OK(builder.Finish(&result));
    return result;
  }

  // Run BFS with max_depth = k
  ARROW_ASSIGN_OR_RAISE(auto bfs_result, BFS(indptr, indices, num_vertices, source, k));

  // Filter vertices that are exactly k hops away
  arrow::Int64Builder builder;
  for (int64_t i = 0; i < bfs_result.distances->length(); ++i) {
    if (bfs_result.distances->Value(i) == k) {
      ARROW_RETURN_NOT_OK(builder.Append(bfs_result.vertices->Value(i)));
    }
  }

  std::shared_ptr<arrow::Int64Array> result;
  ARROW_RETURN_NOT_OK(builder.Finish(&result));
  return result;
}

arrow::Result<BFSResult> MultiBFS(
    std::shared_ptr<arrow::Int64Array> indptr,
    std::shared_ptr<arrow::Int64Array> indices,
    int64_t num_vertices,
    std::shared_ptr<arrow::Int64Array> sources,
    int64_t max_depth) {

  // Initialize result builders
  arrow::Int64Builder vertex_builder;
  arrow::Int64Builder distance_builder;
  arrow::Int64Builder predecessor_builder;

  // BFS state
  std::vector<bool> visited(num_vertices, false);
  std::vector<int64_t> distance(num_vertices, -1);
  std::vector<int64_t> predecessor(num_vertices, -1);

  // BFS queue: (vertex, depth)
  std::queue<std::pair<int64_t, int64_t>> queue;

  // Initialize with all sources
  for (int64_t i = 0; i < sources->length(); ++i) {
    int64_t source = sources->Value(i);
    if (source >= 0 && source < num_vertices && !visited[source]) {
      queue.push({source, 0});
      visited[source] = true;
      distance[source] = 0;
      predecessor[source] = source;  // Mark as source (predecessor = self)
    }
  }

  int64_t num_visited = 0;

  // BFS traversal (same as single-source)
  while (!queue.empty()) {
    auto [v, depth] = queue.front();
    queue.pop();

    // Add to result
    ARROW_RETURN_NOT_OK(vertex_builder.Append(v));
    ARROW_RETURN_NOT_OK(distance_builder.Append(distance[v]));
    ARROW_RETURN_NOT_OK(predecessor_builder.Append(predecessor[v]));
    num_visited++;

    // Check max depth
    if (max_depth >= 0 && depth >= max_depth) {
      continue;
    }

    // Explore neighbors
    int64_t start = indptr->Value(v);
    int64_t end = indptr->Value(v + 1);

    for (int64_t i = start; i < end; ++i) {
      int64_t neighbor = indices->Value(i);

      if (!visited[neighbor]) {
        visited[neighbor] = true;
        distance[neighbor] = depth + 1;
        predecessor[neighbor] = v;
        queue.push({neighbor, depth + 1});
      }
    }
  }

  // Build result arrays
  std::shared_ptr<arrow::Int64Array> vertices_array;
  std::shared_ptr<arrow::Int64Array> distances_array;
  std::shared_ptr<arrow::Int64Array> predecessors_array;

  ARROW_RETURN_NOT_OK(vertex_builder.Finish(&vertices_array));
  ARROW_RETURN_NOT_OK(distance_builder.Finish(&distances_array));
  ARROW_RETURN_NOT_OK(predecessor_builder.Finish(&predecessors_array));

  return BFSResult{vertices_array, distances_array, predecessors_array, num_visited};
}

arrow::Result<std::shared_ptr<arrow::Int64Array>> ComponentBFS(
    std::shared_ptr<arrow::Int64Array> indptr,
    std::shared_ptr<arrow::Int64Array> indices,
    int64_t num_vertices,
    int64_t source) {

  // Run BFS with unlimited depth
  ARROW_ASSIGN_OR_RAISE(auto bfs_result, BFS(indptr, indices, num_vertices, source, -1));

  // Return all visited vertices (the connected component)
  return bfs_result.vertices;
}

}  // namespace traversal
}  // namespace graph
}  // namespace sabot
