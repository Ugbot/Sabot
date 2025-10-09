// shortest_paths_impl.cpp - Shortest Paths Implementation

#include "shortest_paths.h"
#include <arrow/builder.h>
#include <algorithm>
#include <queue>
#include <vector>
#include <limits>
#include <cmath>

namespace sabot {
namespace graph {
namespace traversal {

constexpr double INF = std::numeric_limits<double>::infinity();

// Min-heap node for Dijkstra's algorithm
struct DijkstraNode {
  int64_t vertex;
  double distance;

  bool operator>(const DijkstraNode& other) const {
    return distance > other.distance;
  }
};

arrow::Result<ShortestPathsResult> Dijkstra(
    std::shared_ptr<arrow::Int64Array> indptr,
    std::shared_ptr<arrow::Int64Array> indices,
    std::shared_ptr<arrow::DoubleArray> weights,
    int64_t num_vertices,
    int64_t source) {

  // Validate input
  if (source < 0 || source >= num_vertices) {
    return arrow::Status::Invalid("Source vertex out of range");
  }

  if (indices->length() != weights->length()) {
    return arrow::Status::Invalid("Indices and weights arrays must have same length");
  }

  // Initialize distances and predecessors
  std::vector<double> distances(num_vertices, INF);
  std::vector<int64_t> predecessors(num_vertices, -1);
  std::vector<bool> visited(num_vertices, false);

  distances[source] = 0.0;
  int64_t num_reached = 0;

  // Min-heap priority queue
  std::priority_queue<DijkstraNode, std::vector<DijkstraNode>, std::greater<DijkstraNode>> pq;
  pq.push({source, 0.0});

  while (!pq.empty()) {
    DijkstraNode current = pq.top();
    pq.pop();

    int64_t u = current.vertex;

    if (visited[u]) {
      continue;  // Already processed
    }

    visited[u] = true;
    num_reached++;

    // Explore neighbors
    int64_t start = indptr->Value(u);
    int64_t end = indptr->Value(u + 1);

    for (int64_t i = start; i < end; ++i) {
      int64_t v = indices->Value(i);
      double weight = weights->Value(i);

      // Validate weight
      if (weight < 0.0) {
        return arrow::Status::Invalid("Negative edge weights not supported in Dijkstra");
      }

      double new_dist = distances[u] + weight;

      if (new_dist < distances[v]) {
        distances[v] = new_dist;
        predecessors[v] = u;
        pq.push({v, new_dist});
      }
    }
  }

  // Build result arrays
  arrow::DoubleBuilder dist_builder;
  arrow::Int64Builder pred_builder;

  for (int64_t v = 0; v < num_vertices; ++v) {
    ARROW_RETURN_NOT_OK(dist_builder.Append(distances[v]));
    ARROW_RETURN_NOT_OK(pred_builder.Append(predecessors[v]));
  }

  std::shared_ptr<arrow::DoubleArray> distances_array;
  std::shared_ptr<arrow::Int64Array> predecessors_array;

  ARROW_RETURN_NOT_OK(dist_builder.Finish(&distances_array));
  ARROW_RETURN_NOT_OK(pred_builder.Finish(&predecessors_array));

  return ShortestPathsResult{distances_array, predecessors_array, source, num_reached};
}

arrow::Result<ShortestPathsResult> UnweightedShortestPaths(
    std::shared_ptr<arrow::Int64Array> indptr,
    std::shared_ptr<arrow::Int64Array> indices,
    int64_t num_vertices,
    int64_t source) {

  // Validate input
  if (source < 0 || source >= num_vertices) {
    return arrow::Status::Invalid("Source vertex out of range");
  }

  // BFS-based shortest paths (all edges weight 1)
  std::vector<double> distances(num_vertices, INF);
  std::vector<int64_t> predecessors(num_vertices, -1);
  std::vector<bool> visited(num_vertices, false);

  distances[source] = 0.0;
  visited[source] = true;

  std::queue<int64_t> queue;
  queue.push(source);

  int64_t num_reached = 1;

  while (!queue.empty()) {
    int64_t u = queue.front();
    queue.pop();

    int64_t start = indptr->Value(u);
    int64_t end = indptr->Value(u + 1);

    for (int64_t i = start; i < end; ++i) {
      int64_t v = indices->Value(i);

      if (!visited[v]) {
        visited[v] = true;
        distances[v] = distances[u] + 1.0;
        predecessors[v] = u;
        queue.push(v);
        num_reached++;
      }
    }
  }

  // Build result arrays
  arrow::DoubleBuilder dist_builder;
  arrow::Int64Builder pred_builder;

  for (int64_t v = 0; v < num_vertices; ++v) {
    ARROW_RETURN_NOT_OK(dist_builder.Append(distances[v]));
    ARROW_RETURN_NOT_OK(pred_builder.Append(predecessors[v]));
  }

  std::shared_ptr<arrow::DoubleArray> distances_array;
  std::shared_ptr<arrow::Int64Array> predecessors_array;

  ARROW_RETURN_NOT_OK(dist_builder.Finish(&distances_array));
  ARROW_RETURN_NOT_OK(pred_builder.Finish(&predecessors_array));

  return ShortestPathsResult{distances_array, predecessors_array, source, num_reached};
}

arrow::Result<std::shared_ptr<arrow::Int64Array>> ReconstructPath(
    std::shared_ptr<arrow::Int64Array> predecessors,
    int64_t source,
    int64_t target) {

  arrow::Int64Builder path_builder;

  // Check if target is reachable
  if (target >= predecessors->length()) {
    return arrow::Status::Invalid("Target vertex out of range");
  }

  // If target is unreachable (predecessor = -1 and target != source)
  if (predecessors->Value(target) == -1 && target != source) {
    // Return empty path
    std::shared_ptr<arrow::Int64Array> empty_path;
    ARROW_RETURN_NOT_OK(path_builder.Finish(&empty_path));
    return empty_path;
  }

  // Reconstruct path backwards from target to source
  std::vector<int64_t> path;
  int64_t current = target;

  while (current != -1) {
    path.push_back(current);
    if (current == source) {
      break;  // Reached source
    }
    current = predecessors->Value(current);
  }

  // Check if we reached the source
  if (path.empty() || path.back() != source) {
    // Path reconstruction failed
    std::shared_ptr<arrow::Int64Array> empty_path;
    ARROW_RETURN_NOT_OK(path_builder.Finish(&empty_path));
    return empty_path;
  }

  // Reverse path (now source to target)
  std::reverse(path.begin(), path.end());

  // Build Arrow array
  for (int64_t v : path) {
    ARROW_RETURN_NOT_OK(path_builder.Append(v));
  }

  std::shared_ptr<arrow::Int64Array> path_array;
  ARROW_RETURN_NOT_OK(path_builder.Finish(&path_array));
  return path_array;
}

arrow::Result<std::shared_ptr<arrow::RecordBatch>> FloydWarshall(
    std::shared_ptr<arrow::Int64Array> indptr,
    std::shared_ptr<arrow::Int64Array> indices,
    std::shared_ptr<arrow::DoubleArray> weights,
    int64_t num_vertices) {

  // WARNING: O(V^3) algorithm - only for small graphs
  if (num_vertices > 500) {
    return arrow::Status::Invalid(
        "Floyd-Warshall not recommended for graphs with >500 vertices (O(V^3) complexity)");
  }

  // Initialize distance matrix (V x V)
  std::vector<std::vector<double>> dist(num_vertices, std::vector<double>(num_vertices, INF));
  std::vector<std::vector<int64_t>> via(num_vertices, std::vector<int64_t>(num_vertices, -1));

  // Distance from vertex to itself is 0
  for (int64_t v = 0; v < num_vertices; ++v) {
    dist[v][v] = 0.0;
  }

  // Initialize with direct edges
  for (int64_t u = 0; u < num_vertices; ++u) {
    int64_t start = indptr->Value(u);
    int64_t end = indptr->Value(u + 1);

    for (int64_t i = start; i < end; ++i) {
      int64_t v = indices->Value(i);
      double weight = weights->Value(i);

      dist[u][v] = weight;
      via[u][v] = -1;  // Direct edge
    }
  }

  // Floyd-Warshall: try all intermediate vertices
  for (int64_t k = 0; k < num_vertices; ++k) {
    for (int64_t i = 0; i < num_vertices; ++i) {
      for (int64_t j = 0; j < num_vertices; ++j) {
        if (dist[i][k] + dist[k][j] < dist[i][j]) {
          dist[i][j] = dist[i][k] + dist[k][j];
          via[i][j] = k;  // Path goes through k
        }
      }
    }
  }

  // Build result RecordBatch (only finite distances)
  arrow::Int64Builder src_builder;
  arrow::Int64Builder dst_builder;
  arrow::DoubleBuilder dist_builder;
  arrow::Int64Builder via_builder;

  for (int64_t i = 0; i < num_vertices; ++i) {
    for (int64_t j = 0; j < num_vertices; ++j) {
      if (std::isfinite(dist[i][j])) {
        ARROW_RETURN_NOT_OK(src_builder.Append(i));
        ARROW_RETURN_NOT_OK(dst_builder.Append(j));
        ARROW_RETURN_NOT_OK(dist_builder.Append(dist[i][j]));
        ARROW_RETURN_NOT_OK(via_builder.Append(via[i][j]));
      }
    }
  }

  std::shared_ptr<arrow::Int64Array> src_array;
  std::shared_ptr<arrow::Int64Array> dst_array;
  std::shared_ptr<arrow::DoubleArray> dist_array;
  std::shared_ptr<arrow::Int64Array> via_array;

  ARROW_RETURN_NOT_OK(src_builder.Finish(&src_array));
  ARROW_RETURN_NOT_OK(dst_builder.Finish(&dst_array));
  ARROW_RETURN_NOT_OK(dist_builder.Finish(&dist_array));
  ARROW_RETURN_NOT_OK(via_builder.Finish(&via_array));

  auto schema = arrow::schema({
    arrow::field("src", arrow::int64()),
    arrow::field("dst", arrow::int64()),
    arrow::field("distance", arrow::float64()),
    arrow::field("via", arrow::int64())
  });

  return arrow::RecordBatch::Make(schema, src_array->length(), {src_array, dst_array, dist_array, via_array});
}

arrow::Result<ShortestPathsResult> AStar(
    std::shared_ptr<arrow::Int64Array> indptr,
    std::shared_ptr<arrow::Int64Array> indices,
    std::shared_ptr<arrow::DoubleArray> weights,
    int64_t num_vertices,
    int64_t source,
    int64_t target,
    std::shared_ptr<arrow::DoubleArray> heuristic) {

  // Validate input
  if (source < 0 || source >= num_vertices || target < 0 || target >= num_vertices) {
    return arrow::Status::Invalid("Source or target vertex out of range");
  }

  if (heuristic->length() != num_vertices) {
    return arrow::Status::Invalid("Heuristic array length must equal num_vertices");
  }

  // Initialize
  std::vector<double> g_score(num_vertices, INF);  // Actual cost from source
  std::vector<double> f_score(num_vertices, INF);  // Estimated total cost (g + h)
  std::vector<int64_t> predecessors(num_vertices, -1);
  std::vector<bool> closed(num_vertices, false);

  g_score[source] = 0.0;
  f_score[source] = heuristic->Value(source);

  // Priority queue ordered by f_score
  std::priority_queue<DijkstraNode, std::vector<DijkstraNode>, std::greater<DijkstraNode>> open;
  open.push({source, f_score[source]});

  int64_t num_reached = 0;

  while (!open.empty()) {
    DijkstraNode current = open.top();
    open.pop();

    int64_t u = current.vertex;

    if (closed[u]) {
      continue;
    }

    closed[u] = true;
    num_reached++;

    // Found target?
    if (u == target) {
      break;  // Early termination
    }

    // Explore neighbors
    int64_t start = indptr->Value(u);
    int64_t end = indptr->Value(u + 1);

    for (int64_t i = start; i < end; ++i) {
      int64_t v = indices->Value(i);
      double weight = weights->Value(i);

      if (closed[v]) {
        continue;
      }

      double tentative_g = g_score[u] + weight;

      if (tentative_g < g_score[v]) {
        predecessors[v] = u;
        g_score[v] = tentative_g;
        f_score[v] = g_score[v] + heuristic->Value(v);
        open.push({v, f_score[v]});
      }
    }
  }

  // Build result arrays (use g_score as distances)
  arrow::DoubleBuilder dist_builder;
  arrow::Int64Builder pred_builder;

  for (int64_t v = 0; v < num_vertices; ++v) {
    ARROW_RETURN_NOT_OK(dist_builder.Append(g_score[v]));
    ARROW_RETURN_NOT_OK(pred_builder.Append(predecessors[v]));
  }

  std::shared_ptr<arrow::DoubleArray> distances_array;
  std::shared_ptr<arrow::Int64Array> predecessors_array;

  ARROW_RETURN_NOT_OK(dist_builder.Finish(&distances_array));
  ARROW_RETURN_NOT_OK(pred_builder.Finish(&predecessors_array));

  return ShortestPathsResult{distances_array, predecessors_array, source, num_reached};
}

}  // namespace traversal
}  // namespace graph
}  // namespace sabot
