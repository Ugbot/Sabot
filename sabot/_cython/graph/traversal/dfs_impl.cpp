// dfs_impl.cpp - Depth-First Search Implementation

#include "dfs.h"
#include <arrow/builder.h>
#include <algorithm>
#include <stack>
#include <vector>

namespace sabot {
namespace graph {
namespace traversal {

// DFS state enum
enum class DFSState : uint8_t {
  UNVISITED = 0,
  VISITING = 1,  // Currently on the stack (gray in CLRS)
  VISITED = 2    // Finished (black in CLRS)
};

arrow::Result<DFSResult> DFS(
    std::shared_ptr<arrow::Int64Array> indptr,
    std::shared_ptr<arrow::Int64Array> indices,
    int64_t num_vertices,
    int64_t source) {

  // Validate input
  if (source < 0 || source >= num_vertices) {
    return arrow::Status::Invalid("Source vertex out of range");
  }

  // Initialize result builders
  arrow::Int64Builder vertex_builder;
  arrow::Int64Builder discovery_builder;
  arrow::Int64Builder finish_builder;
  arrow::Int64Builder predecessor_builder;

  // DFS state: visited status, times, and predecessors
  std::vector<DFSState> state(num_vertices, DFSState::UNVISITED);
  std::vector<int64_t> discovery(num_vertices, -1);
  std::vector<int64_t> finish(num_vertices, -1);
  std::vector<int64_t> predecessor(num_vertices, -1);

  // DFS stack: (vertex, is_backtrack)
  // is_backtrack = false on first visit, true when popping to set finish time
  std::stack<std::pair<int64_t, bool>> stack;
  stack.push({source, false});

  int64_t time_counter = 0;
  int64_t num_visited = 0;

  // DFS traversal
  while (!stack.empty()) {
    auto [v, is_backtrack] = stack.top();
    stack.pop();

    if (is_backtrack) {
      // Finish vertex (post-order)
      finish[v] = time_counter++;
      state[v] = DFSState::VISITED;
      continue;
    }

    if (state[v] != DFSState::UNVISITED) {
      continue;  // Already visited or visiting
    }

    // Mark as visiting (gray) and record discovery time
    state[v] = DFSState::VISITING;
    discovery[v] = time_counter++;
    num_visited++;

    // Push finish marker for this vertex
    stack.push({v, true});

    // Explore neighbors (push in reverse order for DFS left-to-right)
    int64_t start = indptr->Value(v);
    int64_t end = indptr->Value(v + 1);

    for (int64_t i = end - 1; i >= start; --i) {
      int64_t neighbor = indices->Value(i);

      if (state[neighbor] == DFSState::UNVISITED) {
        predecessor[neighbor] = v;
        stack.push({neighbor, false});
      }
    }
  }

  // Build result arrays (in discovery order)
  // Create mapping from discovery time to vertex
  std::vector<std::pair<int64_t, int64_t>> timed_vertices;
  for (int64_t v = 0; v < num_vertices; ++v) {
    if (discovery[v] >= 0) {
      timed_vertices.push_back({discovery[v], v});
    }
  }
  std::sort(timed_vertices.begin(), timed_vertices.end());

  for (const auto& [disc_time, v] : timed_vertices) {
    ARROW_RETURN_NOT_OK(vertex_builder.Append(v));
    ARROW_RETURN_NOT_OK(discovery_builder.Append(discovery[v]));
    ARROW_RETURN_NOT_OK(finish_builder.Append(finish[v]));
    ARROW_RETURN_NOT_OK(predecessor_builder.Append(predecessor[v]));
  }

  std::shared_ptr<arrow::Int64Array> vertices_array;
  std::shared_ptr<arrow::Int64Array> discovery_array;
  std::shared_ptr<arrow::Int64Array> finish_array;
  std::shared_ptr<arrow::Int64Array> predecessors_array;

  ARROW_RETURN_NOT_OK(vertex_builder.Finish(&vertices_array));
  ARROW_RETURN_NOT_OK(discovery_builder.Finish(&discovery_array));
  ARROW_RETURN_NOT_OK(finish_builder.Finish(&finish_array));
  ARROW_RETURN_NOT_OK(predecessor_builder.Finish(&predecessors_array));

  return DFSResult{vertices_array, discovery_array, finish_array, predecessors_array, num_visited};
}

arrow::Result<DFSResult> FullDFS(
    std::shared_ptr<arrow::Int64Array> indptr,
    std::shared_ptr<arrow::Int64Array> indices,
    int64_t num_vertices) {

  // Initialize result builders
  arrow::Int64Builder vertex_builder;
  arrow::Int64Builder discovery_builder;
  arrow::Int64Builder finish_builder;
  arrow::Int64Builder predecessor_builder;

  // DFS state
  std::vector<DFSState> state(num_vertices, DFSState::UNVISITED);
  std::vector<int64_t> discovery(num_vertices, -1);
  std::vector<int64_t> finish(num_vertices, -1);
  std::vector<int64_t> predecessor(num_vertices, -1);

  int64_t time_counter = 0;
  int64_t num_visited = 0;

  // Visit all vertices (handles disconnected components)
  for (int64_t source = 0; source < num_vertices; ++source) {
    if (state[source] != DFSState::UNVISITED) {
      continue;  // Already visited
    }

    // DFS from this source
    std::stack<std::pair<int64_t, bool>> stack;
    stack.push({source, false});

    while (!stack.empty()) {
      auto [v, is_backtrack] = stack.top();
      stack.pop();

      if (is_backtrack) {
        finish[v] = time_counter++;
        state[v] = DFSState::VISITED;
        continue;
      }

      if (state[v] != DFSState::UNVISITED) {
        continue;
      }

      state[v] = DFSState::VISITING;
      discovery[v] = time_counter++;
      num_visited++;

      stack.push({v, true});

      int64_t start = indptr->Value(v);
      int64_t end = indptr->Value(v + 1);

      for (int64_t i = end - 1; i >= start; --i) {
        int64_t neighbor = indices->Value(i);
        if (state[neighbor] == DFSState::UNVISITED) {
          predecessor[neighbor] = v;
          stack.push({neighbor, false});
        }
      }
    }
  }

  // Build result arrays (in discovery order)
  std::vector<std::pair<int64_t, int64_t>> timed_vertices;
  for (int64_t v = 0; v < num_vertices; ++v) {
    timed_vertices.push_back({discovery[v], v});
  }
  std::sort(timed_vertices.begin(), timed_vertices.end());

  for (const auto& [disc_time, v] : timed_vertices) {
    ARROW_RETURN_NOT_OK(vertex_builder.Append(v));
    ARROW_RETURN_NOT_OK(discovery_builder.Append(discovery[v]));
    ARROW_RETURN_NOT_OK(finish_builder.Append(finish[v]));
    ARROW_RETURN_NOT_OK(predecessor_builder.Append(predecessor[v]));
  }

  std::shared_ptr<arrow::Int64Array> vertices_array;
  std::shared_ptr<arrow::Int64Array> discovery_array;
  std::shared_ptr<arrow::Int64Array> finish_array;
  std::shared_ptr<arrow::Int64Array> predecessors_array;

  ARROW_RETURN_NOT_OK(vertex_builder.Finish(&vertices_array));
  ARROW_RETURN_NOT_OK(discovery_builder.Finish(&discovery_array));
  ARROW_RETURN_NOT_OK(finish_builder.Finish(&finish_array));
  ARROW_RETURN_NOT_OK(predecessor_builder.Finish(&predecessors_array));

  return DFSResult{vertices_array, discovery_array, finish_array, predecessors_array, num_visited};
}

arrow::Result<std::shared_ptr<arrow::Int64Array>> TopologicalSort(
    std::shared_ptr<arrow::Int64Array> indptr,
    std::shared_ptr<arrow::Int64Array> indices,
    int64_t num_vertices) {

  // DFS state
  std::vector<DFSState> state(num_vertices, DFSState::UNVISITED);
  std::vector<int64_t> finish_order;  // Vertices in reverse finish order

  // DFS from all vertices
  for (int64_t source = 0; source < num_vertices; ++source) {
    if (state[source] != DFSState::UNVISITED) {
      continue;
    }

    std::stack<std::pair<int64_t, bool>> stack;
    stack.push({source, false});

    while (!stack.empty()) {
      auto [v, is_backtrack] = stack.top();
      stack.pop();

      if (is_backtrack) {
        finish_order.push_back(v);  // Add to finish order
        state[v] = DFSState::VISITED;
        continue;
      }

      if (state[v] != DFSState::UNVISITED) {
        continue;
      }

      state[v] = DFSState::VISITING;
      stack.push({v, true});

      int64_t start = indptr->Value(v);
      int64_t end = indptr->Value(v + 1);

      for (int64_t i = end - 1; i >= start; --i) {
        int64_t neighbor = indices->Value(i);

        if (state[neighbor] == DFSState::VISITING) {
          // Back edge detected - cycle exists
          return arrow::Status::Invalid("Graph contains a cycle - topological sort not possible");
        }

        if (state[neighbor] == DFSState::UNVISITED) {
          stack.push({neighbor, false});
        }
      }
    }
  }

  // Reverse finish order = topological order
  std::reverse(finish_order.begin(), finish_order.end());

  arrow::Int64Builder builder;
  for (int64_t v : finish_order) {
    ARROW_RETURN_NOT_OK(builder.Append(v));
  }

  std::shared_ptr<arrow::Int64Array> result;
  ARROW_RETURN_NOT_OK(builder.Finish(&result));
  return result;
}

arrow::Result<bool> HasCycle(
    std::shared_ptr<arrow::Int64Array> indptr,
    std::shared_ptr<arrow::Int64Array> indices,
    int64_t num_vertices) {

  std::vector<DFSState> state(num_vertices, DFSState::UNVISITED);

  for (int64_t source = 0; source < num_vertices; ++source) {
    if (state[source] != DFSState::UNVISITED) {
      continue;
    }

    std::stack<std::pair<int64_t, bool>> stack;
    stack.push({source, false});

    while (!stack.empty()) {
      auto [v, is_backtrack] = stack.top();
      stack.pop();

      if (is_backtrack) {
        state[v] = DFSState::VISITED;
        continue;
      }

      if (state[v] != DFSState::UNVISITED) {
        continue;
      }

      state[v] = DFSState::VISITING;
      stack.push({v, true});

      int64_t start = indptr->Value(v);
      int64_t end = indptr->Value(v + 1);

      for (int64_t i = start; i < end; ++i) {
        int64_t neighbor = indices->Value(i);

        if (state[neighbor] == DFSState::VISITING) {
          // Back edge detected - cycle found
          return true;
        }

        if (state[neighbor] == DFSState::UNVISITED) {
          stack.push({neighbor, false});
        }
      }
    }
  }

  return false;  // No cycle found
}

arrow::Result<std::shared_ptr<arrow::RecordBatch>> FindBackEdges(
    std::shared_ptr<arrow::Int64Array> indptr,
    std::shared_ptr<arrow::Int64Array> indices,
    int64_t num_vertices) {

  std::vector<DFSState> state(num_vertices, DFSState::UNVISITED);
  std::vector<int64_t> back_edge_src;
  std::vector<int64_t> back_edge_dst;

  for (int64_t source = 0; source < num_vertices; ++source) {
    if (state[source] != DFSState::UNVISITED) {
      continue;
    }

    std::stack<std::pair<int64_t, bool>> stack;
    stack.push({source, false});

    while (!stack.empty()) {
      auto [v, is_backtrack] = stack.top();
      stack.pop();

      if (is_backtrack) {
        state[v] = DFSState::VISITED;
        continue;
      }

      if (state[v] != DFSState::UNVISITED) {
        continue;
      }

      state[v] = DFSState::VISITING;
      stack.push({v, true});

      int64_t start = indptr->Value(v);
      int64_t end = indptr->Value(v + 1);

      for (int64_t i = start; i < end; ++i) {
        int64_t neighbor = indices->Value(i);

        if (state[neighbor] == DFSState::VISITING) {
          // Back edge found
          back_edge_src.push_back(v);
          back_edge_dst.push_back(neighbor);
        } else if (state[neighbor] == DFSState::UNVISITED) {
          stack.push({neighbor, false});
        }
      }
    }
  }

  // Build result RecordBatch
  auto src_array = std::make_shared<arrow::Int64Builder>();
  auto dst_array = std::make_shared<arrow::Int64Builder>();

  for (size_t i = 0; i < back_edge_src.size(); ++i) {
    ARROW_RETURN_NOT_OK(src_array->Append(back_edge_src[i]));
    ARROW_RETURN_NOT_OK(dst_array->Append(back_edge_dst[i]));
  }

  std::shared_ptr<arrow::Int64Array> src_result;
  std::shared_ptr<arrow::Int64Array> dst_result;

  ARROW_RETURN_NOT_OK(src_array->Finish(&src_result));
  ARROW_RETURN_NOT_OK(dst_array->Finish(&dst_result));

  auto schema = arrow::schema({
    arrow::field("src", arrow::int64()),
    arrow::field("dst", arrow::int64())
  });

  return arrow::RecordBatch::Make(schema, back_edge_src.size(), {src_result, dst_result});
}

}  // namespace traversal
}  // namespace graph
}  // namespace sabot
