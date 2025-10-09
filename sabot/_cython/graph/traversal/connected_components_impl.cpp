#include "connected_components.h"
#include <arrow/builder.h>
#include <arrow/status.h>
#include <vector>
#include <queue>
#include <unordered_map>
#include <algorithm>

namespace sabot {
namespace graph {
namespace traversal {

/**
 * Union-Find data structure with path compression and union by rank.
 */
class UnionFind {
public:
    explicit UnionFind(int64_t n) : parent_(n), rank_(n, 0) {
        for (int64_t i = 0; i < n; i++) {
            parent_[i] = i;
        }
    }

    // Find with path compression
    int64_t Find(int64_t x) {
        if (parent_[x] != x) {
            parent_[x] = Find(parent_[x]);  // Path compression
        }
        return parent_[x];
    }

    // Union by rank
    void Union(int64_t x, int64_t y) {
        int64_t root_x = Find(x);
        int64_t root_y = Find(y);

        if (root_x == root_y) {
            return;
        }

        // Union by rank
        if (rank_[root_x] < rank_[root_y]) {
            parent_[root_x] = root_y;
        } else if (rank_[root_x] > rank_[root_y]) {
            parent_[root_y] = root_x;
        } else {
            parent_[root_y] = root_x;
            rank_[root_x]++;
        }
    }

    const std::vector<int64_t>& GetParents() const {
        return parent_;
    }

private:
    std::vector<int64_t> parent_;
    std::vector<int64_t> rank_;
};

/**
 * Find connected components using union-find.
 */
arrow::Result<ConnectedComponentsResult> FindConnectedComponents(
    const std::shared_ptr<arrow::Int64Array>& indptr,
    const std::shared_ptr<arrow::Int64Array>& indices,
    int64_t num_vertices
) {
    // Validate inputs
    if (!indptr || !indices) {
        return arrow::Status::Invalid("indptr and indices cannot be null");
    }
    if (num_vertices <= 0) {
        return arrow::Status::Invalid("num_vertices must be positive");
    }
    if (indptr->length() != num_vertices + 1) {
        return arrow::Status::Invalid("indptr length must be num_vertices + 1");
    }

    const int64_t* indptr_data = indptr->raw_values();
    const int64_t* indices_data = indices->raw_values();

    // Initialize union-find
    UnionFind uf(num_vertices);

    // Process all edges
    for (int64_t u = 0; u < num_vertices; u++) {
        int64_t start = indptr_data[u];
        int64_t end = indptr_data[u + 1];

        for (int64_t i = start; i < end; i++) {
            int64_t v = indices_data[i];
            if (v >= 0 && v < num_vertices) {
                uf.Union(u, v);
            }
        }
    }

    // Assign component IDs (normalize to 0-based)
    std::unordered_map<int64_t, int64_t> root_to_component;
    int64_t component_counter = 0;

    std::vector<int64_t> component_ids(num_vertices);
    for (int64_t v = 0; v < num_vertices; v++) {
        int64_t root = uf.Find(v);
        if (root_to_component.find(root) == root_to_component.end()) {
            root_to_component[root] = component_counter++;
        }
        component_ids[v] = root_to_component[root];
    }

    int64_t num_components = component_counter;

    // Compute component sizes
    std::vector<int64_t> component_sizes(num_components, 0);
    for (int64_t v = 0; v < num_vertices; v++) {
        component_sizes[component_ids[v]]++;
    }

    // Find largest component
    int64_t largest_size = 0;
    for (int64_t size : component_sizes) {
        if (size > largest_size) {
            largest_size = size;
        }
    }

    // Build Arrow arrays
    arrow::Int64Builder component_id_builder;
    arrow::Int64Builder component_size_builder;

    ARROW_RETURN_NOT_OK(component_id_builder.AppendValues(component_ids));
    ARROW_RETURN_NOT_OK(component_size_builder.AppendValues(component_sizes));

    std::shared_ptr<arrow::Int64Array> component_id_array;
    std::shared_ptr<arrow::Int64Array> component_size_array;
    ARROW_RETURN_NOT_OK(component_id_builder.Finish(&component_id_array));
    ARROW_RETURN_NOT_OK(component_size_builder.Finish(&component_size_array));

    ConnectedComponentsResult result;
    result.component_ids = component_id_array;
    result.num_components = num_components;
    result.component_sizes = component_size_array;
    result.largest_component_size = largest_size;
    result.num_vertices = num_vertices;

    return result;
}

/**
 * Find connected components using BFS (alternative approach).
 */
arrow::Result<ConnectedComponentsResult> FindConnectedComponentsBFS(
    const std::shared_ptr<arrow::Int64Array>& indptr,
    const std::shared_ptr<arrow::Int64Array>& indices,
    int64_t num_vertices
) {
    // Validate inputs
    if (!indptr || !indices) {
        return arrow::Status::Invalid("indptr and indices cannot be null");
    }
    if (num_vertices <= 0) {
        return arrow::Status::Invalid("num_vertices must be positive");
    }

    const int64_t* indptr_data = indptr->raw_values();
    const int64_t* indices_data = indices->raw_values();

    std::vector<int64_t> component_ids(num_vertices, -1);
    int64_t component_counter = 0;

    // BFS from each unvisited vertex
    for (int64_t start = 0; start < num_vertices; start++) {
        if (component_ids[start] != -1) {
            continue;  // Already visited
        }

        // BFS from start vertex
        std::queue<int64_t> Q;
        Q.push(start);
        component_ids[start] = component_counter;

        while (!Q.empty()) {
            int64_t u = Q.front();
            Q.pop();

            int64_t start_u = indptr_data[u];
            int64_t end_u = indptr_data[u + 1];

            for (int64_t i = start_u; i < end_u; i++) {
                int64_t v = indices_data[i];
                if (v >= 0 && v < num_vertices && component_ids[v] == -1) {
                    component_ids[v] = component_counter;
                    Q.push(v);
                }
            }
        }

        component_counter++;
    }

    int64_t num_components = component_counter;

    // Compute component sizes
    std::vector<int64_t> component_sizes(num_components, 0);
    for (int64_t v = 0; v < num_vertices; v++) {
        component_sizes[component_ids[v]]++;
    }

    // Find largest component
    int64_t largest_size = 0;
    for (int64_t size : component_sizes) {
        if (size > largest_size) {
            largest_size = size;
        }
    }

    // Build Arrow arrays
    arrow::Int64Builder component_id_builder;
    arrow::Int64Builder component_size_builder;

    ARROW_RETURN_NOT_OK(component_id_builder.AppendValues(component_ids));
    ARROW_RETURN_NOT_OK(component_size_builder.AppendValues(component_sizes));

    std::shared_ptr<arrow::Int64Array> component_id_array;
    std::shared_ptr<arrow::Int64Array> component_size_array;
    ARROW_RETURN_NOT_OK(component_id_builder.Finish(&component_id_array));
    ARROW_RETURN_NOT_OK(component_size_builder.Finish(&component_size_array));

    ConnectedComponentsResult result;
    result.component_ids = component_id_array;
    result.num_components = num_components;
    result.component_sizes = component_size_array;
    result.largest_component_size = largest_size;
    result.num_vertices = num_vertices;

    return result;
}

/**
 * Get vertices in the largest connected component.
 */
arrow::Result<std::shared_ptr<arrow::Int64Array>> GetLargestComponent(
    const std::shared_ptr<arrow::Int64Array>& component_ids,
    const std::shared_ptr<arrow::Int64Array>& component_sizes
) {
    if (!component_ids || !component_sizes) {
        return arrow::Status::Invalid("component_ids and component_sizes cannot be null");
    }

    const int64_t* ids_data = component_ids->raw_values();
    const int64_t* sizes_data = component_sizes->raw_values();

    int64_t num_vertices = component_ids->length();
    int64_t num_components = component_sizes->length();

    // Find largest component ID
    int64_t largest_component_id = 0;
    int64_t largest_size = 0;
    for (int64_t i = 0; i < num_components; i++) {
        if (sizes_data[i] > largest_size) {
            largest_size = sizes_data[i];
            largest_component_id = i;
        }
    }

    // Collect vertices in largest component
    std::vector<int64_t> vertices;
    vertices.reserve(largest_size);

    for (int64_t v = 0; v < num_vertices; v++) {
        if (ids_data[v] == largest_component_id) {
            vertices.push_back(v);
        }
    }

    // Build Arrow array
    arrow::Int64Builder builder;
    ARROW_RETURN_NOT_OK(builder.AppendValues(vertices));

    std::shared_ptr<arrow::Int64Array> result_array;
    ARROW_RETURN_NOT_OK(builder.Finish(&result_array));

    return result_array;
}

/**
 * Get component statistics.
 */
arrow::Result<std::shared_ptr<arrow::RecordBatch>> ComponentStatistics(
    const std::shared_ptr<arrow::Int64Array>& component_ids,
    int64_t num_components
) {
    if (!component_ids) {
        return arrow::Status::Invalid("component_ids cannot be null");
    }

    const int64_t* ids_data = component_ids->raw_values();
    int64_t num_vertices = component_ids->length();

    // Compute sizes
    std::vector<int64_t> sizes(num_components, 0);
    for (int64_t v = 0; v < num_vertices; v++) {
        sizes[ids_data[v]]++;
    }

    // Build result arrays
    arrow::Int64Builder id_builder, size_builder;
    arrow::BooleanBuilder isolated_builder;

    for (int64_t i = 0; i < num_components; i++) {
        ARROW_RETURN_NOT_OK(id_builder.Append(i));
        ARROW_RETURN_NOT_OK(size_builder.Append(sizes[i]));
        ARROW_RETURN_NOT_OK(isolated_builder.Append(sizes[i] == 1));
    }

    std::shared_ptr<arrow::Int64Array> id_array, size_array;
    std::shared_ptr<arrow::BooleanArray> isolated_array;
    ARROW_RETURN_NOT_OK(id_builder.Finish(&id_array));
    ARROW_RETURN_NOT_OK(size_builder.Finish(&size_array));
    ARROW_RETURN_NOT_OK(isolated_builder.Finish(&isolated_array));

    auto schema = arrow::schema({
        arrow::field("component_id", arrow::int64()),
        arrow::field("size", arrow::int64()),
        arrow::field("isolated", arrow::boolean())
    });

    return arrow::RecordBatch::Make(schema, num_components, {id_array, size_array, isolated_array});
}

/**
 * Count isolated vertices.
 */
arrow::Result<int64_t> CountIsolatedVertices(
    const std::shared_ptr<arrow::Int64Array>& component_sizes
) {
    if (!component_sizes) {
        return arrow::Status::Invalid("component_sizes cannot be null");
    }

    const int64_t* sizes_data = component_sizes->raw_values();
    int64_t num_components = component_sizes->length();

    int64_t isolated_count = 0;
    for (int64_t i = 0; i < num_components; i++) {
        if (sizes_data[i] == 1) {
            isolated_count++;
        }
    }

    return isolated_count;
}

/**
 * Symmetrize graph by adding reverse edges.
 */
arrow::Result<std::pair<std::shared_ptr<arrow::Int64Array>, std::shared_ptr<arrow::Int64Array>>>
SymmetrizeGraph(
    const std::shared_ptr<arrow::Int64Array>& indptr,
    const std::shared_ptr<arrow::Int64Array>& indices,
    int64_t num_vertices
) {
    if (!indptr || !indices) {
        return arrow::Status::Invalid("indptr and indices cannot be null");
    }
    if (num_vertices <= 0) {
        return arrow::Status::Invalid("num_vertices must be positive");
    }

    const int64_t* indptr_data = indptr->raw_values();
    const int64_t* indices_data = indices->raw_values();

    // Build edge list with both forward and reverse edges
    std::vector<std::pair<int64_t, int64_t>> edges;

    // Add all existing edges
    for (int64_t u = 0; u < num_vertices; u++) {
        int64_t start = indptr_data[u];
        int64_t end = indptr_data[u + 1];

        for (int64_t i = start; i < end; i++) {
            int64_t v = indices_data[i];
            if (v >= 0 && v < num_vertices) {
                edges.push_back({u, v});
                // Add reverse edge if not a self-loop
                if (u != v) {
                    edges.push_back({v, u});
                }
            }
        }
    }

    // Sort edges for CSR construction
    std::sort(edges.begin(), edges.end());

    // Remove duplicates
    edges.erase(std::unique(edges.begin(), edges.end()), edges.end());

    // Build new CSR
    std::vector<int64_t> new_indptr(num_vertices + 1, 0);
    std::vector<int64_t> new_indices;
    new_indices.reserve(edges.size());

    int64_t current_vertex = 0;
    for (const auto& edge : edges) {
        // Fill indptr gaps for vertices with no outgoing edges
        while (current_vertex < edge.first) {
            new_indptr[current_vertex + 1] = new_indices.size();
            current_vertex++;
        }
        new_indices.push_back(edge.second);
    }

    // Fill remaining indptr entries
    while (current_vertex < num_vertices) {
        new_indptr[current_vertex + 1] = new_indices.size();
        current_vertex++;
    }

    // Build Arrow arrays
    arrow::Int64Builder indptr_builder, indices_builder;
    ARROW_RETURN_NOT_OK(indptr_builder.AppendValues(new_indptr));
    ARROW_RETURN_NOT_OK(indices_builder.AppendValues(new_indices));

    std::shared_ptr<arrow::Int64Array> new_indptr_array, new_indices_array;
    ARROW_RETURN_NOT_OK(indptr_builder.Finish(&new_indptr_array));
    ARROW_RETURN_NOT_OK(indices_builder.Finish(&new_indices_array));

    return std::make_pair(new_indptr_array, new_indices_array);
}

/**
 * Find weakly connected components (directed graph treated as undirected).
 */
arrow::Result<ConnectedComponentsResult> WeaklyConnectedComponents(
    const std::shared_ptr<arrow::Int64Array>& indptr,
    const std::shared_ptr<arrow::Int64Array>& indices,
    int64_t num_vertices
) {
    // Symmetrize the graph
    auto symmetrize_result = SymmetrizeGraph(indptr, indices, num_vertices);
    if (!symmetrize_result.ok()) {
        return symmetrize_result.status();
    }

    auto [sym_indptr, sym_indices] = symmetrize_result.ValueOrDie();

    // Run regular connected components on symmetrized graph
    return FindConnectedComponents(sym_indptr, sym_indices, num_vertices);
}

}  // namespace traversal
}  // namespace graph
}  // namespace sabot
