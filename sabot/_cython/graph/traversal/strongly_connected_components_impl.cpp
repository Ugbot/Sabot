#include "strongly_connected_components.h"
#include <arrow/builder.h>
#include <arrow/status.h>
#include <vector>
#include <stack>
#include <algorithm>
#include <unordered_set>
#include <unordered_map>

namespace sabot {
namespace graph {
namespace traversal {

/**
 * Tarjan's algorithm for finding strongly connected components.
 *
 * Uses DFS with discovery time (index) and low-link values.
 * Maintains a stack of vertices in the current SCC being discovered.
 */
class TarjanSCC {
public:
    TarjanSCC(
        const int64_t* indptr_data,
        const int64_t* indices_data,
        int64_t num_vertices
    ) : indptr_data_(indptr_data),
        indices_data_(indices_data),
        num_vertices_(num_vertices),
        index_counter_(0),
        scc_counter_(0) {

        index_.resize(num_vertices, -1);
        lowlink_.resize(num_vertices, -1);
        on_stack_.resize(num_vertices, false);
        component_ids_.resize(num_vertices, -1);
    }

    void Run() {
        // Run DFS from each unvisited vertex
        for (int64_t v = 0; v < num_vertices_; v++) {
            if (index_[v] == -1) {
                StrongConnect(v);
            }
        }
    }

    const std::vector<int64_t>& GetComponentIds() const {
        return component_ids_;
    }

    int64_t GetNumComponents() const {
        return scc_counter_;
    }

private:
    void StrongConnect(int64_t v) {
        // Set discovery time and low-link value
        index_[v] = index_counter_;
        lowlink_[v] = index_counter_;
        index_counter_++;

        stack_.push(v);
        on_stack_[v] = true;

        // Visit neighbors
        int64_t start = indptr_data_[v];
        int64_t end = indptr_data_[v + 1];

        for (int64_t i = start; i < end; i++) {
            int64_t w = indices_data_[i];

            if (w < 0 || w >= num_vertices_) {
                continue;  // Skip invalid neighbors
            }

            if (index_[w] == -1) {
                // Neighbor not visited, recurse
                StrongConnect(w);
                lowlink_[v] = std::min(lowlink_[v], lowlink_[w]);
            } else if (on_stack_[w]) {
                // Neighbor is on stack (part of current SCC)
                lowlink_[v] = std::min(lowlink_[v], index_[w]);
            }
        }

        // If v is a root node, pop the stack and generate an SCC
        if (lowlink_[v] == index_[v]) {
            while (true) {
                int64_t w = stack_.top();
                stack_.pop();
                on_stack_[w] = false;
                component_ids_[w] = scc_counter_;

                if (w == v) {
                    break;
                }
            }
            scc_counter_++;
        }
    }

    const int64_t* indptr_data_;
    const int64_t* indices_data_;
    int64_t num_vertices_;

    int64_t index_counter_;
    int64_t scc_counter_;

    std::vector<int64_t> index_;
    std::vector<int64_t> lowlink_;
    std::vector<bool> on_stack_;
    std::vector<int64_t> component_ids_;
    std::stack<int64_t> stack_;
};

/**
 * Find strongly connected components using Tarjan's algorithm.
 */
arrow::Result<StronglyConnectedComponentsResult> FindStronglyConnectedComponents(
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

    // Run Tarjan's algorithm
    TarjanSCC tarjan(indptr_data, indices_data, num_vertices);
    tarjan.Run();

    const std::vector<int64_t>& component_ids = tarjan.GetComponentIds();
    int64_t num_components = tarjan.GetNumComponents();

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

    StronglyConnectedComponentsResult result;
    result.component_ids = component_id_array;
    result.num_components = num_components;
    result.component_sizes = component_size_array;
    result.largest_component_size = largest_size;
    result.num_vertices = num_vertices;

    return result;
}

/**
 * Get SCC membership as a RecordBatch.
 */
arrow::Result<std::shared_ptr<arrow::RecordBatch>> GetSCCMembership(
    const std::shared_ptr<arrow::Int64Array>& indptr,
    const std::shared_ptr<arrow::Int64Array>& indices,
    int64_t num_vertices
) {
    auto scc_result = FindStronglyConnectedComponents(indptr, indices, num_vertices);
    if (!scc_result.ok()) {
        return scc_result.status();
    }

    StronglyConnectedComponentsResult result = scc_result.ValueOrDie();

    const int64_t* component_ids_data = result.component_ids->raw_values();

    // Build membership arrays
    arrow::Int64Builder component_id_builder;
    arrow::Int64Builder vertex_id_builder;

    for (int64_t v = 0; v < num_vertices; v++) {
        ARROW_RETURN_NOT_OK(component_id_builder.Append(component_ids_data[v]));
        ARROW_RETURN_NOT_OK(vertex_id_builder.Append(v));
    }

    std::shared_ptr<arrow::Int64Array> component_id_array;
    std::shared_ptr<arrow::Int64Array> vertex_id_array;
    ARROW_RETURN_NOT_OK(component_id_builder.Finish(&component_id_array));
    ARROW_RETURN_NOT_OK(vertex_id_builder.Finish(&vertex_id_array));

    auto schema = arrow::schema({
        arrow::field("component_id", arrow::int64()),
        arrow::field("vertex_id", arrow::int64())
    });

    return arrow::RecordBatch::Make(schema, num_vertices, {component_id_array, vertex_id_array});
}

/**
 * Get vertices in the largest strongly connected component.
 */
arrow::Result<std::shared_ptr<arrow::Int64Array>> GetLargestSCC(
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

    // Find largest SCC ID
    int64_t largest_scc_id = 0;
    int64_t largest_size = 0;
    for (int64_t i = 0; i < num_components; i++) {
        if (sizes_data[i] > largest_size) {
            largest_size = sizes_data[i];
            largest_scc_id = i;
        }
    }

    // Collect vertices in largest SCC
    std::vector<int64_t> vertices;
    vertices.reserve(largest_size);

    for (int64_t v = 0; v < num_vertices; v++) {
        if (ids_data[v] == largest_scc_id) {
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
 * Check if graph is strongly connected.
 */
arrow::Result<bool> IsStronglyConnected(
    const std::shared_ptr<arrow::Int64Array>& indptr,
    const std::shared_ptr<arrow::Int64Array>& indices,
    int64_t num_vertices
) {
    auto scc_result = FindStronglyConnectedComponents(indptr, indices, num_vertices);
    if (!scc_result.ok()) {
        return scc_result.status();
    }

    return scc_result.ValueOrDie().num_components == 1;
}

/**
 * Condense graph into DAG of SCCs.
 */
arrow::Result<std::pair<std::shared_ptr<arrow::Int64Array>, std::shared_ptr<arrow::Int64Array>>>
CondenseGraph(
    const std::shared_ptr<arrow::Int64Array>& indptr,
    const std::shared_ptr<arrow::Int64Array>& indices,
    int64_t num_vertices
) {
    // First find SCCs
    auto scc_result = FindStronglyConnectedComponents(indptr, indices, num_vertices);
    if (!scc_result.ok()) {
        return scc_result.status();
    }

    StronglyConnectedComponentsResult result = scc_result.ValueOrDie();
    const int64_t* component_ids_data = result.component_ids->raw_values();
    const int64_t* indptr_data = indptr->raw_values();
    const int64_t* indices_data = indices->raw_values();
    int64_t num_components = result.num_components;

    // Build condensed graph edges (between SCCs)
    std::vector<std::pair<int64_t, int64_t>> condensed_edges;

    for (int64_t u = 0; u < num_vertices; u++) {
        int64_t scc_u = component_ids_data[u];
        int64_t start = indptr_data[u];
        int64_t end = indptr_data[u + 1];

        for (int64_t i = start; i < end; i++) {
            int64_t v = indices_data[i];
            if (v >= 0 && v < num_vertices) {
                int64_t scc_v = component_ids_data[v];

                // Only add edge if SCCs are different
                if (scc_u != scc_v) {
                    condensed_edges.push_back({scc_u, scc_v});
                }
            }
        }
    }

    // Sort and deduplicate edges
    std::sort(condensed_edges.begin(), condensed_edges.end());
    condensed_edges.erase(
        std::unique(condensed_edges.begin(), condensed_edges.end()),
        condensed_edges.end()
    );

    // Build CSR for condensed graph
    std::vector<int64_t> condensed_indptr(num_components + 1, 0);
    std::vector<int64_t> condensed_indices;
    condensed_indices.reserve(condensed_edges.size());

    int64_t current_scc = 0;
    for (const auto& edge : condensed_edges) {
        // Fill indptr gaps
        while (current_scc < edge.first) {
            condensed_indptr[current_scc + 1] = condensed_indices.size();
            current_scc++;
        }
        condensed_indices.push_back(edge.second);
    }

    // Fill remaining indptr entries
    while (current_scc < num_components) {
        condensed_indptr[current_scc + 1] = condensed_indices.size();
        current_scc++;
    }

    // Build Arrow arrays
    arrow::Int64Builder indptr_builder, indices_builder;
    ARROW_RETURN_NOT_OK(indptr_builder.AppendValues(condensed_indptr));
    ARROW_RETURN_NOT_OK(indices_builder.AppendValues(condensed_indices));

    std::shared_ptr<arrow::Int64Array> condensed_indptr_array;
    std::shared_ptr<arrow::Int64Array> condensed_indices_array;
    ARROW_RETURN_NOT_OK(indptr_builder.Finish(&condensed_indptr_array));
    ARROW_RETURN_NOT_OK(indices_builder.Finish(&condensed_indices_array));

    return std::make_pair(condensed_indptr_array, condensed_indices_array);
}

/**
 * Find source SCCs (no incoming edges from other SCCs).
 */
arrow::Result<std::shared_ptr<arrow::Int64Array>> FindSourceSCCs(
    const std::shared_ptr<arrow::Int64Array>& component_ids,
    const std::shared_ptr<arrow::Int64Array>& indptr,
    const std::shared_ptr<arrow::Int64Array>& indices,
    int64_t num_vertices
) {
    if (!component_ids || !indptr || !indices) {
        return arrow::Status::Invalid("Input arrays cannot be null");
    }

    const int64_t* component_ids_data = component_ids->raw_values();
    const int64_t* indptr_data = indptr->raw_values();
    const int64_t* indices_data = indices->raw_values();

    // Find which SCCs have incoming edges from other SCCs
    std::unordered_set<int64_t> has_incoming;

    for (int64_t u = 0; u < num_vertices; u++) {
        int64_t scc_u = component_ids_data[u];
        int64_t start = indptr_data[u];
        int64_t end = indptr_data[u + 1];

        for (int64_t i = start; i < end; i++) {
            int64_t v = indices_data[i];
            if (v >= 0 && v < num_vertices) {
                int64_t scc_v = component_ids_data[v];

                // If u -> v and they're in different SCCs, then scc_v has incoming
                if (scc_u != scc_v) {
                    has_incoming.insert(scc_v);
                }
            }
        }
    }

    // Find unique SCC IDs
    std::unordered_set<int64_t> all_sccs;
    for (int64_t v = 0; v < num_vertices; v++) {
        all_sccs.insert(component_ids_data[v]);
    }

    // Source SCCs = all SCCs - SCCs with incoming
    std::vector<int64_t> sources;
    for (int64_t scc : all_sccs) {
        if (has_incoming.find(scc) == has_incoming.end()) {
            sources.push_back(scc);
        }
    }

    std::sort(sources.begin(), sources.end());

    // Build Arrow array
    arrow::Int64Builder builder;
    ARROW_RETURN_NOT_OK(builder.AppendValues(sources));

    std::shared_ptr<arrow::Int64Array> result_array;
    ARROW_RETURN_NOT_OK(builder.Finish(&result_array));

    return result_array;
}

/**
 * Find sink SCCs (no outgoing edges to other SCCs).
 */
arrow::Result<std::shared_ptr<arrow::Int64Array>> FindSinkSCCs(
    const std::shared_ptr<arrow::Int64Array>& component_ids,
    const std::shared_ptr<arrow::Int64Array>& indptr,
    const std::shared_ptr<arrow::Int64Array>& indices,
    int64_t num_vertices
) {
    if (!component_ids || !indptr || !indices) {
        return arrow::Status::Invalid("Input arrays cannot be null");
    }

    const int64_t* component_ids_data = component_ids->raw_values();
    const int64_t* indptr_data = indptr->raw_values();
    const int64_t* indices_data = indices->raw_values();

    // Find which SCCs have outgoing edges to other SCCs
    std::unordered_set<int64_t> has_outgoing;

    for (int64_t u = 0; u < num_vertices; u++) {
        int64_t scc_u = component_ids_data[u];
        int64_t start = indptr_data[u];
        int64_t end = indptr_data[u + 1];

        for (int64_t i = start; i < end; i++) {
            int64_t v = indices_data[i];
            if (v >= 0 && v < num_vertices) {
                int64_t scc_v = component_ids_data[v];

                // If u -> v and they're in different SCCs, then scc_u has outgoing
                if (scc_u != scc_v) {
                    has_outgoing.insert(scc_u);
                }
            }
        }
    }

    // Find unique SCC IDs
    std::unordered_set<int64_t> all_sccs;
    for (int64_t v = 0; v < num_vertices; v++) {
        all_sccs.insert(component_ids_data[v]);
    }

    // Sink SCCs = all SCCs - SCCs with outgoing
    std::vector<int64_t> sinks;
    for (int64_t scc : all_sccs) {
        if (has_outgoing.find(scc) == has_outgoing.end()) {
            sinks.push_back(scc);
        }
    }

    std::sort(sinks.begin(), sinks.end());

    // Build Arrow array
    arrow::Int64Builder builder;
    ARROW_RETURN_NOT_OK(builder.AppendValues(sinks));

    std::shared_ptr<arrow::Int64Array> result_array;
    ARROW_RETURN_NOT_OK(builder.Finish(&result_array));

    return result_array;
}

}  // namespace traversal
}  // namespace graph
}  // namespace sabot
