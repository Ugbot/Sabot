#include "centrality.h"
#include <arrow/builder.h>
#include <arrow/status.h>
#include <queue>
#include <vector>
#include <stack>
#include <map>
#include <algorithm>
#include <limits>
#include <cmath>

namespace sabot {
namespace graph {
namespace traversal {

/**
 * Brandes' algorithm for betweenness centrality.
 */
arrow::Result<BetweennessCentralityResult> BetweennessCentrality(
    const std::shared_ptr<arrow::Int64Array>& indptr,
    const std::shared_ptr<arrow::Int64Array>& indices,
    int64_t num_vertices,
    bool normalized
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

    // Initialize betweenness scores to 0
    std::vector<double> betweenness(num_vertices, 0.0);

    // Brandes' algorithm: compute betweenness for all vertices
    for (int64_t source = 0; source < num_vertices; source++) {
        // Data structures for single-source shortest paths
        std::vector<std::vector<int64_t>> predecessors(num_vertices);
        std::vector<int64_t> distances(num_vertices, -1);
        std::vector<int64_t> num_paths(num_vertices, 0);
        std::stack<int64_t> S;  // Stack of vertices in order of non-increasing distance
        std::queue<int64_t> Q;  // BFS queue

        // Initialize source
        distances[source] = 0;
        num_paths[source] = 1;
        Q.push(source);

        // BFS to find shortest paths
        while (!Q.empty()) {
            int64_t v = Q.front();
            Q.pop();
            S.push(v);  // Save for dependency accumulation

            // Explore neighbors
            int64_t start = indptr_data[v];
            int64_t end = indptr_data[v + 1];
            for (int64_t i = start; i < end; i++) {
                int64_t w = indices_data[i];
                if (w < 0 || w >= num_vertices) continue;

                // First time visiting w?
                if (distances[w] < 0) {
                    distances[w] = distances[v] + 1;
                    Q.push(w);
                }

                // Shortest path to w via v?
                if (distances[w] == distances[v] + 1) {
                    num_paths[w] += num_paths[v];
                    predecessors[w].push_back(v);
                }
            }
        }

        // Dependency accumulation (backpropagation)
        std::vector<double> dependency(num_vertices, 0.0);

        // S contains vertices in order of non-increasing distance from source
        while (!S.empty()) {
            int64_t w = S.top();
            S.pop();

            // Accumulate dependency from w to its predecessors
            for (int64_t v : predecessors[w]) {
                // Fraction of shortest paths through v to w
                double fraction = static_cast<double>(num_paths[v]) / num_paths[w];
                dependency[v] += fraction * (1.0 + dependency[w]);
            }

            // Add to betweenness (exclude source)
            if (w != source) {
                betweenness[w] += dependency[w];
            }
        }
    }

    // Normalization
    if (normalized && num_vertices > 2) {
        // For undirected graphs: divide by (n-1)(n-2)/2
        // For directed graphs: divide by (n-1)(n-2)
        // We assume undirected here (user can adjust)
        double normalization_factor = (num_vertices - 1) * (num_vertices - 2) / 2.0;
        for (double& score : betweenness) {
            score /= normalization_factor;
        }
    }

    // Build Arrow array
    arrow::DoubleBuilder builder;
    ARROW_RETURN_NOT_OK(builder.AppendValues(betweenness));
    std::shared_ptr<arrow::DoubleArray> centrality_array;
    ARROW_RETURN_NOT_OK(builder.Finish(&centrality_array));

    BetweennessCentralityResult result;
    result.centrality = centrality_array;
    result.num_vertices = num_vertices;
    result.normalized = normalized;

    return result;
}

/**
 * Weighted betweenness centrality using Dijkstra.
 */
arrow::Result<BetweennessCentralityResult> WeightedBetweennessCentrality(
    const std::shared_ptr<arrow::Int64Array>& indptr,
    const std::shared_ptr<arrow::Int64Array>& indices,
    const std::shared_ptr<arrow::DoubleArray>& weights,
    int64_t num_vertices,
    bool normalized
) {
    // Validate inputs
    if (!weights) {
        return arrow::Status::Invalid("weights cannot be null");
    }
    if (weights->length() != indices->length()) {
        return arrow::Status::Invalid("weights length must match indices length");
    }

    const int64_t* indptr_data = indptr->raw_values();
    const int64_t* indices_data = indices->raw_values();
    const double* weights_data = weights->raw_values();

    // Initialize betweenness scores
    std::vector<double> betweenness(num_vertices, 0.0);

    // Weighted betweenness using Dijkstra
    for (int64_t source = 0; source < num_vertices; source++) {
        // Priority queue for Dijkstra: (distance, vertex)
        struct Node {
            double distance;
            int64_t vertex;
            bool operator>(const Node& other) const {
                return distance > other.distance;
            }
        };
        std::priority_queue<Node, std::vector<Node>, std::greater<Node>> pq;

        std::vector<double> distances(num_vertices, std::numeric_limits<double>::infinity());
        std::vector<int64_t> num_paths(num_vertices, 0);
        std::vector<std::vector<int64_t>> predecessors(num_vertices);
        std::stack<int64_t> S;

        // Initialize source
        distances[source] = 0.0;
        num_paths[source] = 1;
        pq.push({0.0, source});

        // Dijkstra to find shortest paths
        while (!pq.empty()) {
            Node current = pq.top();
            pq.pop();

            int64_t v = current.vertex;
            double dist_v = current.distance;

            // Skip if we've already processed this vertex with a shorter distance
            if (dist_v > distances[v]) continue;

            S.push(v);

            // Explore neighbors
            int64_t start = indptr_data[v];
            int64_t end = indptr_data[v + 1];
            for (int64_t i = start; i < end; i++) {
                int64_t w = indices_data[i];
                double weight = weights_data[i];

                if (w < 0 || w >= num_vertices) continue;
                if (weight < 0.0) {
                    return arrow::Status::Invalid("Edge weights must be non-negative");
                }

                double new_dist = dist_v + weight;

                // Found a shorter path to w?
                if (new_dist < distances[w]) {
                    distances[w] = new_dist;
                    num_paths[w] = num_paths[v];
                    predecessors[w].clear();
                    predecessors[w].push_back(v);
                    pq.push({new_dist, w});
                }
                // Found another shortest path to w?
                else if (std::abs(new_dist - distances[w]) < 1e-9) {
                    num_paths[w] += num_paths[v];
                    predecessors[w].push_back(v);
                }
            }
        }

        // Dependency accumulation
        std::vector<double> dependency(num_vertices, 0.0);

        while (!S.empty()) {
            int64_t w = S.top();
            S.pop();

            for (int64_t v : predecessors[w]) {
                if (num_paths[w] > 0) {
                    double fraction = static_cast<double>(num_paths[v]) / num_paths[w];
                    dependency[v] += fraction * (1.0 + dependency[w]);
                }
            }

            if (w != source) {
                betweenness[w] += dependency[w];
            }
        }
    }

    // Normalization
    if (normalized && num_vertices > 2) {
        double normalization_factor = (num_vertices - 1) * (num_vertices - 2) / 2.0;
        for (double& score : betweenness) {
            score /= normalization_factor;
        }
    }

    // Build Arrow array
    arrow::DoubleBuilder builder;
    ARROW_RETURN_NOT_OK(builder.AppendValues(betweenness));
    std::shared_ptr<arrow::DoubleArray> centrality_array;
    ARROW_RETURN_NOT_OK(builder.Finish(&centrality_array));

    BetweennessCentralityResult result;
    result.centrality = centrality_array;
    result.num_vertices = num_vertices;
    result.normalized = normalized;

    return result;
}

/**
 * Edge betweenness centrality.
 */
arrow::Result<std::shared_ptr<arrow::RecordBatch>> EdgeBetweennessCentrality(
    const std::shared_ptr<arrow::Int64Array>& indptr,
    const std::shared_ptr<arrow::Int64Array>& indices,
    int64_t num_vertices
) {
    const int64_t* indptr_data = indptr->raw_values();
    const int64_t* indices_data = indices->raw_values();
    int64_t num_edges = indices->length();

    // Map (src, dst) -> edge index
    std::map<std::pair<int64_t, int64_t>, int64_t> edge_map;
    for (int64_t v = 0; v < num_vertices; v++) {
        int64_t start = indptr_data[v];
        int64_t end = indptr_data[v + 1];
        for (int64_t i = start; i < end; i++) {
            int64_t w = indices_data[i];
            if (w >= 0 && w < num_vertices) {
                edge_map[{v, w}] = i;
            }
        }
    }

    // Initialize edge betweenness scores
    std::vector<double> edge_betweenness(num_edges, 0.0);

    // Compute edge betweenness using Brandes' algorithm variant
    for (int64_t source = 0; source < num_vertices; source++) {
        std::vector<std::vector<int64_t>> predecessors(num_vertices);
        std::vector<int64_t> distances(num_vertices, -1);
        std::vector<int64_t> num_paths(num_vertices, 0);
        std::stack<int64_t> S;
        std::queue<int64_t> Q;

        distances[source] = 0;
        num_paths[source] = 1;
        Q.push(source);

        // BFS
        while (!Q.empty()) {
            int64_t v = Q.front();
            Q.pop();
            S.push(v);

            int64_t start = indptr_data[v];
            int64_t end = indptr_data[v + 1];
            for (int64_t i = start; i < end; i++) {
                int64_t w = indices_data[i];
                if (w < 0 || w >= num_vertices) continue;

                if (distances[w] < 0) {
                    distances[w] = distances[v] + 1;
                    Q.push(w);
                }

                if (distances[w] == distances[v] + 1) {
                    num_paths[w] += num_paths[v];
                    predecessors[w].push_back(v);
                }
            }
        }

        // Edge dependency accumulation
        std::vector<double> dependency(num_vertices, 0.0);

        while (!S.empty()) {
            int64_t w = S.top();
            S.pop();

            for (int64_t v : predecessors[w]) {
                double fraction = static_cast<double>(num_paths[v]) / num_paths[w];
                double edge_contrib = fraction * (1.0 + dependency[w]);

                // Add to edge (v, w)
                auto it = edge_map.find({v, w});
                if (it != edge_map.end()) {
                    edge_betweenness[it->second] += edge_contrib;
                }

                dependency[v] += edge_contrib;
            }
        }
    }

    // Build result RecordBatch
    arrow::Int64Builder src_builder, dst_builder;
    arrow::DoubleBuilder betweenness_builder;

    for (const auto& entry : edge_map) {
        int64_t src = entry.first.first;
        int64_t dst = entry.first.second;
        int64_t edge_idx = entry.second;

        ARROW_RETURN_NOT_OK(src_builder.Append(src));
        ARROW_RETURN_NOT_OK(dst_builder.Append(dst));
        ARROW_RETURN_NOT_OK(betweenness_builder.Append(edge_betweenness[edge_idx]));
    }

    std::shared_ptr<arrow::Int64Array> src_array, dst_array;
    std::shared_ptr<arrow::DoubleArray> betweenness_array;
    ARROW_RETURN_NOT_OK(src_builder.Finish(&src_array));
    ARROW_RETURN_NOT_OK(dst_builder.Finish(&dst_array));
    ARROW_RETURN_NOT_OK(betweenness_builder.Finish(&betweenness_array));

    auto schema = arrow::schema({
        arrow::field("src", arrow::int64()),
        arrow::field("dst", arrow::int64()),
        arrow::field("betweenness", arrow::float64())
    });

    return arrow::RecordBatch::Make(schema, edge_map.size(), {src_array, dst_array, betweenness_array});
}

/**
 * Get top-K vertices by betweenness centrality.
 */
arrow::Result<std::shared_ptr<arrow::RecordBatch>> TopKBetweenness(
    const std::shared_ptr<arrow::DoubleArray>& centrality,
    int64_t k
) {
    if (!centrality) {
        return arrow::Status::Invalid("centrality cannot be null");
    }

    int64_t num_vertices = centrality->length();
    if (k <= 0 || k > num_vertices) {
        k = num_vertices;
    }

    const double* centrality_data = centrality->raw_values();

    // Create vector of (vertex_id, betweenness) pairs
    std::vector<std::pair<int64_t, double>> vertex_scores;
    vertex_scores.reserve(num_vertices);
    for (int64_t v = 0; v < num_vertices; v++) {
        vertex_scores.push_back({v, centrality_data[v]});
    }

    // Partial sort to get top-K
    if (k < num_vertices) {
        std::partial_sort(
            vertex_scores.begin(),
            vertex_scores.begin() + k,
            vertex_scores.end(),
            [](const auto& a, const auto& b) { return a.second > b.second; }
        );
        vertex_scores.resize(k);
    } else {
        std::sort(
            vertex_scores.begin(),
            vertex_scores.end(),
            [](const auto& a, const auto& b) { return a.second > b.second; }
        );
    }

    // Build result arrays
    arrow::Int64Builder vertex_builder;
    arrow::DoubleBuilder betweenness_builder;

    ARROW_RETURN_NOT_OK(vertex_builder.Reserve(k));
    ARROW_RETURN_NOT_OK(betweenness_builder.Reserve(k));

    for (const auto& pair : vertex_scores) {
        ARROW_RETURN_NOT_OK(vertex_builder.Append(pair.first));
        ARROW_RETURN_NOT_OK(betweenness_builder.Append(pair.second));
    }

    std::shared_ptr<arrow::Int64Array> vertex_array;
    std::shared_ptr<arrow::DoubleArray> betweenness_array;
    ARROW_RETURN_NOT_OK(vertex_builder.Finish(&vertex_array));
    ARROW_RETURN_NOT_OK(betweenness_builder.Finish(&betweenness_array));

    auto schema = arrow::schema({
        arrow::field("vertex_id", arrow::int64()),
        arrow::field("betweenness", arrow::float64())
    });

    return arrow::RecordBatch::Make(schema, k, {vertex_array, betweenness_array});
}

}  // namespace traversal
}  // namespace graph
}  // namespace sabot
