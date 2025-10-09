#include "pagerank.h"
#include <arrow/builder.h>
#include <arrow/status.h>
#include <cmath>
#include <vector>
#include <algorithm>
#include <numeric>

namespace sabot {
namespace graph {
namespace traversal {

/**
 * Compute PageRank using power iteration.
 */
arrow::Result<PageRankResult> PageRank(
    const std::shared_ptr<arrow::Int64Array>& indptr,
    const std::shared_ptr<arrow::Int64Array>& indices,
    int64_t num_vertices,
    double damping,
    int64_t max_iterations,
    double tolerance
) {
    // Validate inputs
    if (!indptr || !indices) {
        return arrow::Status::Invalid("indptr and indices cannot be null");
    }
    if (num_vertices <= 0) {
        return arrow::Status::Invalid("num_vertices must be positive");
    }
    if (damping < 0.0 || damping > 1.0) {
        return arrow::Status::Invalid("damping must be in [0, 1]");
    }
    if (indptr->length() != num_vertices + 1) {
        return arrow::Status::Invalid("indptr length must be num_vertices + 1");
    }

    const int64_t* indptr_data = indptr->raw_values();
    const int64_t* indices_data = indices->raw_values();

    // Initialize ranks (uniform distribution)
    std::vector<double> ranks(num_vertices, 1.0 / num_vertices);
    std::vector<double> new_ranks(num_vertices, 0.0);

    // Compute out-degrees for each vertex
    std::vector<int64_t> out_degrees(num_vertices, 0);
    for (int64_t v = 0; v < num_vertices; v++) {
        out_degrees[v] = indptr_data[v + 1] - indptr_data[v];
    }

    // Build reverse graph (incoming edges) for efficient PageRank computation
    // For each edge u→v, we need to track v's incoming edges from u
    std::vector<std::vector<int64_t>> incoming_edges(num_vertices);
    for (int64_t v = 0; v < num_vertices; v++) {
        int64_t start = indptr_data[v];
        int64_t end = indptr_data[v + 1];
        for (int64_t i = start; i < end; i++) {
            int64_t neighbor = indices_data[i];
            if (neighbor >= 0 && neighbor < num_vertices) {
                incoming_edges[neighbor].push_back(v);
            }
        }
    }

    // Power iteration
    double teleport = (1.0 - damping) / num_vertices;
    bool converged = false;
    int64_t iteration = 0;

    for (; iteration < max_iterations; iteration++) {
        // Compute new ranks
        for (int64_t v = 0; v < num_vertices; v++) {
            double rank_sum = 0.0;

            // Sum contributions from incoming edges
            for (int64_t u : incoming_edges[v]) {
                if (out_degrees[u] > 0) {
                    rank_sum += ranks[u] / out_degrees[u];
                }
            }

            // PageRank formula: (1-d)/N + d * sum(rank[u] / out_degree[u])
            new_ranks[v] = teleport + damping * rank_sum;
        }

        // Check convergence (max absolute difference)
        double max_delta = 0.0;
        for (int64_t v = 0; v < num_vertices; v++) {
            double delta = std::abs(new_ranks[v] - ranks[v]);
            max_delta = std::max(max_delta, delta);
        }

        // Swap ranks
        std::swap(ranks, new_ranks);

        // Check for convergence
        if (max_delta < tolerance) {
            converged = true;
            iteration++;  // Count this iteration
            break;
        }
    }

    // Normalize ranks to sum to 1.0 (for numerical stability)
    double rank_sum = std::accumulate(ranks.begin(), ranks.end(), 0.0);
    if (rank_sum > 0.0) {
        for (double& rank : ranks) {
            rank /= rank_sum;
        }
    }

    // Build Arrow array for ranks
    arrow::DoubleBuilder rank_builder;
    ARROW_RETURN_NOT_OK(rank_builder.AppendValues(ranks));
    std::shared_ptr<arrow::DoubleArray> ranks_array;
    ARROW_RETURN_NOT_OK(rank_builder.Finish(&ranks_array));

    // Compute final delta
    double final_delta = 0.0;
    if (iteration > 0) {
        for (int64_t v = 0; v < num_vertices; v++) {
            double delta = std::abs(new_ranks[v] - ranks[v]);
            final_delta = std::max(final_delta, delta);
        }
    }

    // Return result
    PageRankResult result;
    result.ranks = ranks_array;
    result.num_vertices = num_vertices;
    result.num_iterations = iteration;
    result.final_delta = final_delta;
    result.converged = converged;

    return result;
}

/**
 * Compute Personalized PageRank.
 */
arrow::Result<PageRankResult> PersonalizedPageRank(
    const std::shared_ptr<arrow::Int64Array>& indptr,
    const std::shared_ptr<arrow::Int64Array>& indices,
    int64_t num_vertices,
    const std::shared_ptr<arrow::Int64Array>& source_vertices,
    double damping,
    int64_t max_iterations,
    double tolerance
) {
    // Validate inputs
    if (!source_vertices || source_vertices->length() == 0) {
        return arrow::Status::Invalid("source_vertices cannot be empty");
    }

    const int64_t* indptr_data = indptr->raw_values();
    const int64_t* indices_data = indices->raw_values();
    const int64_t* sources_data = source_vertices->raw_values();
    int64_t num_sources = source_vertices->length();

    // Build source set for fast lookup
    std::vector<bool> is_source(num_vertices, false);
    for (int64_t i = 0; i < num_sources; i++) {
        int64_t src = sources_data[i];
        if (src >= 0 && src < num_vertices) {
            is_source[src] = true;
        }
    }

    // Initialize ranks (uniform over source vertices)
    std::vector<double> ranks(num_vertices, 0.0);
    double initial_rank = 1.0 / num_sources;
    for (int64_t i = 0; i < num_sources; i++) {
        int64_t src = sources_data[i];
        if (src >= 0 && src < num_vertices) {
            ranks[src] = initial_rank;
        }
    }

    std::vector<double> new_ranks(num_vertices, 0.0);

    // Compute out-degrees
    std::vector<int64_t> out_degrees(num_vertices, 0);
    for (int64_t v = 0; v < num_vertices; v++) {
        out_degrees[v] = indptr_data[v + 1] - indptr_data[v];
    }

    // Build reverse graph
    std::vector<std::vector<int64_t>> incoming_edges(num_vertices);
    for (int64_t v = 0; v < num_vertices; v++) {
        int64_t start = indptr_data[v];
        int64_t end = indptr_data[v + 1];
        for (int64_t i = start; i < end; i++) {
            int64_t neighbor = indices_data[i];
            if (neighbor >= 0 && neighbor < num_vertices) {
                incoming_edges[neighbor].push_back(v);
            }
        }
    }

    // Teleport probability (distributed over source vertices)
    double teleport = (1.0 - damping) / num_sources;

    bool converged = false;
    int64_t iteration = 0;

    for (; iteration < max_iterations; iteration++) {
        // Compute new ranks
        for (int64_t v = 0; v < num_vertices; v++) {
            double rank_sum = 0.0;

            // Sum contributions from incoming edges
            for (int64_t u : incoming_edges[v]) {
                if (out_degrees[u] > 0) {
                    rank_sum += ranks[u] / out_degrees[u];
                }
            }

            // Personalized PageRank: teleport only to source vertices
            double teleport_contrib = is_source[v] ? teleport : 0.0;
            new_ranks[v] = teleport_contrib + damping * rank_sum;
        }

        // Check convergence
        double max_delta = 0.0;
        for (int64_t v = 0; v < num_vertices; v++) {
            double delta = std::abs(new_ranks[v] - ranks[v]);
            max_delta = std::max(max_delta, delta);
        }

        std::swap(ranks, new_ranks);

        if (max_delta < tolerance) {
            converged = true;
            iteration++;
            break;
        }
    }

    // Normalize
    double rank_sum = std::accumulate(ranks.begin(), ranks.end(), 0.0);
    if (rank_sum > 0.0) {
        for (double& rank : ranks) {
            rank /= rank_sum;
        }
    }

    // Build Arrow array
    arrow::DoubleBuilder rank_builder;
    ARROW_RETURN_NOT_OK(rank_builder.AppendValues(ranks));
    std::shared_ptr<arrow::DoubleArray> ranks_array;
    ARROW_RETURN_NOT_OK(rank_builder.Finish(&ranks_array));

    PageRankResult result;
    result.ranks = ranks_array;
    result.num_vertices = num_vertices;
    result.num_iterations = iteration;
    result.final_delta = 0.0;  // Could compute if needed
    result.converged = converged;

    return result;
}

/**
 * Compute Weighted PageRank.
 */
arrow::Result<PageRankResult> WeightedPageRank(
    const std::shared_ptr<arrow::Int64Array>& indptr,
    const std::shared_ptr<arrow::Int64Array>& indices,
    const std::shared_ptr<arrow::DoubleArray>& weights,
    int64_t num_vertices,
    double damping,
    int64_t max_iterations,
    double tolerance
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

    // Initialize ranks
    std::vector<double> ranks(num_vertices, 1.0 / num_vertices);
    std::vector<double> new_ranks(num_vertices, 0.0);

    // Compute weighted out-degrees (sum of edge weights)
    std::vector<double> weighted_out_degrees(num_vertices, 0.0);
    for (int64_t v = 0; v < num_vertices; v++) {
        int64_t start = indptr_data[v];
        int64_t end = indptr_data[v + 1];
        for (int64_t i = start; i < end; i++) {
            if (weights_data[i] < 0.0) {
                return arrow::Status::Invalid("Edge weights must be non-negative");
            }
            weighted_out_degrees[v] += weights_data[i];
        }
    }

    // Build reverse graph with weights
    struct WeightedEdge {
        int64_t source;
        double weight;
    };
    std::vector<std::vector<WeightedEdge>> incoming_edges(num_vertices);

    for (int64_t v = 0; v < num_vertices; v++) {
        int64_t start = indptr_data[v];
        int64_t end = indptr_data[v + 1];
        for (int64_t i = start; i < end; i++) {
            int64_t neighbor = indices_data[i];
            double weight = weights_data[i];
            if (neighbor >= 0 && neighbor < num_vertices) {
                incoming_edges[neighbor].push_back({v, weight});
            }
        }
    }

    double teleport = (1.0 - damping) / num_vertices;
    bool converged = false;
    int64_t iteration = 0;

    for (; iteration < max_iterations; iteration++) {
        // Compute new ranks
        for (int64_t v = 0; v < num_vertices; v++) {
            double rank_sum = 0.0;

            // Sum weighted contributions from incoming edges
            for (const auto& edge : incoming_edges[v]) {
                int64_t u = edge.source;
                double weight = edge.weight;
                if (weighted_out_degrees[u] > 0.0) {
                    rank_sum += (ranks[u] * weight) / weighted_out_degrees[u];
                }
            }

            new_ranks[v] = teleport + damping * rank_sum;
        }

        // Check convergence
        double max_delta = 0.0;
        for (int64_t v = 0; v < num_vertices; v++) {
            double delta = std::abs(new_ranks[v] - ranks[v]);
            max_delta = std::max(max_delta, delta);
        }

        std::swap(ranks, new_ranks);

        if (max_delta < tolerance) {
            converged = true;
            iteration++;
            break;
        }
    }

    // Normalize
    double rank_sum = std::accumulate(ranks.begin(), ranks.end(), 0.0);
    if (rank_sum > 0.0) {
        for (double& rank : ranks) {
            rank /= rank_sum;
        }
    }

    // Build Arrow array
    arrow::DoubleBuilder rank_builder;
    ARROW_RETURN_NOT_OK(rank_builder.AppendValues(ranks));
    std::shared_ptr<arrow::DoubleArray> ranks_array;
    ARROW_RETURN_NOT_OK(rank_builder.Finish(&ranks_array));

    PageRankResult result;
    result.ranks = ranks_array;
    result.num_vertices = num_vertices;
    result.num_iterations = iteration;
    result.final_delta = 0.0;
    result.converged = converged;

    return result;
}

/**
 * Get top-K vertices by PageRank score.
 */
arrow::Result<std::shared_ptr<arrow::RecordBatch>> TopKPageRank(
    const std::shared_ptr<arrow::DoubleArray>& ranks,
    int64_t k
) {
    if (!ranks) {
        return arrow::Status::Invalid("ranks cannot be null");
    }

    int64_t num_vertices = ranks->length();
    if (k <= 0 || k > num_vertices) {
        k = num_vertices;  // Return all if k is invalid
    }

    const double* ranks_data = ranks->raw_values();

    // Create vector of (vertex_id, rank) pairs
    std::vector<std::pair<int64_t, double>> vertex_ranks;
    vertex_ranks.reserve(num_vertices);
    for (int64_t v = 0; v < num_vertices; v++) {
        vertex_ranks.push_back({v, ranks_data[v]});
    }

    // Partial sort to get top-K (descending by rank)
    if (k < num_vertices) {
        std::partial_sort(
            vertex_ranks.begin(),
            vertex_ranks.begin() + k,
            vertex_ranks.end(),
            [](const auto& a, const auto& b) { return a.second > b.second; }
        );
        vertex_ranks.resize(k);
    } else {
        std::sort(
            vertex_ranks.begin(),
            vertex_ranks.end(),
            [](const auto& a, const auto& b) { return a.second > b.second; }
        );
    }

    // Build result arrays
    arrow::Int64Builder vertex_builder;
    arrow::DoubleBuilder rank_builder;

    ARROW_RETURN_NOT_OK(vertex_builder.Reserve(k));
    ARROW_RETURN_NOT_OK(rank_builder.Reserve(k));

    for (const auto& pair : vertex_ranks) {
        ARROW_RETURN_NOT_OK(vertex_builder.Append(pair.first));
        ARROW_RETURN_NOT_OK(rank_builder.Append(pair.second));
    }

    std::shared_ptr<arrow::Int64Array> vertex_array;
    std::shared_ptr<arrow::DoubleArray> rank_array;
    ARROW_RETURN_NOT_OK(vertex_builder.Finish(&vertex_array));
    ARROW_RETURN_NOT_OK(rank_builder.Finish(&rank_array));

    // Create schema
    auto schema = arrow::schema({
        arrow::field("vertex_id", arrow::int64()),
        arrow::field("rank", arrow::float64())
    });

    // Create RecordBatch
    return arrow::RecordBatch::Make(schema, k, {vertex_array, rank_array});
}

}  // namespace traversal
}  // namespace graph
}  // namespace sabot
