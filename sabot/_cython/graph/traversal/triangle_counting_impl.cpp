#include "triangle_counting.h"
#include <arrow/builder.h>
#include <arrow/status.h>
#include <vector>
#include <algorithm>
#include <unordered_set>
#include <cmath>

namespace sabot {
namespace graph {
namespace traversal {

/**
 * Count triangles using node-iterator method.
 */
arrow::Result<TriangleCountResult> CountTriangles(
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

    // Per-vertex triangle counts
    std::vector<int64_t> per_vertex_triangles(num_vertices, 0);
    int64_t total_triangles = 0;

    // For each vertex v
    for (int64_t v = 0; v < num_vertices; v++) {
        int64_t start_v = indptr_data[v];
        int64_t end_v = indptr_data[v + 1];

        // Get neighbors of v
        std::vector<int64_t> neighbors_v;
        for (int64_t i = start_v; i < end_v; i++) {
            int64_t neighbor = indices_data[i];
            if (neighbor >= 0 && neighbor < num_vertices && neighbor != v) {
                neighbors_v.push_back(neighbor);
            }
        }

        // For each pair of neighbors (u, w) of v
        for (size_t i = 0; i < neighbors_v.size(); i++) {
            int64_t u = neighbors_v[i];

            // Get neighbors of u for quick lookup
            std::unordered_set<int64_t> neighbors_u;
            int64_t start_u = indptr_data[u];
            int64_t end_u = indptr_data[u + 1];
            for (int64_t j = start_u; j < end_u; j++) {
                int64_t neighbor = indices_data[j];
                if (neighbor >= 0 && neighbor < num_vertices) {
                    neighbors_u.insert(neighbor);
                }
            }

            for (size_t j = i + 1; j < neighbors_v.size(); j++) {
                int64_t w = neighbors_v[j];

                // Check if u and w are connected (triangle: v-u-w-v)
                if (neighbors_u.count(w) > 0) {
                    per_vertex_triangles[v]++;

                    // Only count each triangle once (when v is the smallest ID)
                    if (v < u && v < w) {
                        total_triangles++;
                    }
                }
            }
        }
    }

    // Compute local clustering coefficients
    std::vector<double> clustering_coefficient(num_vertices, 0.0);

    for (int64_t v = 0; v < num_vertices; v++) {
        int64_t start = indptr_data[v];
        int64_t end = indptr_data[v + 1];
        int64_t degree = end - start;

        // Clustering coefficient = (2 * triangles) / (degree * (degree - 1))
        if (degree >= 2) {
            double num_possible_triangles = degree * (degree - 1) / 2.0;
            clustering_coefficient[v] = per_vertex_triangles[v] / num_possible_triangles;
        } else {
            clustering_coefficient[v] = 0.0;
        }
    }

    // Compute global clustering coefficient (average of local coefficients)
    double global_clustering = 0.0;
    int64_t num_valid_vertices = 0;
    for (int64_t v = 0; v < num_vertices; v++) {
        int64_t degree = indptr_data[v + 1] - indptr_data[v];
        if (degree >= 2) {
            global_clustering += clustering_coefficient[v];
            num_valid_vertices++;
        }
    }
    if (num_valid_vertices > 0) {
        global_clustering /= num_valid_vertices;
    }

    // Build Arrow arrays
    arrow::Int64Builder triangle_builder;
    arrow::DoubleBuilder clustering_builder;

    ARROW_RETURN_NOT_OK(triangle_builder.AppendValues(per_vertex_triangles));
    ARROW_RETURN_NOT_OK(clustering_builder.AppendValues(clustering_coefficient));

    std::shared_ptr<arrow::Int64Array> triangle_array;
    std::shared_ptr<arrow::DoubleArray> clustering_array;
    ARROW_RETURN_NOT_OK(triangle_builder.Finish(&triangle_array));
    ARROW_RETURN_NOT_OK(clustering_builder.Finish(&clustering_array));

    TriangleCountResult result;
    result.per_vertex_triangles = triangle_array;
    result.clustering_coefficient = clustering_array;
    result.total_triangles = total_triangles;
    result.global_clustering_coefficient = global_clustering;
    result.num_vertices = num_vertices;

    return result;
}

/**
 * Weighted triangle counting (weights ignored, topology only).
 */
arrow::Result<TriangleCountResult> CountTrianglesWeighted(
    const std::shared_ptr<arrow::Int64Array>& indptr,
    const std::shared_ptr<arrow::Int64Array>& indices,
    const std::shared_ptr<arrow::DoubleArray>& weights,
    int64_t num_vertices
) {
    // Validate weights
    if (!weights) {
        return arrow::Status::Invalid("weights cannot be null");
    }
    if (weights->length() != indices->length()) {
        return arrow::Status::Invalid("weights length must match indices length");
    }

    // Triangle counting is purely topological, so just call unweighted version
    return CountTriangles(indptr, indices, num_vertices);
}

/**
 * Get top-K vertices by triangle count.
 */
arrow::Result<std::shared_ptr<arrow::RecordBatch>> TopKTriangleCounts(
    const std::shared_ptr<arrow::Int64Array>& per_vertex_triangles,
    const std::shared_ptr<arrow::DoubleArray>& clustering_coefficient,
    int64_t k
) {
    if (!per_vertex_triangles || !clustering_coefficient) {
        return arrow::Status::Invalid("Input arrays cannot be null");
    }
    if (per_vertex_triangles->length() != clustering_coefficient->length()) {
        return arrow::Status::Invalid("Input arrays must have same length");
    }

    int64_t num_vertices = per_vertex_triangles->length();
    if (k <= 0 || k > num_vertices) {
        k = num_vertices;
    }

    const int64_t* triangle_data = per_vertex_triangles->raw_values();
    const double* clustering_data = clustering_coefficient->raw_values();

    // Create vector of (vertex_id, triangle_count, clustering_coefficient)
    struct VertexScore {
        int64_t vertex_id;
        int64_t triangle_count;
        double clustering_coefficient;
    };

    std::vector<VertexScore> vertex_scores;
    vertex_scores.reserve(num_vertices);

    for (int64_t v = 0; v < num_vertices; v++) {
        vertex_scores.push_back({v, triangle_data[v], clustering_data[v]});
    }

    // Partial sort to get top-K by triangle count
    if (k < num_vertices) {
        std::partial_sort(
            vertex_scores.begin(),
            vertex_scores.begin() + k,
            vertex_scores.end(),
            [](const VertexScore& a, const VertexScore& b) {
                if (a.triangle_count != b.triangle_count) {
                    return a.triangle_count > b.triangle_count;
                }
                return a.clustering_coefficient > b.clustering_coefficient;
            }
        );
        vertex_scores.resize(k);
    } else {
        std::sort(
            vertex_scores.begin(),
            vertex_scores.end(),
            [](const VertexScore& a, const VertexScore& b) {
                if (a.triangle_count != b.triangle_count) {
                    return a.triangle_count > b.triangle_count;
                }
                return a.clustering_coefficient > b.clustering_coefficient;
            }
        );
    }

    // Build result arrays
    arrow::Int64Builder vertex_builder, triangle_builder;
    arrow::DoubleBuilder clustering_builder;

    ARROW_RETURN_NOT_OK(vertex_builder.Reserve(k));
    ARROW_RETURN_NOT_OK(triangle_builder.Reserve(k));
    ARROW_RETURN_NOT_OK(clustering_builder.Reserve(k));

    for (const auto& score : vertex_scores) {
        ARROW_RETURN_NOT_OK(vertex_builder.Append(score.vertex_id));
        ARROW_RETURN_NOT_OK(triangle_builder.Append(score.triangle_count));
        ARROW_RETURN_NOT_OK(clustering_builder.Append(score.clustering_coefficient));
    }

    std::shared_ptr<arrow::Int64Array> vertex_array, triangle_array;
    std::shared_ptr<arrow::DoubleArray> clustering_array;
    ARROW_RETURN_NOT_OK(vertex_builder.Finish(&vertex_array));
    ARROW_RETURN_NOT_OK(triangle_builder.Finish(&triangle_array));
    ARROW_RETURN_NOT_OK(clustering_builder.Finish(&clustering_array));

    auto schema = arrow::schema({
        arrow::field("vertex_id", arrow::int64()),
        arrow::field("triangle_count", arrow::int64()),
        arrow::field("clustering_coefficient", arrow::float64())
    });

    return arrow::RecordBatch::Make(schema, k, {vertex_array, triangle_array, clustering_array});
}

/**
 * Compute transitivity (global clustering coefficient).
 */
arrow::Result<double> ComputeTransitivity(
    const std::shared_ptr<arrow::Int64Array>& indptr,
    const std::shared_ptr<arrow::Int64Array>& indices,
    int64_t num_vertices
) {
    // Count triangles
    auto result = CountTriangles(indptr, indices, num_vertices);
    if (!result.ok()) {
        return result.status();
    }

    TriangleCountResult triangle_result = result.ValueOrDie();
    int64_t num_triangles = triangle_result.total_triangles;

    // Count connected triples (paths of length 2)
    const int64_t* indptr_data = indptr->raw_values();
    int64_t num_triples = 0;

    for (int64_t v = 0; v < num_vertices; v++) {
        int64_t degree = indptr_data[v + 1] - indptr_data[v];
        if (degree >= 2) {
            // Number of ways to choose 2 neighbors from degree neighbors
            num_triples += degree * (degree - 1) / 2;
        }
    }

    // Transitivity = 3 * triangles / triples
    // (Each triangle contributes to 3 triples, one for each vertex)
    if (num_triples == 0) {
        return 0.0;
    }

    double transitivity = (3.0 * num_triangles) / num_triples;
    return transitivity;
}

}  // namespace traversal
}  // namespace graph
}  // namespace sabot
