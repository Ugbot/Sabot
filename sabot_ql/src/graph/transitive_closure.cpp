/**
 * Transitive Closure Implementation
 *
 * DFS-based algorithm borrowed from QLever.
 */

#include "sabot_ql/graph/transitive_closure.h"
#include <arrow/builder.h>
#include <arrow/util/bitmap.h>
#include <vector>
#include <stack>
#include <algorithm>

namespace sabot {
namespace graph {

arrow::Result<TransitiveClosureResult> TransitiveClosureSingle(
    const CSRGraph& graph,
    int64_t start_node,
    int64_t min_dist,
    int64_t max_dist,
    arrow::MemoryPool* pool) {

    TransitiveClosureResult tc_result;

    // Boundary checks
    if (start_node < 0 || start_node >= graph.num_nodes) {
        // Return empty result
        auto schema = arrow::schema({
            arrow::field("start", arrow::int64()),
            arrow::field("end", arrow::int64()),
            arrow::field("dist", arrow::int64())
        });
        ARROW_ASSIGN_OR_RAISE(tc_result.result,
            arrow::RecordBatch::MakeEmpty(schema, pool));
        return tc_result;
    }

    // DFS stack: (node, distance)
    std::vector<std::pair<int64_t, int64_t>> stack;

    // Visited set (memory efficient bitset)
    std::vector<bool> visited(graph.num_nodes, false);

    // Result builders
    arrow::Int64Builder start_builder(pool);
    arrow::Int64Builder end_builder(pool);
    arrow::Int64Builder dist_builder(pool);

    // Initialize stack with start node at distance 0
    stack.emplace_back(start_node, 0);

    // DFS traversal (algorithm from QLever TransitivePathImpl.h:203-238)
    while (!stack.empty()) {
        auto [node, steps] = stack.back();
        stack.pop_back();

        // Skip if already visited
        if (visited[node]) {
            continue;
        }

        // Check distance bounds
        if (steps > max_dist) {
            continue;
        }

        // Mark as visited
        visited[node] = true;

        // Emit result if within min distance
        if (steps >= min_dist) {
            ARROW_RETURN_NOT_OK(start_builder.Append(start_node));
            ARROW_RETURN_NOT_OK(end_builder.Append(node));
            ARROW_RETURN_NOT_OK(dist_builder.Append(steps));
        }

        // Expand to successors
        ARROW_ASSIGN_OR_RAISE(auto successors, graph.GetSuccessors(node));

        for (int64_t i = 0; i < successors->length(); ++i) {
            int64_t successor = successors->Value(i);
            // Push with incremented distance
            stack.emplace_back(successor, steps + 1);
        }
    }

    // Build result RecordBatch
    std::shared_ptr<arrow::Int64Array> start_array;
    std::shared_ptr<arrow::Int64Array> end_array;
    std::shared_ptr<arrow::Int64Array> dist_array;

    ARROW_RETURN_NOT_OK(start_builder.Finish(&start_array));
    ARROW_RETURN_NOT_OK(end_builder.Finish(&end_array));
    ARROW_RETURN_NOT_OK(dist_builder.Finish(&dist_array));

    auto schema = arrow::schema({
        arrow::field("start", arrow::int64()),
        arrow::field("end", arrow::int64()),
        arrow::field("dist", arrow::int64())
    });

    tc_result.result = arrow::RecordBatch::Make(
        schema,
        start_array->length(),
        {start_array, end_array, dist_array}
    );

    return tc_result;
}

arrow::Result<TransitiveClosureResult> TransitiveClosure(
    const CSRGraph& graph,
    const std::shared_ptr<arrow::Int64Array>& start_nodes,
    int64_t min_dist,
    int64_t max_dist,
    arrow::MemoryPool* pool) {

    // Handle empty start nodes
    if (!start_nodes || start_nodes->length() == 0) {
        TransitiveClosureResult tc_result;
        auto schema = arrow::schema({
            arrow::field("start", arrow::int64()),
            arrow::field("end", arrow::int64()),
            arrow::field("dist", arrow::int64())
        });
        ARROW_ASSIGN_OR_RAISE(tc_result.result,
            arrow::RecordBatch::MakeEmpty(schema, pool));
        return tc_result;
    }

    // Accumulate results from all start nodes
    arrow::Int64Builder start_builder(pool);
    arrow::Int64Builder end_builder(pool);
    arrow::Int64Builder dist_builder(pool);

    // Process each start node independently
    for (int64_t i = 0; i < start_nodes->length(); ++i) {
        int64_t start_node = start_nodes->Value(i);

        // Run single-node transitive closure
        ARROW_ASSIGN_OR_RAISE(auto single_result,
            TransitiveClosureSingle(graph, start_node, min_dist, max_dist, pool));

        // Append results to builders
        if (single_result.result && single_result.result->num_rows() > 0) {
            auto start_col = std::static_pointer_cast<arrow::Int64Array>(
                single_result.result->column(0));
            auto end_col = std::static_pointer_cast<arrow::Int64Array>(
                single_result.result->column(1));
            auto dist_col = std::static_pointer_cast<arrow::Int64Array>(
                single_result.result->column(2));

            ARROW_RETURN_NOT_OK(start_builder.AppendValues(
                start_col->raw_values(), start_col->length()));
            ARROW_RETURN_NOT_OK(end_builder.AppendValues(
                end_col->raw_values(), end_col->length()));
            ARROW_RETURN_NOT_OK(dist_builder.AppendValues(
                dist_col->raw_values(), dist_col->length()));
        }
    }

    // Build final result
    std::shared_ptr<arrow::Int64Array> start_array;
    std::shared_ptr<arrow::Int64Array> end_array;
    std::shared_ptr<arrow::Int64Array> dist_array;

    ARROW_RETURN_NOT_OK(start_builder.Finish(&start_array));
    ARROW_RETURN_NOT_OK(end_builder.Finish(&end_array));
    ARROW_RETURN_NOT_OK(dist_builder.Finish(&dist_array));

    auto schema = arrow::schema({
        arrow::field("start", arrow::int64()),
        arrow::field("end", arrow::int64()),
        arrow::field("dist", arrow::int64())
    });

    TransitiveClosureResult tc_result;
    tc_result.result = arrow::RecordBatch::Make(
        schema,
        start_array->length(),
        {start_array, end_array, dist_array}
    );

    return tc_result;
}

}  // namespace graph
}  // namespace sabot
