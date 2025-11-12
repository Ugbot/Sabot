/**
 * CSR Graph Implementation
 *
 * Compressed Sparse Row format for efficient graph traversal.
 */

#include "sabot_ql/graph/csr_graph.h"
#include <arrow/builder.h>
#include <algorithm>
#include <vector>

namespace sabot {
namespace graph {

arrow::Result<std::shared_ptr<arrow::Int64Array>> CSRGraph::GetSuccessors(int64_t node) const {
    // Boundary check
    if (node < 0 || node >= num_nodes) {
        // Return empty array
        ARROW_ASSIGN_OR_RAISE(auto empty_array, arrow::MakeEmptyArray(arrow::int64()));
        return std::static_pointer_cast<arrow::Int64Array>(empty_array);
    }

    // Get edge range for this node
    int64_t start = offsets->Value(node);
    int64_t end = offsets->Value(node + 1);

    // Return zero-copy slice of targets array
    return std::static_pointer_cast<arrow::Int64Array>(
        targets->Slice(start, end - start));
}

arrow::Result<std::shared_ptr<arrow::Int64Array>> CSRGraph::GetEdgeIds(int64_t node) const {
    // Boundary check
    if (node < 0 || node >= num_nodes) {
        ARROW_ASSIGN_OR_RAISE(auto empty_array, arrow::MakeEmptyArray(arrow::int64()));
        return std::static_pointer_cast<arrow::Int64Array>(empty_array);
    }

    int64_t start = offsets->Value(node);
    int64_t end = offsets->Value(node + 1);

    return std::static_pointer_cast<arrow::Int64Array>(
        edge_ids->Slice(start, end - start));
}

arrow::Result<CSRGraph> BuildCSRGraph(
    const std::shared_ptr<arrow::RecordBatch>& edges,
    const std::string& source_col,
    const std::string& target_col,
    arrow::MemoryPool* pool) {

    CSRGraph csr;

    // Handle empty graph
    if (edges->num_rows() == 0) {
        arrow::Int64Builder offset_builder(pool);
        ARROW_RETURN_NOT_OK(offset_builder.Append(0));
        ARROW_RETURN_NOT_OK(offset_builder.Finish(&csr.offsets));

        ARROW_ASSIGN_OR_RAISE(auto targets_array, arrow::MakeEmptyArray(arrow::int64()));
        csr.targets = std::static_pointer_cast<arrow::Int64Array>(targets_array);
        ARROW_ASSIGN_OR_RAISE(auto edge_ids_array, arrow::MakeEmptyArray(arrow::int64()));
        csr.edge_ids = std::static_pointer_cast<arrow::Int64Array>(edge_ids_array);

        csr.num_nodes = 0;
        csr.num_edges = 0;
        return csr;
    }

    // Extract source and target columns
    auto sources = std::static_pointer_cast<arrow::Int64Array>(
        edges->GetColumnByName(source_col));
    auto targets_col = std::static_pointer_cast<arrow::Int64Array>(
        edges->GetColumnByName(target_col));

    if (!sources || !targets_col) {
        return arrow::Status::Invalid("Source or target column not found");
    }

    int64_t num_edges_val = sources->length();

    // Find max node ID to determine number of nodes
    int64_t max_node = 0;
    for (int64_t i = 0; i < num_edges_val; ++i) {
        if (sources->IsValid(i)) {
            max_node = std::max(max_node, sources->Value(i));
        }
        if (targets_col->IsValid(i)) {
            max_node = std::max(max_node, targets_col->Value(i));
        }
    }

    int64_t num_nodes_val = max_node + 1;

    // Count outgoing edges per node
    std::vector<int64_t> degree(num_nodes_val, 0);
    for (int64_t i = 0; i < num_edges_val; ++i) {
        if (sources->IsValid(i)) {
            degree[sources->Value(i)]++;
        }
    }

    // Build offsets array (cumulative sum)
    arrow::Int64Builder offset_builder(pool);
    int64_t cumsum = 0;
    for (int64_t d : degree) {
        ARROW_RETURN_NOT_OK(offset_builder.Append(cumsum));
        cumsum += d;
    }
    ARROW_RETURN_NOT_OK(offset_builder.Append(cumsum));  // Final boundary
    ARROW_RETURN_NOT_OK(offset_builder.Finish(&csr.offsets));

    // Allocate targets and edge_ids arrays
    std::vector<int64_t> target_vec(num_edges_val);
    std::vector<int64_t> edge_id_vec(num_edges_val);

    // Track current write position for each node
    std::vector<int64_t> current_offset(degree);  // Copy of degree

    // Fill targets and edge_ids by iterating edges
    for (int64_t i = 0; i < num_edges_val; ++i) {
        if (!sources->IsValid(i) || !targets_col->IsValid(i)) {
            continue;  // Skip null values
        }

        int64_t src = sources->Value(i);
        int64_t tgt = targets_col->Value(i);

        // Calculate write position
        int64_t pos = csr.offsets->Value(src) + (degree[src] - current_offset[src]);
        current_offset[src]--;

        target_vec[pos] = tgt;
        edge_id_vec[pos] = i;
    }

    // Build Arrow arrays from vectors
    arrow::Int64Builder target_builder(pool);
    arrow::Int64Builder edge_id_builder(pool);

    ARROW_RETURN_NOT_OK(target_builder.AppendValues(target_vec));
    ARROW_RETURN_NOT_OK(edge_id_builder.AppendValues(edge_id_vec));

    ARROW_RETURN_NOT_OK(target_builder.Finish(&csr.targets));
    ARROW_RETURN_NOT_OK(edge_id_builder.Finish(&csr.edge_ids));

    csr.num_nodes = num_nodes_val;
    csr.num_edges = num_edges_val;

    return csr;
}

arrow::Result<CSRGraph> BuildReverseCSRGraph(
    const std::shared_ptr<arrow::RecordBatch>& edges,
    const std::string& source_col,
    const std::string& target_col,
    arrow::MemoryPool* pool) {

    // Simply swap source and target columns
    return BuildCSRGraph(edges, target_col, source_col, pool);
}

}  // namespace graph
}  // namespace sabot
