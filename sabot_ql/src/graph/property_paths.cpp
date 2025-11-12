/**
 * Property Path Operators Implementation
 *
 * Arrow-native implementations of SPARQL 1.1 property path operators.
 */

#include "sabot_ql/graph/property_paths.h"
#include <arrow/compute/api.h>
#include <arrow/builder.h>
#include <unordered_set>
#include <unordered_map>

namespace sabot {
namespace graph {

// Hash function for (int64_t, int64_t) pairs for deduplication
struct PairHash {
    std::size_t operator()(const std::pair<int64_t, int64_t>& p) const {
        return std::hash<int64_t>()(p.first) ^ (std::hash<int64_t>()(p.second) << 1);
    }
};

arrow::Result<std::shared_ptr<arrow::RecordBatch>> SequencePath(
    const std::shared_ptr<arrow::RecordBatch>& path_p,
    const std::shared_ptr<arrow::RecordBatch>& path_q,
    arrow::MemoryPool* pool) {

    // Extract columns from path_p
    auto p_start_array = std::static_pointer_cast<arrow::Int64Array>(
        path_p->GetColumnByName("start"));
    auto p_end_array = std::static_pointer_cast<arrow::Int64Array>(
        path_p->GetColumnByName("end"));

    // Extract columns from path_q
    auto q_start_array = std::static_pointer_cast<arrow::Int64Array>(
        path_q->GetColumnByName("start"));
    auto q_end_array = std::static_pointer_cast<arrow::Int64Array>(
        path_q->GetColumnByName("end"));

    // Build hash map: intermediate_node -> list of start nodes from path_p
    std::unordered_map<int64_t, std::vector<int64_t>> intermediate_to_starts;
    for (int64_t i = 0; i < path_p->num_rows(); ++i) {
        int64_t start = p_start_array->Value(i);
        int64_t intermediate = p_end_array->Value(i);
        intermediate_to_starts[intermediate].push_back(start);
    }

    // Build result by joining on intermediate node
    arrow::Int64Builder start_builder(pool);
    arrow::Int64Builder end_builder(pool);

    for (int64_t i = 0; i < path_q->num_rows(); ++i) {
        int64_t intermediate = q_start_array->Value(i);
        int64_t end = q_end_array->Value(i);

        // Find all starts that reach this intermediate node
        auto it = intermediate_to_starts.find(intermediate);
        if (it != intermediate_to_starts.end()) {
            for (int64_t start : it->second) {
                ARROW_RETURN_NOT_OK(start_builder.Append(start));
                ARROW_RETURN_NOT_OK(end_builder.Append(end));
            }
        }
    }

    // Build result RecordBatch
    std::shared_ptr<arrow::Int64Array> start_array;
    std::shared_ptr<arrow::Int64Array> end_array;

    ARROW_RETURN_NOT_OK(start_builder.Finish(&start_array));
    ARROW_RETURN_NOT_OK(end_builder.Finish(&end_array));

    auto schema = arrow::schema({
        arrow::field("start", arrow::int64()),
        arrow::field("end", arrow::int64())
    });

    return arrow::RecordBatch::Make(
        schema,
        start_array->length(),
        {start_array, end_array}
    );
}

arrow::Result<std::shared_ptr<arrow::RecordBatch>> AlternativePath(
    const std::shared_ptr<arrow::RecordBatch>& path_p,
    const std::shared_ptr<arrow::RecordBatch>& path_q,
    arrow::MemoryPool* pool) {

    // Use hash set for deduplication
    std::unordered_set<std::pair<int64_t, int64_t>, PairHash> unique_pairs;

    // Add all pairs from path_p
    auto p_start_array = std::static_pointer_cast<arrow::Int64Array>(
        path_p->GetColumnByName("start"));
    auto p_end_array = std::static_pointer_cast<arrow::Int64Array>(
        path_p->GetColumnByName("end"));

    for (int64_t i = 0; i < path_p->num_rows(); ++i) {
        int64_t start = p_start_array->Value(i);
        int64_t end = p_end_array->Value(i);
        unique_pairs.insert({start, end});
    }

    // Add all pairs from path_q
    auto q_start_array = std::static_pointer_cast<arrow::Int64Array>(
        path_q->GetColumnByName("start"));
    auto q_end_array = std::static_pointer_cast<arrow::Int64Array>(
        path_q->GetColumnByName("end"));

    for (int64_t i = 0; i < path_q->num_rows(); ++i) {
        int64_t start = q_start_array->Value(i);
        int64_t end = q_end_array->Value(i);
        unique_pairs.insert({start, end});
    }

    // Build result RecordBatch from unique pairs
    arrow::Int64Builder start_builder(pool);
    arrow::Int64Builder end_builder(pool);

    for (const auto& pair : unique_pairs) {
        ARROW_RETURN_NOT_OK(start_builder.Append(pair.first));
        ARROW_RETURN_NOT_OK(end_builder.Append(pair.second));
    }

    std::shared_ptr<arrow::Int64Array> start_array;
    std::shared_ptr<arrow::Int64Array> end_array;

    ARROW_RETURN_NOT_OK(start_builder.Finish(&start_array));
    ARROW_RETURN_NOT_OK(end_builder.Finish(&end_array));

    auto schema = arrow::schema({
        arrow::field("start", arrow::int64()),
        arrow::field("end", arrow::int64())
    });

    return arrow::RecordBatch::Make(
        schema,
        start_array->length(),
        {start_array, end_array}
    );
}

arrow::Result<std::shared_ptr<arrow::RecordBatch>> FilterByPredicate(
    const std::shared_ptr<arrow::RecordBatch>& edges,
    const std::vector<int64_t>& excluded_predicates,
    const std::string& predicate_col,
    arrow::MemoryPool* pool) {

    // Get predicate column
    auto predicate_array = std::static_pointer_cast<arrow::Int64Array>(
        edges->GetColumnByName(predicate_col));

    if (!predicate_array) {
        return arrow::Status::Invalid("Predicate column not found: ", predicate_col);
    }

    // Build exclusion set for O(1) lookup
    std::unordered_set<int64_t> excluded_set(
        excluded_predicates.begin(),
        excluded_predicates.end()
    );

    // Build filter mask
    arrow::BooleanBuilder mask_builder(pool);

    for (int64_t i = 0; i < edges->num_rows(); ++i) {
        int64_t predicate = predicate_array->Value(i);
        bool include = (excluded_set.find(predicate) == excluded_set.end());
        ARROW_RETURN_NOT_OK(mask_builder.Append(include));
    }

    std::shared_ptr<arrow::BooleanArray> mask;
    ARROW_RETURN_NOT_OK(mask_builder.Finish(&mask));

    // Apply filter using Arrow compute
    ARROW_ASSIGN_OR_RAISE(
        auto filtered_datum,
        arrow::compute::Filter(
            edges,
            mask,
            arrow::compute::FilterOptions::Defaults()
        )
    );

    return filtered_datum.record_batch();
}

}  // namespace graph
}  // namespace sabot
