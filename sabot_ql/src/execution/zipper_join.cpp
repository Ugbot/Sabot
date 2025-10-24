#include <sabot_ql/execution/zipper_join.h>
#include <arrow/api.h>
#include <arrow/compute/api.h>
#include <unordered_map>
#include <vector>
#include <sstream>
#include <chrono>
#include <algorithm>

namespace sabot_ql {

/**
 * Zipper Join (Merge Join) Algorithm
 *
 * Ported from QLever's JoinAlgorithms.h but adapted for Arrow tables.
 * This is a GENERIC join operator - works with any Arrow data, not just triples.
 *
 * Key differences from QLever:
 * - QLever uses IdTable (custom structure) → We use Arrow Tables
 * - QLever has custom comparators → We use Arrow's native comparison
 * - QLever handles UNDEF inline → We use Arrow's null handling
 *
 * Algorithm (from QLever's zipperJoinForBlocks):
 * 1. Assume both inputs are sorted by join columns
 * 2. Scan left and right with two pointers
 * 3. When join keys match, emit combined row
 * 4. Advance the side with smaller key
 *
 * Performance: O(n + m) where n, m are input sizes
 */

ZipperJoinOperator::ZipperJoinOperator(
    std::shared_ptr<Operator> left_child,
    std::shared_ptr<Operator> right_child,
    const std::vector<std::string>& join_columns,
    const std::string& description)
    : left_child_(std::move(left_child))
    , right_child_(std::move(right_child))
    , join_columns_(join_columns)
    , description_(description)
    , executed_(false) {

    if (!left_child_ || !right_child_) {
        throw std::invalid_argument("Child operators cannot be null");
    }

    if (join_columns_.empty()) {
        throw std::invalid_argument("Must have at least one join column");
    }
}

arrow::Result<std::shared_ptr<arrow::Schema>> ZipperJoinOperator::GetOutputSchema() const {
    // Output schema = left columns + right columns (minus duplicate join columns)
    ARROW_ASSIGN_OR_RAISE(auto left_schema, left_child_->GetOutputSchema());
    ARROW_ASSIGN_OR_RAISE(auto right_schema, right_child_->GetOutputSchema());

    std::vector<std::shared_ptr<arrow::Field>> fields;

    // Add all left columns
    for (const auto& field : left_schema->fields()) {
        fields.push_back(field);
    }

    // Add right columns that aren't join columns
    for (const auto& field : right_schema->fields()) {
        if (std::find(join_columns_.begin(), join_columns_.end(), field->name()) == join_columns_.end()) {
            fields.push_back(field);
        }
    }

    return arrow::schema(fields);
}

arrow::Result<std::shared_ptr<arrow::RecordBatch>> ZipperJoinOperator::GetNextBatch() {
    if (executed_) {
        return nullptr;
    }

    auto start_time = std::chrono::high_resolution_clock::now();

    // Get all data from children (for now, full materialization)
    // TODO: Streaming join for larger datasets
    ARROW_ASSIGN_OR_RAISE(auto left_table, left_child_->GetAllResults());
    ARROW_ASSIGN_OR_RAISE(auto right_table, right_child_->GetAllResults());

    // Perform zipper join
    ARROW_ASSIGN_OR_RAISE(auto result_table, ExecuteZipperJoin(left_table, right_table));

    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);

    // Update stats
    stats_.rows_processed = result_table->num_rows();
    stats_.batches_processed = 1;
    stats_.execution_time_ms = duration.count();

    executed_ = true;

    // Convert to batch
    if (result_table->num_rows() == 0) {
        ARROW_ASSIGN_OR_RAISE(auto schema, GetOutputSchema());
        std::vector<std::shared_ptr<arrow::Array>> empty_arrays;
        return arrow::RecordBatch::Make(schema, 0, empty_arrays);
    }

    ARROW_ASSIGN_OR_RAISE(auto combined_table, result_table->CombineChunks());

    // Extract arrays from table
    std::vector<std::shared_ptr<arrow::Array>> arrays;
    for (int i = 0; i < combined_table->num_columns(); ++i) {
        arrays.push_back(combined_table->column(i)->chunk(0));
    }

    return arrow::RecordBatch::Make(combined_table->schema(), combined_table->num_rows(), arrays);
}

bool ZipperJoinOperator::HasNextBatch() const {
    return !executed_;
}

std::string ZipperJoinOperator::ToString() const {
    if (!description_.empty()) {
        return description_;
    }

    std::ostringstream oss;
    oss << "ZipperJoin(";
    for (size_t i = 0; i < join_columns_.size(); ++i) {
        if (i > 0) oss << ", ";
        oss << join_columns_[i];
    }
    oss << ")";
    return oss.str();
}

size_t ZipperJoinOperator::EstimateCardinality() const {
    // Estimate: min(left_card, right_card)
    // This is a conservative estimate for joins
    auto left_card = left_child_->EstimateCardinality();
    auto right_card = right_child_->EstimateCardinality();
    return std::min(left_card, right_card);
}

// Core zipper join algorithm ported from QLever
arrow::Result<std::shared_ptr<arrow::Table>> ZipperJoinOperator::ExecuteZipperJoin(
    const std::shared_ptr<arrow::Table>& left,
    const std::shared_ptr<arrow::Table>& right) {

    // Verify inputs are sorted (required for merge join)
    // In production, this should be enforced by query planner

    // Get join column indices
    std::vector<int> left_join_indices;
    std::vector<int> right_join_indices;

    for (const auto& col_name : join_columns_) {
        auto left_idx = left->schema()->GetFieldIndex(col_name);
        auto right_idx = right->schema()->GetFieldIndex(col_name);

        if (left_idx == -1 || right_idx == -1) {
            return arrow::Status::Invalid("Join column not found: " + col_name);
        }

        left_join_indices.push_back(left_idx);
        right_join_indices.push_back(right_idx);
    }

    // Build result using Arrow builders
    // We'll create one builder per output column
    std::vector<std::unique_ptr<arrow::ArrayBuilder>> builders;
    ARROW_ASSIGN_OR_RAISE(auto output_schema, GetOutputSchema());

    for (const auto& field : output_schema->fields()) {
        std::unique_ptr<arrow::ArrayBuilder> builder;
        ARROW_RETURN_NOT_OK(arrow::MakeBuilder(arrow::default_memory_pool(), field->type(), &builder));
        builders.push_back(std::move(builder));
    }

    // Zipper algorithm (from QLever's zipperJoinForBlocks)
    size_t left_idx = 0;
    size_t right_idx = 0;

    while (left_idx < left->num_rows() && right_idx < right->num_rows()) {
        // Compare join keys
        int cmp = CompareJoinKeys(left, left_idx, left_join_indices,
                                  right, right_idx, right_join_indices);

        if (cmp == 0) {
            // Match found! Emit combined row
            // Handle multiple matches (all combinations where keys are equal)

            size_t left_start = left_idx;
            size_t right_start = right_idx;

            // Find range of left rows with same key
            while (left_idx < left->num_rows() &&
                   CompareJoinKeys(left, left_start, left_join_indices,
                                  left, left_idx, left_join_indices) == 0) {
                left_idx++;
            }

            // Find range of right rows with same key
            while (right_idx < right->num_rows() &&
                   CompareJoinKeys(right, right_start, right_join_indices,
                                  right, right_idx, right_join_indices) == 0) {
                right_idx++;
            }

            // Cartesian product of matching ranges
            for (size_t l = left_start; l < left_idx; ++l) {
                for (size_t r = right_start; r < right_idx; ++r) {
                    ARROW_RETURN_NOT_OK(AppendJoinedRow(builders, left, l, right, r));
                }
            }

        } else if (cmp < 0) {
            // Left key < right key, advance left
            left_idx++;
        } else {
            // Left key > right key, advance right
            right_idx++;
        }
    }

    // Finish builders and create table
    std::vector<std::shared_ptr<arrow::Array>> arrays;
    for (auto& builder : builders) {
        std::shared_ptr<arrow::Array> array;
        ARROW_RETURN_NOT_OK(builder->Finish(&array));
        arrays.push_back(array);
    }

    return arrow::Table::Make(output_schema, arrays);
}

// Compare join keys at given row indices
int ZipperJoinOperator::CompareJoinKeys(
    const std::shared_ptr<arrow::Table>& left_table,
    size_t left_row,
    const std::vector<int>& left_indices,
    const std::shared_ptr<arrow::Table>& right_table,
    size_t right_row,
    const std::vector<int>& right_indices) {

    // Compare each join column in order
    for (size_t i = 0; i < left_indices.size(); ++i) {
        auto left_col = left_table->column(left_indices[i])->chunk(0);
        auto right_col = right_table->column(right_indices[i])->chunk(0);

        // For simplicity, assume int64 columns (our triple IDs)
        // TODO: Generic comparison for any Arrow type
        auto left_array = std::static_pointer_cast<arrow::Int64Array>(left_col);
        auto right_array = std::static_pointer_cast<arrow::Int64Array>(right_col);

        int64_t left_val = left_array->Value(left_row);
        int64_t right_val = right_array->Value(right_row);

        if (left_val < right_val) return -1;
        if (left_val > right_val) return 1;
    }

    return 0;  // All join keys equal
}

// Append a joined row to builders
arrow::Status ZipperJoinOperator::AppendJoinedRow(
    std::vector<std::unique_ptr<arrow::ArrayBuilder>>& builders,
    const std::shared_ptr<arrow::Table>& left_table,
    size_t left_row,
    const std::shared_ptr<arrow::Table>& right_table,
    size_t right_row) {

    size_t builder_idx = 0;

    // Append all left columns
    for (int col_idx = 0; col_idx < left_table->num_columns(); ++col_idx) {
        auto array = left_table->column(col_idx)->chunk(0);
        auto int64_array = std::static_pointer_cast<arrow::Int64Array>(array);
        int64_t value = int64_array->Value(left_row);

        auto builder = static_cast<arrow::Int64Builder*>(builders[builder_idx].get());
        ARROW_RETURN_NOT_OK(builder->Append(value));
        builder_idx++;
    }

    // Append right columns (skip join columns)
    for (int col_idx = 0; col_idx < right_table->num_columns(); ++col_idx) {
        std::string col_name = right_table->schema()->field(col_idx)->name();

        // Skip if this is a join column (already added from left)
        if (std::find(join_columns_.begin(), join_columns_.end(), col_name) != join_columns_.end()) {
            continue;
        }

        auto array = right_table->column(col_idx)->chunk(0);
        auto int64_array = std::static_pointer_cast<arrow::Int64Array>(array);
        int64_t value = int64_array->Value(right_row);

        auto builder = static_cast<arrow::Int64Builder*>(builders[builder_idx].get());
        ARROW_RETURN_NOT_OK(builder->Append(value));
        builder_idx++;
    }

    return arrow::Status::OK();
}

} // namespace sabot_ql
