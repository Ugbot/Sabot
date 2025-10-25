#include <sabot_ql/operators/radix_sort.h>
#include <arrow/api.h>
#include <arrow/compute/api.h>
#include <algorithm>
#include <cstring>
#include <sstream>

namespace sabot_ql {

RadixSortOperator::RadixSortOperator(
    std::shared_ptr<Operator> input,
    const std::vector<SortKey>& sort_keys,
    const std::string& description)
    : input_(std::move(input))
    , sort_keys_(sort_keys)
    , description_(description)
    , executed_(false) {}

arrow::Result<std::shared_ptr<arrow::Schema>> RadixSortOperator::GetOutputSchema() const {
    return input_->GetOutputSchema();
}

arrow::Result<std::shared_ptr<arrow::RecordBatch>> RadixSortOperator::GetNextBatch() {
    if (executed_) {
        return nullptr;
    }

    // Get all input data
    ARROW_ASSIGN_OR_RAISE(auto input_table, input_->GetAllResults());

    // Execute radix sort
    ARROW_ASSIGN_OR_RAISE(auto sorted_table, ExecuteRadixSort(input_table));

    executed_ = true;

    // Convert to single batch
    ARROW_ASSIGN_OR_RAISE(auto combined, sorted_table->CombineChunksToBatch());
    return combined;
}

bool RadixSortOperator::HasNextBatch() const {
    return !executed_;
}

std::string RadixSortOperator::ToString() const {
    std::ostringstream oss;
    oss << "RadixSort(";
    for (size_t i = 0; i < sort_keys_.size(); ++i) {
        if (i > 0) oss << ", ";
        oss << sort_keys_[i].column_name;
        oss << (sort_keys_[i].direction == SortDirection::Ascending ? " ASC" : " DESC");
    }
    oss << ")";
    if (!description_.empty()) {
        oss << " [" << description_ << "]";
    }
    return oss.str();
}

size_t RadixSortOperator::EstimateCardinality() const {
    return input_->EstimateCardinality();
}

OrderingProperty RadixSortOperator::GetOutputOrdering() const {
    OrderingProperty ordering;
    for (const auto& key : sort_keys_) {
        ordering.columns.push_back(key.column_name);
        ordering.directions.push_back(key.direction);
    }
    return ordering;
}

arrow::Result<std::shared_ptr<arrow::Table>> RadixSortOperator::ExecuteRadixSort(
    const std::shared_ptr<arrow::Table>& input) {

    if (input->num_rows() == 0) {
        return input;
    }

    // Check if we can use radix sort (all int64 columns)
    ARROW_ASSIGN_OR_RAISE(bool can_radix, CanUseRadixSort(input->schema()));

    if (!can_radix) {
        // Fall back to Arrow's comparison sort
        std::vector<arrow::compute::SortKey> arrow_keys;
        for (const auto& key : sort_keys_) {
            arrow_keys.push_back(arrow::compute::SortKey(
                key.column_name,
                key.direction == SortDirection::Ascending
                    ? arrow::compute::SortOrder::Ascending
                    : arrow::compute::SortOrder::Descending
            ));
        }

        ARROW_ASSIGN_OR_RAISE(
            auto indices,
            arrow::compute::SortIndices(input, arrow::compute::SortOptions(arrow_keys))
        );

        ARROW_ASSIGN_OR_RAISE(
            auto sorted_table,
            arrow::compute::Take(input, indices)
        );

        return std::static_pointer_cast<arrow::Table>(sorted_table.table());
    }

    // Radix sort implementation
    // Start with identity permutation
    arrow::UInt64Builder indices_builder;
    ARROW_RETURN_NOT_OK(indices_builder.Reserve(input->num_rows()));
    for (int64_t i = 0; i < input->num_rows(); ++i) {
        ARROW_RETURN_NOT_OK(indices_builder.Append(i));
    }
    std::shared_ptr<arrow::UInt64Array> indices;
    ARROW_RETURN_NOT_OK(indices_builder.Finish(&indices));

    // Sort by each column (stable sort, right to left for multi-column)
    for (int col_idx = sort_keys_.size() - 1; col_idx >= 0; --col_idx) {
        const auto& sort_key = sort_keys_[col_idx];

        // Get column
        auto chunked_col = input->GetColumnByName(sort_key.column_name);
        if (!chunked_col) {
            return arrow::Status::Invalid("Column not found: " + sort_key.column_name);
        }

        // Combine chunks if needed
        ARROW_ASSIGN_OR_RAISE(auto combined_datum, arrow::Concatenate(chunked_col->chunks()));
        auto int64_array = std::static_pointer_cast<arrow::Int64Array>(combined_datum);

        // Radix sort this column
        ARROW_ASSIGN_OR_RAISE(indices, RadixSortInt64Column(int64_array, indices));

        // Handle descending order by reversing
        if (sort_key.direction == SortDirection::Descending) {
            arrow::UInt64Builder reversed_builder;
            ARROW_RETURN_NOT_OK(reversed_builder.Reserve(indices->length()));
            for (int64_t i = indices->length() - 1; i >= 0; --i) {
                ARROW_RETURN_NOT_OK(reversed_builder.Append(indices->Value(i)));
            }
            ARROW_RETURN_NOT_OK(reversed_builder.Finish(&indices));
        }
    }

    // Apply final permutation
    return ApplyPermutation(input, indices);
}

arrow::Result<std::shared_ptr<arrow::UInt64Array>> RadixSortOperator::RadixSortInt64Column(
    const std::shared_ptr<arrow::Int64Array>& array,
    const std::shared_ptr<arrow::UInt64Array>& indices) {

    std::shared_ptr<arrow::UInt64Array> current_indices = indices;

    // Process each byte from LSB to MSB (8 bytes for int64)
    for (int byte_pos = 0; byte_pos < 8; ++byte_pos) {
        ARROW_ASSIGN_OR_RAISE(
            current_indices,
            CountingSortByByte(array, current_indices, byte_pos)
        );
    }

    return current_indices;
}

arrow::Result<std::shared_ptr<arrow::UInt64Array>> RadixSortOperator::CountingSortByByte(
    const std::shared_ptr<arrow::Int64Array>& array,
    const std::shared_ptr<arrow::UInt64Array>& indices,
    int byte_pos) {

    const int64_t n = indices->length();
    const int NUM_BUCKETS = 256;

    // Count occurrences of each byte value
    std::vector<int64_t> count(NUM_BUCKETS, 0);

    for (int64_t i = 0; i < n; ++i) {
        uint64_t row = indices->Value(i);
        int64_t value = array->Value(row);

        // Handle negative numbers: flip sign bit for proper ordering
        uint64_t unsigned_value;
        if (byte_pos == 7) {
            // MSB: flip sign bit so negatives sort before positives
            unsigned_value = static_cast<uint64_t>(value) ^ 0x8000000000000000ULL;
        } else {
            unsigned_value = static_cast<uint64_t>(value);
        }

        uint8_t byte = (unsigned_value >> (byte_pos * 8)) & 0xFF;
        count[byte]++;
    }

    // Convert counts to positions (prefix sum)
    for (int i = 1; i < NUM_BUCKETS; ++i) {
        count[i] += count[i - 1];
    }

    // Build output in reverse to maintain stability
    arrow::UInt64Builder output_builder;
    ARROW_RETURN_NOT_OK(output_builder.Reserve(n));

    std::vector<uint64_t> output(n);
    for (int64_t i = n - 1; i >= 0; --i) {
        uint64_t row = indices->Value(i);
        int64_t value = array->Value(row);

        // Same unsigned conversion as above
        uint64_t unsigned_value;
        if (byte_pos == 7) {
            unsigned_value = static_cast<uint64_t>(value) ^ 0x8000000000000000ULL;
        } else {
            unsigned_value = static_cast<uint64_t>(value);
        }

        uint8_t byte = (unsigned_value >> (byte_pos * 8)) & 0xFF;
        output[--count[byte]] = row;
    }

    // Convert to Arrow array
    for (int64_t i = 0; i < n; ++i) {
        ARROW_RETURN_NOT_OK(output_builder.Append(output[i]));
    }

    std::shared_ptr<arrow::UInt64Array> result;
    ARROW_RETURN_NOT_OK(output_builder.Finish(&result));
    return result;
}

arrow::Result<std::shared_ptr<arrow::Table>> RadixSortOperator::ApplyPermutation(
    const std::shared_ptr<arrow::Table>& table,
    const std::shared_ptr<arrow::UInt64Array>& indices) {

    // Use Arrow's Take kernel to reorder all columns
    ARROW_ASSIGN_OR_RAISE(
        auto result,
        arrow::compute::Take(table, indices)
    );

    return std::static_pointer_cast<arrow::Table>(result.table());
}

arrow::Result<bool> RadixSortOperator::CanUseRadixSort(
    const std::shared_ptr<arrow::Schema>& schema) const {

    // Check if all sort key columns are int64
    for (const auto& key : sort_keys_) {
        auto field = schema->GetFieldByName(key.column_name);
        if (!field) {
            return arrow::Status::Invalid("Column not found: " + key.column_name);
        }

        if (field->type()->id() != arrow::Type::INT64) {
            return false;  // Not int64, can't use radix sort
        }
    }

    return true;
}

} // namespace sabot_ql
