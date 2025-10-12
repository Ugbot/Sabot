#pragma once

#include <sabot_ql/operators/operator.h>
#include <vector>
#include <string>

namespace sabot_ql {

// Sort direction for ORDER BY
enum class SortDirection {
    Ascending,
    Descending
};

// Sort key: column name and direction
struct SortKey {
    std::string column_name;
    SortDirection direction;

    SortKey(std::string col, SortDirection dir)
        : column_name(std::move(col)), direction(dir) {}

    std::string ToString() const {
        return column_name + (direction == SortDirection::Ascending ? " ASC" : " DESC");
    }
};

// Sort operator: sorts input by one or more columns
// Uses Arrow compute SortIndices kernel for vectorized sorting
//
// Example:
//   auto sort_op = std::make_shared<SortOperator>(
//       input,
//       std::vector<SortKey>{
//           {"age", SortDirection::Descending},
//           {"name", SortDirection::Ascending}
//       }
//   );
//
// This will sort by age DESC, then by name ASC (for ties)
class SortOperator : public UnaryOperator {
public:
    SortOperator(std::shared_ptr<Operator> input,
                 std::vector<SortKey> sort_keys);

    arrow::Result<std::shared_ptr<arrow::Schema>> GetOutputSchema() const override;
    arrow::Result<std::shared_ptr<arrow::RecordBatch>> GetNextBatch() override;
    std::string ToString() const override;
    size_t EstimateCardinality() const override;

private:
    // Sort all data at once
    // We need to collect all input batches, sort them, then return sorted batches
    arrow::Status SortAllData();

    // Check if sorting has been performed
    bool IsSorted() const { return sorted_table_ != nullptr; }

    std::vector<SortKey> sort_keys_;

    // Sorted data (materialized once)
    std::shared_ptr<arrow::Table> sorted_table_;

    // Current batch index for GetNextBatch()
    size_t current_batch_ = 0;
    size_t batch_size_ = 10000;  // 10K rows per batch
};

} // namespace sabot_ql
