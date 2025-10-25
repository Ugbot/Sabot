#pragma once

#include <sabot_ql/operators/operator.h>
#include <sabot_ql/operators/operator_metadata.h>
#include <sabot_ql/operators/sort.h>
#include <memory>
#include <vector>
#include <string>

namespace sabot_ql {

/**
 * @brief Radix Sort Operator - O(n) sorting for int64 columns
 *
 * Radix sort is optimal for fixed-size integer keys like ValueIds.
 * Time complexity: O(n*k) where k=8 bytes for int64.
 * Compare to quicksort/mergesort: O(n log n) comparisons.
 *
 * For 10K rows:  O(80K) vs O(130K) = 1.6x faster
 * For 100K rows: O(800K) vs O(1.7M) = 2.1x faster
 *
 * Algorithm: LSD (Least Significant Digit) radix sort
 * - Process bytes from LSB to MSB
 * - Use counting sort for each byte (256 buckets)
 * - Stable sort preserves relative order
 *
 * Use cases in SPARQL:
 * - Sorting scan results before merge join
 * - Sorting by ValueIds (subject/predicate/object columns)
 *
 * Falls back to comparison sort for:
 * - Non-integer column types
 * - Mixed-type sorts
 */
class RadixSortOperator : public Operator {
public:
    /**
     * @brief Construct radix sort operator
     * @param input Input operator
     * @param sort_keys Columns and directions to sort by
     * @param description Optional human-readable description
     */
    RadixSortOperator(
        std::shared_ptr<Operator> input,
        const std::vector<SortKey>& sort_keys,
        const std::string& description = ""
    );

    // Operator interface
    arrow::Result<std::shared_ptr<arrow::Schema>> GetOutputSchema() const override;
    arrow::Result<std::shared_ptr<arrow::RecordBatch>> GetNextBatch() override;
    bool HasNextBatch() const override;
    std::string ToString() const override;
    size_t EstimateCardinality() const override;

    /**
     * @brief Get output ordering (sorted by sort keys)
     */
    OrderingProperty GetOutputOrdering() const override;

    /**
     * @brief Get sort keys
     */
    const std::vector<SortKey>& GetSortKeys() const { return sort_keys_; }

private:
    /**
     * @brief Execute radix sort on int64 table
     *
     * @param input Input table with int64 columns
     * @return Sorted table
     */
    arrow::Result<std::shared_ptr<arrow::Table>> ExecuteRadixSort(
        const std::shared_ptr<arrow::Table>& input
    );

    /**
     * @brief Radix sort a single int64 array
     *
     * LSD radix sort: process bytes from least to most significant.
     * Uses counting sort for each byte position (0-7 for int64).
     *
     * @param array Input int64 array
     * @param indices Row indices to sort (for multi-column sort)
     * @return Sorted indices
     */
    arrow::Result<std::shared_ptr<arrow::UInt64Array>> RadixSortInt64Column(
        const std::shared_ptr<arrow::Int64Array>& array,
        const std::shared_ptr<arrow::UInt64Array>& indices
    );

    /**
     * @brief Count sort for one byte position
     *
     * @param array Input int64 array
     * @param indices Current row order
     * @param byte_pos Byte position (0=LSB, 7=MSB)
     * @return New row order after sorting by this byte
     */
    arrow::Result<std::shared_ptr<arrow::UInt64Array>> CountingSortByByte(
        const std::shared_ptr<arrow::Int64Array>& array,
        const std::shared_ptr<arrow::UInt64Array>& indices,
        int byte_pos
    );

    /**
     * @brief Apply row permutation to table
     *
     * Reorder all columns according to sorted indices.
     *
     * @param table Input table
     * @param indices Row order (result of radix sort)
     * @return Reordered table
     */
    arrow::Result<std::shared_ptr<arrow::Table>> ApplyPermutation(
        const std::shared_ptr<arrow::Table>& table,
        const std::shared_ptr<arrow::UInt64Array>& indices
    );

    /**
     * @brief Check if all sort keys are int64 type
     *
     * Radix sort only works for int64 columns.
     * If any column is non-int64, must fall back to comparison sort.
     */
    arrow::Result<bool> CanUseRadixSort(const std::shared_ptr<arrow::Schema>& schema) const;

    std::shared_ptr<Operator> input_;
    std::vector<SortKey> sort_keys_;
    std::string description_;
    bool executed_;
};

} // namespace sabot_ql
