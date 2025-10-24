#pragma once

#include <sabot_ql/operators/operator.h>
#include <memory>
#include <string>
#include <vector>

namespace sabot_ql {

/**
 * @brief Zipper Join (Merge Join) Operator
 *
 * GENERIC OPERATOR: Works with any Arrow tables, not just triples.
 * Can be used across all Sabot components that need sorted merge joins.
 *
 * Ported from QLever's zipperJoinForBlocks algorithm:
 * - vendor/qlever/src/util/JoinAlgorithms/JoinAlgorithms.h
 *
 * Key adaptation: QLever's IdTable → Arrow Table
 *
 * Algorithm:
 * 1. Requires both inputs sorted by join columns
 * 2. Two-pointer scan (like merging sorted arrays)
 * 3. When keys match, emit all combinations (handles duplicates)
 * 4. Time complexity: O(n + m)
 *
 * Example:
 *   // Join on "subject" column
 *   ZipperJoinOperator join(left_op, right_op, {"subject"});
 *   auto result = join.GetAllResults();
 *
 * Multi-column join:
 *   // Join on multiple columns
 *   ZipperJoinOperator join(left_op, right_op, {"col1", "col2"});
 *
 * Why Zipper Join for SPARQL?
 * - Triple stores have sorted indexes (SPO, POS, OSP)
 * - Scans return sorted results naturally
 * - No hash table needed (memory efficient)
 * - Optimal for sorted data (QLever's proven approach)
 */
class ZipperJoinOperator : public Operator {
public:
    /**
     * @brief Construct a zipper join operator
     * @param left_child Left input operator (must be sorted by join columns)
     * @param right_child Right input operator (must be sorted by join columns)
     * @param join_columns Columns to join on (must exist in both inputs)
     * @param description Optional human-readable description
     */
    ZipperJoinOperator(
        std::shared_ptr<Operator> left_child,
        std::shared_ptr<Operator> right_child,
        const std::vector<std::string>& join_columns,
        const std::string& description = ""
    );

    // Operator interface
    arrow::Result<std::shared_ptr<arrow::Schema>> GetOutputSchema() const override;
    arrow::Result<std::shared_ptr<arrow::RecordBatch>> GetNextBatch() override;
    bool HasNextBatch() const override;
    std::string ToString() const override;
    size_t EstimateCardinality() const override;

    /**
     * @brief Get join columns
     */
    const std::vector<std::string>& GetJoinColumns() const { return join_columns_; }

private:
    /**
     * @brief Execute the zipper join algorithm
     *
     * This is the core algorithm ported from QLever.
     * Generic - works with any Arrow tables.
     *
     * @param left Left input table (sorted by join columns)
     * @param right Right input table (sorted by join columns)
     * @return Joined table
     */
    arrow::Result<std::shared_ptr<arrow::Table>> ExecuteZipperJoin(
        const std::shared_ptr<arrow::Table>& left,
        const std::shared_ptr<arrow::Table>& right
    );

    /**
     * @brief Compare join keys at given row indices
     *
     * Returns:
     *   < 0 if left < right
     *   = 0 if left == right
     *   > 0 if left > right
     */
    int CompareJoinKeys(
        const std::shared_ptr<arrow::Table>& left_table,
        size_t left_row,
        const std::vector<int>& left_indices,
        const std::shared_ptr<arrow::Table>& right_table,
        size_t right_row,
        const std::vector<int>& right_indices
    );

    /**
     * @brief Append a joined row to output builders
     */
    arrow::Status AppendJoinedRow(
        std::vector<std::unique_ptr<arrow::ArrayBuilder>>& builders,
        const std::shared_ptr<arrow::Table>& left_table,
        size_t left_row,
        const std::shared_ptr<arrow::Table>& right_table,
        size_t right_row
    );

    std::shared_ptr<Operator> left_child_;
    std::shared_ptr<Operator> right_child_;
    std::vector<std::string> join_columns_;
    std::string description_;
    bool executed_;
};

} // namespace sabot_ql
