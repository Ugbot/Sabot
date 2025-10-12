#pragma once

#include <sabot_ql/operators/operator.h>
#include <arrow/api.h>
#include <vector>
#include <memory>

namespace sabot_ql {

// UnionOperator - Combines results from multiple input operators
//
// Supports both:
// - UNION (with deduplication)
// - UNION ALL (without deduplication)
//
// Schema Unification:
// - All inputs must have compatible schemas
// - Column names must match
// - Column types must be compatible (will cast if needed)
//
// Algorithm:
// 1. Collect all batches from all inputs
// 2. Unify schemas (add missing columns as nulls)
// 3. Concatenate all batches
// 4. If UNION (not UNION ALL), deduplicate rows
// 5. Return unified batches
//
// Performance:
// - UNION ALL: O(n) - simple concatenation
// - UNION: O(n log n) - requires sort + unique or hash-based dedup
//
class UnionOperator : public Operator {
public:
    // Constructor
    // - inputs: Vector of input operators to union
    // - deduplicate: If true, performs UNION (with dedup), else UNION ALL
    UnionOperator(std::vector<std::shared_ptr<Operator>> inputs,
                  bool deduplicate = true);

    // Operator interface
    arrow::Result<std::shared_ptr<arrow::Schema>> GetOutputSchema() const override;
    arrow::Result<std::shared_ptr<arrow::RecordBatch>> GetNextBatch() override;
    bool HasNextBatch() const override { return current_batch_ < union_table_->num_rows(); }
    std::string ToString() const override;
    size_t EstimateCardinality() const override;
    const OperatorStats& GetStats() const override { return stats_; }

private:
    // Execute union operation
    arrow::Result<void> ExecuteUnion();

    // Unify schemas across all inputs
    arrow::Result<std::shared_ptr<arrow::Schema>> UnifySchemas();

    // Add missing columns to a batch (with null values)
    arrow::Result<std::shared_ptr<arrow::RecordBatch>> PadBatch(
        const std::shared_ptr<arrow::RecordBatch>& batch,
        const std::shared_ptr<arrow::Schema>& target_schema);

    // Deduplicate rows in a table
    arrow::Result<std::shared_ptr<arrow::Table>> DeduplicateRows(
        const std::shared_ptr<arrow::Table>& table);

    // Check if union has been executed
    bool IsUnionExecuted() const { return union_table_ != nullptr; }

    std::vector<std::shared_ptr<Operator>> inputs_;
    bool deduplicate_;
    std::shared_ptr<arrow::Table> union_table_;
    int64_t current_batch_ = 0;
    size_t batch_size_ = 10000;  // Batch size for returning results
    OperatorStats stats_;
};

} // namespace sabot_ql
