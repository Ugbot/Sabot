#pragma once

#include "sabot_sql/operators/operator.h"
#include <vector>
#include <string>

namespace sabot_sql {
namespace operators {

/**
 * Sort order
 */
enum class SortOrder {
    ASCENDING,
    DESCENDING
};

/**
 * Sort specification
 */
struct SortSpec {
    std::string column_name;
    SortOrder order;
};

/**
 * Sort operator - sorts data using Arrow compute
 * 
 * Uses arrow::compute::sort_indices for SIMD-optimized sorting
 */
class SortOperator : public Operator {
public:
    /**
     * Create sort operator
     * 
     * @param child Child operator providing input data
     * @param sort_keys Columns to sort by with order
     * @param limit Optional limit (for ORDER BY ... LIMIT optimization)
     */
    SortOperator(
        std::shared_ptr<Operator> child,
        const std::vector<SortSpec>& sort_keys,
        int64_t limit = -1
    );
    
    ~SortOperator() override = default;
    
    // Operator interface
    arrow::Result<std::shared_ptr<arrow::Schema>> GetOutputSchema() const override;
    arrow::Result<std::shared_ptr<arrow::RecordBatch>> GetNextBatch() override;
    bool HasNextBatch() const override;
    std::string ToString() const override;
    size_t EstimateCardinality() const override;
    arrow::Result<std::shared_ptr<arrow::Table>> GetAllResults() override;

private:
    std::shared_ptr<Operator> child_;
    std::vector<SortSpec> sort_keys_;
    int64_t limit_;
    bool result_computed_;
    std::shared_ptr<arrow::Table> result_;
    
    // Perform sort
    arrow::Result<std::shared_ptr<arrow::Table>> PerformSort();
};

/**
 * Limit operator - limits output rows
 * 
 * Simple wrapper that stops after N rows
 */
class LimitOperator : public Operator {
public:
    LimitOperator(std::shared_ptr<Operator> child, int64_t limit);
    
    ~LimitOperator() override = default;
    
    // Operator interface
    arrow::Result<std::shared_ptr<arrow::Schema>> GetOutputSchema() const override;
    arrow::Result<std::shared_ptr<arrow::RecordBatch>> GetNextBatch() override;
    bool HasNextBatch() const override;
    std::string ToString() const override;
    size_t EstimateCardinality() const override;
    arrow::Result<std::shared_ptr<arrow::Table>> GetAllResults() override;

private:
    std::shared_ptr<Operator> child_;
    int64_t limit_;
    int64_t rows_returned_;
};

} // namespace operators
} // namespace sabot_sql

