#pragma once

#include "sabot_sql/operators/operator.h"
#include <arrow/compute/api.h>
#include <functional>
#include <memory>

namespace sabot_sql {
namespace operators {

/**
 * Filter operator - applies predicates using Arrow compute
 * 
 * Uses SIMD-optimized Arrow compute kernels for:
 * - Numeric comparisons (>, <, ==, !=, >=, <=)
 * - String comparisons (==, !=, LIKE, regex)
 * - Boolean logic (AND, OR, NOT)
 */
class FilterOperator : public Operator {
public:
    /**
     * Create filter operator
     * 
     * @param child Child operator providing input data
     * @param predicate_expr Predicate expression (column, op, value)
     */
    FilterOperator(
        std::shared_ptr<Operator> child,
        const std::string& column_name,
        const std::string& op,  // "=", "!=", ">", "<", ">=", "<=", "LIKE", "~"
        const std::string& value
    );
    
    ~FilterOperator() override = default;
    
    // Operator interface
    arrow::Result<std::shared_ptr<arrow::Schema>> GetOutputSchema() const override;
    arrow::Result<std::shared_ptr<arrow::RecordBatch>> GetNextBatch() override;
    bool HasNextBatch() const override;
    std::string ToString() const override;
    size_t EstimateCardinality() const override;
    arrow::Result<std::shared_ptr<arrow::Table>> GetAllResults() override;

private:
    std::shared_ptr<Operator> child_;
    std::string column_name_;
    std::string op_;
    std::string value_;
    
    // Apply filter using Arrow compute
    arrow::Result<std::shared_ptr<arrow::RecordBatch>>
    ApplyFilter(const std::shared_ptr<arrow::RecordBatch>& batch);
    
    // Create predicate mask for different operations
    arrow::Result<std::shared_ptr<arrow::BooleanArray>>
    CreatePredicateMask(const std::shared_ptr<arrow::Array>& column);
};

} // namespace operators
} // namespace sabot_sql

