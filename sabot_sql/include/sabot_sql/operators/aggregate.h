#pragma once

#include "sabot_sql/operators/operator.h"
#include <arrow/compute/api.h>
#include <vector>
#include <string>

namespace sabot_sql {
namespace operators {

/**
 * Aggregation types
 */
enum class AggregationType {
    COUNT,
    COUNT_DISTINCT,
    SUM,
    AVG,
    MIN,
    MAX
};

/**
 * Aggregation specification
 */
struct AggregationSpec {
    AggregationType type;
    std::string column_name;  // Empty for COUNT(*)
    std::string output_name;
};

/**
 * Aggregate operator - computes aggregations using Arrow compute
 * 
 * Supports:
 * - COUNT, COUNT DISTINCT, SUM, AVG, MIN, MAX
 * - GROUP BY (using Arrow's group_by kernel)
 * - Multiple aggregations in one pass
 */
class AggregateOperator : public Operator {
public:
    /**
     * Create aggregate operator without grouping
     * 
     * @param child Child operator providing input data
     * @param aggregations List of aggregations to compute
     */
    AggregateOperator(
        std::shared_ptr<Operator> child,
        const std::vector<AggregationSpec>& aggregations
    );
    
    /**
     * Create aggregate operator with GROUP BY
     * 
     * @param child Child operator providing input data
     * @param group_by_columns Columns to group by
     * @param aggregations List of aggregations to compute
     */
    AggregateOperator(
        std::shared_ptr<Operator> child,
        const std::vector<std::string>& group_by_columns,
        const std::vector<AggregationSpec>& aggregations
    );
    
    ~AggregateOperator() override = default;
    
    // Operator interface
    arrow::Result<std::shared_ptr<arrow::Schema>> GetOutputSchema() const override;
    arrow::Result<std::shared_ptr<arrow::RecordBatch>> GetNextBatch() override;
    bool HasNextBatch() const override;
    std::string ToString() const override;
    size_t EstimateCardinality() const override;
    arrow::Result<std::shared_ptr<arrow::Table>> GetAllResults() override;

private:
    std::shared_ptr<Operator> child_;
    std::vector<std::string> group_by_columns_;
    std::vector<AggregationSpec> aggregations_;
    bool has_group_by_;
    bool result_computed_;
    std::shared_ptr<arrow::Table> result_;
    
    // Compute aggregations
    arrow::Result<std::shared_ptr<arrow::Table>> ComputeAggregations();
    
    // Compute single aggregation
    arrow::Result<std::shared_ptr<arrow::Scalar>>
    ComputeSingleAggregation(
        const std::shared_ptr<arrow::Table>& table,
        const AggregationSpec& agg
    );
};

} // namespace operators
} // namespace sabot_sql

