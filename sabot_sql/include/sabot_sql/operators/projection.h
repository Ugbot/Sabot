#pragma once

#include "sabot_sql/operators/operator.h"
#include <vector>
#include <string>

namespace sabot_sql {
namespace operators {

/**
 * Projection operator - selects specific columns
 * 
 * Zero-copy column selection using Arrow's column extraction
 */
class ProjectionOperator : public Operator {
public:
    /**
     * Create projection operator
     * 
     * @param child Child operator providing input data
     * @param column_names Columns to select
     */
    ProjectionOperator(
        std::shared_ptr<Operator> child,
        const std::vector<std::string>& column_names
    );
    
    ~ProjectionOperator() override = default;
    
    // Operator interface
    arrow::Result<std::shared_ptr<arrow::Schema>> GetOutputSchema() const override;
    arrow::Result<std::shared_ptr<arrow::RecordBatch>> GetNextBatch() override;
    bool HasNextBatch() const override;
    std::string ToString() const override;
    size_t EstimateCardinality() const override;
    arrow::Result<std::shared_ptr<arrow::Table>> GetAllResults() override;

private:
    std::shared_ptr<Operator> child_;
    std::vector<std::string> column_names_;
    std::vector<int> column_indices_;
    std::shared_ptr<arrow::Schema> output_schema_;
    
    // Initialize column indices and output schema
    arrow::Status Initialize();
    
    // Project columns from batch
    arrow::Result<std::shared_ptr<arrow::RecordBatch>>
    ProjectBatch(const std::shared_ptr<arrow::RecordBatch>& batch);
};

} // namespace operators
} // namespace sabot_sql

