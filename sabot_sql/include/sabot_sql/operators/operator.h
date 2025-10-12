#pragma once

#include <memory>
#include <string>
#include <vector>
#include <unordered_map>
#include <arrow/api.h>
#include <arrow/result.h>

namespace sabot_sql {
namespace operators {

/**
 * @brief Base operator interface for SabotSQL
 * 
 * All SabotSQL operators implement this interface to provide
 * a consistent API for query execution.
 */
class Operator {
public:
    virtual ~Operator() = default;
    
    /**
     * @brief Get the output schema of this operator
     */
    virtual arrow::Result<std::shared_ptr<arrow::Schema>> GetOutputSchema() const = 0;
    
    /**
     * @brief Get the next batch of results
     */
    virtual arrow::Result<std::shared_ptr<arrow::RecordBatch>> GetNextBatch() = 0;
    
    /**
     * @brief Check if there are more batches available
     */
    virtual bool HasNextBatch() const = 0;
    
    /**
     * @brief Get a string representation of this operator
     */
    virtual std::string ToString() const = 0;
    
    /**
     * @brief Estimate the cardinality of this operator's output
     */
    virtual size_t EstimateCardinality() const = 0;
    
    /**
     * @brief Get all results as a single Arrow table
     */
    virtual arrow::Result<std::shared_ptr<arrow::Table>> GetAllResults() = 0;
    
    /**
     * @brief Get operator statistics
     */
    struct Statistics {
        size_t rows_processed = 0;
        size_t batches_processed = 0;
        double execution_time_ms = 0.0;
        size_t memory_usage_bytes = 0;
    };
    
    Statistics stats_;
};

} // namespace operators
} // namespace sabot_sql
