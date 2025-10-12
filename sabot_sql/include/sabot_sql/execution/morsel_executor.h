#pragma once

#include <memory>
#include <vector>
#include <functional>
#include <arrow/api.h>
#include <arrow/result.h>

namespace sabot_sql {
namespace execution {

/**
 * @brief Morsel-driven SQL execution engine
 * 
 * This replaces DuckDB's execution engine with Sabot's morsel-driven approach.
 * Instead of DuckDB's physical operators, we use Sabot's morsel operators.
 */
class MorselExecutor {
public:
    /**
     * @brief Create a morsel executor
     */
    static arrow::Result<std::shared_ptr<MorselExecutor>> Create();

    ~MorselExecutor() = default;

    /**
     * @brief Execute a logical plan using morsel-driven execution
     * @param logical_plan The logical plan to execute
     * @return Arrow table result
     */
    arrow::Result<std::shared_ptr<arrow::Table>> 
        ExecutePlan(std::shared_ptr<void> logical_plan);

    /**
     * @brief Execute a logical plan and return streaming batches
     * @param logical_plan The logical plan to execute
     * @return Generator of record batches
     */
    std::function<arrow::Result<std::shared_ptr<arrow::RecordBatch>>()>
        ExecutePlanStreaming(std::shared_ptr<void> logical_plan);

    /**
     * @brief Set morsel size for execution
     */
    void SetMorselSize(size_t morsel_size) { morsel_size_ = morsel_size; }

    /**
     * @brief Set number of worker threads
     */
    void SetNumWorkers(size_t num_workers) { num_workers_ = num_workers; }

private:
    explicit MorselExecutor();

    size_t morsel_size_;
    size_t num_workers_;
    
    // Convert logical plan to Sabot morsel operators
    arrow::Result<std::shared_ptr<void>> 
        ConvertToMorselOperators(std::shared_ptr<void> logical_plan);
    
    // Execute morsel operators
    arrow::Result<std::shared_ptr<arrow::Table>>
        ExecuteMorselOperators(std::shared_ptr<void> morsel_plan);
};

} // namespace execution
} // namespace sabot_sql
