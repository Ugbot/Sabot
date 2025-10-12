#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <arrow/api.h>
#include <arrow/result.h>
#include "sabot_sql/operators/operator.h"

namespace sabot_sql {

/**
 * @brief CTE (Common Table Expression) Operator
 * 
 * Materializes the result of a CTE for reuse in the query.
 * CTEs can be referenced multiple times, so we cache the results.
 * 
 * Features:
 * - Lazy materialization (only execute when first accessed)
 * - Result caching (in-memory or RocksDB for large results)
 * - Support for recursive CTEs (future)
 * 
 * Example SQL:
 *   WITH high_value_orders AS (
 *       SELECT * FROM orders WHERE amount > 10000
 *   )
 *   SELECT * FROM high_value_orders WHERE customer_id = 123;
 */
class CTEOperator : public Operator {
public:
    /**
     * @brief Create a CTE operator
     * @param name CTE name
     * @param source Operator producing CTE results
     * @param materialize_immediately If true, execute and cache immediately
     */
    CTEOperator(const std::string& name,
                std::shared_ptr<Operator> source,
                bool materialize_immediately = false);
    
    ~CTEOperator() override = default;
    
    // Operator interface
    arrow::Result<std::shared_ptr<arrow::Schema>> GetOutputSchema() const override;
    arrow::Result<std::shared_ptr<arrow::RecordBatch>> GetNextBatch() override;
    bool HasNextBatch() const override;
    std::string ToString() const override;
    size_t EstimateCardinality() const override;
    
    /**
     * @brief Force materialization of CTE results
     */
    arrow::Status Materialize();
    
    /**
     * @brief Check if CTE has been materialized
     */
    bool IsMaterialized() const { return materialized_; }
    
    /**
     * @brief Get the CTE name
     */
    const std::string& GetName() const { return name_; }
    
    /**
     * @brief Reset to re-read from beginning
     */
    void Reset();
    
private:
    std::string name_;
    std::shared_ptr<Operator> source_;
    bool materialize_immediately_;
    bool materialized_;
    
    // Cached results
    std::shared_ptr<arrow::Table> materialized_table_;
    size_t current_batch_;
    size_t batch_size_;
};

/**
 * @brief CTE Registry - manages CTEs for a query
 * 
 * Tracks all CTEs defined in a query and provides access to their results.
 * Multiple operators can reference the same CTE.
 */
class CTERegistry {
public:
    CTERegistry() = default;
    
    /**
     * @brief Register a CTE
     */
    void RegisterCTE(const std::string& name, std::shared_ptr<CTEOperator> cte_op);
    
    /**
     * @brief Get a CTE by name
     */
    std::shared_ptr<CTEOperator> GetCTE(const std::string& name);
    
    /**
     * @brief Check if a CTE exists
     */
    bool HasCTE(const std::string& name) const;
    
    /**
     * @brief Materialize all CTEs
     */
    arrow::Status MaterializeAll();
    
    /**
     * @brief Clear all CTEs
     */
    void Clear();
    
private:
    std::unordered_map<std::string, std::shared_ptr<CTEOperator>> ctes_;
};

} // namespace sabot_sql

