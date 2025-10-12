#pragma once

#include <memory>
#include <string>
#include <functional>
#include <arrow/api.h>
#include <arrow/result.h>
#include "sabot_sql/operators/operator.h"

namespace sabot_sql {

/**
 * @brief Subquery Operator Types
 */
enum class SubqueryType {
    SCALAR,         // Returns single value (SELECT max(price) FROM products)
    EXISTS,         // Returns boolean (EXISTS (SELECT * FROM orders WHERE ...))
    IN,             // Returns set for membership test (id IN (SELECT ...))
    CORRELATED,     // Correlated subquery (depends on outer query)
    UNCORRELATED    // Uncorrelated subquery (independent)
};

/**
 * @brief Subquery Operator
 * 
 * Executes subqueries within SQL statements.
 * 
 * Types of subqueries:
 * 1. Scalar subqueries: Return single value
 * 2. EXISTS subqueries: Check existence
 * 3. IN subqueries: Membership test
 * 4. Correlated: Re-execute for each outer row
 * 5. Uncorrelated: Execute once
 * 
 * Examples:
 *   - Scalar: SELECT (SELECT MAX(price) FROM products) as max_price;
 *   - EXISTS: SELECT * FROM customers WHERE EXISTS (SELECT * FROM orders WHERE ...);
 *   - IN: SELECT * FROM products WHERE category_id IN (SELECT id FROM categories WHERE ...);
 *   - Correlated: SELECT * FROM customers c WHERE (SELECT COUNT(*) FROM orders o WHERE o.customer_id = c.id) > 10;
 */
class SubqueryOperator : public operators::Operator {
public:
    /**
     * @brief Create a subquery operator
     * @param outer Outer query operator (for correlated subqueries)
     * @param subquery Subquery operator
     * @param type Subquery type
     */
    SubqueryOperator(std::shared_ptr<operators::Operator> outer,
                     std::shared_ptr<operators::Operator> subquery,
                     SubqueryType type);
    
    ~SubqueryOperator() override = default;
    
    // Operator interface
    arrow::Result<std::shared_ptr<arrow::Schema>> GetOutputSchema() const override;
    arrow::Result<std::shared_ptr<arrow::RecordBatch>> GetNextBatch() override;
    std::string ToString() const override;
    size_t EstimateCardinality() const override;
    arrow::Result<std::shared_ptr<arrow::Table>> GetAllResults() override;
    
    /**
     * @brief Set correlation predicate (for correlated subqueries)
     * The predicate takes outer row and returns filtered subquery results
     */
    void SetCorrelationPredicate(
        std::function<arrow::Result<std::shared_ptr<arrow::Table>>(
            const arrow::RecordBatch&)> predicate);
    
    /**
     * @brief Get subquery type
     */
    SubqueryType GetSubqueryType() const { return type_; }
    
private:
    std::shared_ptr<operators::Operator> subquery_;
    SubqueryType type_;
    
    // For correlated subqueries
    std::function<arrow::Result<std::shared_ptr<arrow::Table>>(
        const arrow::RecordBatch&)> correlation_predicate_;
    
    // Cached subquery result (for uncorrelated subqueries)
    std::shared_ptr<arrow::Table> cached_result_;
    bool result_cached_;
    
    // Execution helpers
    arrow::Result<std::shared_ptr<arrow::RecordBatch>> 
        ExecuteScalarSubquery();
    
    arrow::Result<std::shared_ptr<arrow::RecordBatch>> 
        ExecuteExistsSubquery();
    
    arrow::Result<std::shared_ptr<arrow::RecordBatch>> 
        ExecuteInSubquery();
    
    arrow::Result<std::shared_ptr<arrow::RecordBatch>> 
        ExecuteCorrelatedSubquery();
    
    arrow::Result<std::shared_ptr<arrow::RecordBatch>> 
        ExecuteUncorrelatedSubquery();
};

/**
 * @brief Helper to convert subquery results for IN/EXISTS checks
 */
class SubqueryResultConverter {
public:
    /**
     * @brief Convert table to set for IN checks
     */
    static arrow::Result<std::shared_ptr<arrow::Array>> 
        TableToSet(const std::shared_ptr<arrow::Table>& table,
                  const std::string& column_name);
    
    /**
     * @brief Convert table to boolean for EXISTS
     */
    static arrow::Result<bool> 
        TableToExists(const std::shared_ptr<arrow::Table>& table);
    
    /**
     * @brief Convert table to scalar value
     */
    static arrow::Result<std::shared_ptr<arrow::Scalar>> 
        TableToScalar(const std::shared_ptr<arrow::Table>& table);
};

} // namespace sabot_sql

