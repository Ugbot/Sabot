#pragma once

#include <sabot_ql/operators/operator.h>
#include <arrow/api.h>
#include <memory>
#include <string>
#include <vector>

namespace sabot_ql {

//==============================================================================
// Filter Expression Base Class - GENERIC
//==============================================================================

/**
 * @brief Base class for filter expressions
 *
 * GENERIC: Works with any Arrow table from any column family.
 * Can be used across Sabot for:
 * - SPARQL FILTER
 * - SQL WHERE
 * - Streaming filters
 * - Any row-level filtering
 */
class FilterExpression {
public:
    virtual ~FilterExpression() = default;

    /**
     * @brief Evaluate expression on table to produce boolean mask
     * @param table Input table (any schema, any column family)
     * @return Boolean array indicating which rows match
     */
    virtual arrow::Result<std::shared_ptr<arrow::Array>> Evaluate(
        const std::shared_ptr<arrow::Table>& table
    ) const = 0;

    /**
     * @brief Get human-readable string representation
     */
    virtual std::string ToString() const = 0;
};

//==============================================================================
// Comparison Expression - GENERIC
//==============================================================================

enum class ComparisonOp {
    EQ,   // =
    NEQ,  // !=
    LT,   // <
    LTE,  // <=
    GT,   // >
    GTE   // >=
};

/**
 * @brief Comparison expression for filtering
 *
 * GENERIC: Compares any column to a scalar value.
 * Uses Arrow compute kernels for vectorized execution.
 *
 * Example:
 *   // Filter triple store: subject > 100
 *   ComparisonExpression("subject", ComparisonOp::GT, MakeScalar(100))
 *
 *   // Filter timeseries: timestamp >= start_time
 *   ComparisonExpression("timestamp", ComparisonOp::GTE, MakeScalar(start_time))
 *
 *   // Filter SQL: age < 30
 *   ComparisonExpression("age", ComparisonOp::LT, MakeScalar(30))
 */
class ComparisonExpression : public FilterExpression {
public:
    ComparisonExpression(
        const std::string& column_name,
        ComparisonOp op,
        std::shared_ptr<arrow::Scalar> value
    );

    arrow::Result<std::shared_ptr<arrow::Array>> Evaluate(
        const std::shared_ptr<arrow::Table>& table
    ) const override;

    std::string ToString() const override;

private:
    std::string column_name_;
    ComparisonOp op_;
    std::shared_ptr<arrow::Scalar> value_;
};

//==============================================================================
// Column Comparison Expression - GENERIC
//==============================================================================

/**
 * @brief Column-to-column comparison expression for filtering
 *
 * GENERIC: Compares two columns instead of column-to-scalar.
 * Uses Arrow compute kernels for vectorized column comparisons.
 *
 * Example:
 *   // Filter: ?athlete1 != ?athlete2
 *   ColumnComparisonExpression("athlete1", ComparisonOp::NEQ, "athlete2")
 *
 *   // Filter: start_time < end_time
 *   ColumnComparisonExpression("start_time", ComparisonOp::LT, "end_time")
 *
 *   // Filter: price >= cost
 *   ColumnComparisonExpression("price", ComparisonOp::GTE, "cost")
 */
class ColumnComparisonExpression : public FilterExpression {
public:
    ColumnComparisonExpression(
        const std::string& left_column,
        ComparisonOp op,
        const std::string& right_column
    );

    arrow::Result<std::shared_ptr<arrow::Array>> Evaluate(
        const std::shared_ptr<arrow::Table>& table
    ) const override;

    std::string ToString() const override;

private:
    std::string left_column_;
    ComparisonOp op_;
    std::string right_column_;
};

//==============================================================================
// Logical Expression - GENERIC
//==============================================================================

enum class LogicalOp {
    AND,  // &&
    OR,   // ||
    NOT   // !
};

/**
 * @brief Logical combination of filter expressions
 *
 * GENERIC: Combines any filter expressions with AND/OR/NOT.
 * Uses Arrow compute kernels for vectorized boolean operations.
 *
 * Example:
 *   // (age > 18) AND (age < 65)
 *   LogicalExpression(LogicalOp::AND, {
 *       ComparisonExpression("age", ComparisonOp::GT, MakeScalar(18)),
 *       ComparisonExpression("age", ComparisonOp::LT, MakeScalar(65))
 *   })
 *
 *   // (type = "Person") OR (type = "Organization")
 *   LogicalExpression(LogicalOp::OR, {
 *       ComparisonExpression("type", ComparisonOp::EQ, MakeScalar("Person")),
 *       ComparisonExpression("type", ComparisonOp::EQ, MakeScalar("Organization"))
 *   })
 */
class LogicalExpression : public FilterExpression {
public:
    LogicalExpression(
        LogicalOp op,
        std::vector<std::shared_ptr<FilterExpression>> operands
    );

    arrow::Result<std::shared_ptr<arrow::Array>> Evaluate(
        const std::shared_ptr<arrow::Table>& table
    ) const override;

    std::string ToString() const override;

private:
    LogicalOp op_;
    std::vector<std::shared_ptr<FilterExpression>> operands_;
};

//==============================================================================
// Filter Operator - GENERIC
//==============================================================================

/**
 * @brief Expression-based filter operator for row-level filtering
 *
 * GENERIC OPERATOR: Works with any Arrow table from any column family.
 * Reusable across all Sabot components.
 *
 * Uses Arrow compute kernels for high-performance vectorized filtering.
 * Supports complex filter expressions (comparisons + logical ops).
 *
 * Note: Named ExpressionFilterOperator to avoid conflict with legacy FilterOperator
 * TODO: Migrate legacy code to use this implementation
 *
 * Example SPARQL:
 *   SELECT ?s ?p WHERE {
 *       ?s ?p ?o .
 *       FILTER(?p > 10)
 *   }
 *
 * Operator tree:
 *   ExpressionFilter(p > 10)
 *     └─ Scan(?s ?p ?o)
 *
 * Example SQL:
 *   SELECT * FROM triples WHERE subject > 100
 *
 * Operator tree:
 *   ExpressionFilter(subject > 100)
 *     └─ TableScan(triples)
 *
 * Performance:
 * - Arrow compute kernels use SIMD
 * - Minimal memory overhead (boolean mask)
 * - Works on any Arrow type (int64, string, etc.)
 */
class ExpressionFilterOperator : public Operator {
public:
    /**
     * @brief Construct a filter operator
     * @param child Input operator (any data source)
     * @param filter_expr Filter predicate (works with any columns)
     * @param description Optional human-readable description
     */
    ExpressionFilterOperator(
        std::shared_ptr<Operator> child,
        std::shared_ptr<FilterExpression> filter_expr,
        const std::string& description = ""
    );

    // Operator interface
    arrow::Result<std::shared_ptr<arrow::Schema>> GetOutputSchema() const override;
    arrow::Result<std::shared_ptr<arrow::RecordBatch>> GetNextBatch() override;
    bool HasNextBatch() const override;
    std::string ToString() const override;
    size_t EstimateCardinality() const override;

    /**
     * @brief Get filter expression
     */
    const std::shared_ptr<FilterExpression>& GetFilterExpression() const {
        return filter_expr_;
    }

private:
    std::shared_ptr<Operator> child_;
    std::shared_ptr<FilterExpression> filter_expr_;
    std::string description_;
    bool executed_;
};

} // namespace sabot_ql
