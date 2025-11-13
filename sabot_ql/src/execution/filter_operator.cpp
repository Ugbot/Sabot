#include <sabot_ql/execution/filter_operator.h>
#include <arrow/api.h>
#include <arrow/compute/api.h>
#include <iostream>
#include <sstream>
#include <chrono>
#include <stdexcept>

namespace sabot_ql {

/**
 * Filter Operator - GENERIC across all column families
 *
 * Uses Arrow compute kernels for vectorized filtering.
 * Works with any Arrow table, not just triple stores.
 *
 * This operator can be used by:
 * - SPARQL FILTER expressions
 * - SQL WHERE clauses
 * - Any Sabot component needing row filtering
 *
 * Example usages:
 *   1. SPARQL: FILTER(?age > 30)
 *   2. SQL: WHERE price < 100
 *   3. Stream filtering: filter(lambda x: x > threshold)
 */

ExpressionFilterOperator::ExpressionFilterOperator(
    std::shared_ptr<Operator> child,
    std::shared_ptr<FilterExpression> filter_expr,
    const std::string& description)
    : child_(std::move(child))
    , filter_expr_(std::move(filter_expr))
    , description_(description)
    , executed_(false) {

    if (!child_) {
        throw std::invalid_argument("Child operator cannot be null");
    }
    if (!filter_expr_) {
        throw std::invalid_argument("Filter expression cannot be null");
    }
}

arrow::Result<std::shared_ptr<arrow::Schema>> ExpressionFilterOperator::GetOutputSchema() const {
    // Filter doesn't change schema, just filters rows
    return child_->GetOutputSchema();
}

arrow::Result<std::shared_ptr<arrow::RecordBatch>> ExpressionFilterOperator::GetNextBatch() {
    if (executed_) {
        return nullptr;
    }

    auto start_time = std::chrono::high_resolution_clock::now();

    // Get input from child
    ARROW_ASSIGN_OR_RAISE(auto input_table, child_->GetAllResults());

    // Evaluate filter expression to get boolean mask
    ARROW_ASSIGN_OR_RAISE(auto mask, filter_expr_->Evaluate(input_table));

    // Apply filter using Arrow compute
    arrow::compute::ExecContext ctx;
    ARROW_ASSIGN_OR_RAISE(
        auto filtered_datum,
        arrow::compute::Filter(input_table, mask, arrow::compute::FilterOptions::Defaults(), &ctx)
    );

    auto filtered_table = filtered_datum.table();

    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);

    // Update stats
    stats_.rows_processed = filtered_table->num_rows();
    stats_.batches_processed = 1;
    stats_.execution_time_ms = duration.count();

    executed_ = true;

    // Convert to batch
    if (filtered_table->num_rows() == 0) {
        ARROW_ASSIGN_OR_RAISE(auto schema, GetOutputSchema());
        std::vector<std::shared_ptr<arrow::Array>> empty_arrays;
        return arrow::RecordBatch::Make(schema, 0, empty_arrays);
    }

    ARROW_ASSIGN_OR_RAISE(auto combined_table, filtered_table->CombineChunks());

    // Extract arrays from table
    std::vector<std::shared_ptr<arrow::Array>> arrays;
    for (int i = 0; i < combined_table->num_columns(); ++i) {
        arrays.push_back(combined_table->column(i)->chunk(0));
    }

    return arrow::RecordBatch::Make(combined_table->schema(), combined_table->num_rows(), arrays);
}

bool ExpressionFilterOperator::HasNextBatch() const {
    return !executed_;
}

std::string ExpressionFilterOperator::ToString() const {
    if (!description_.empty()) {
        return description_;
    }

    return "Filter(" + filter_expr_->ToString() + ")";
}

size_t ExpressionFilterOperator::EstimateCardinality() const {
    // Estimate: assume filter reduces rows by 50% (conservative)
    return child_->EstimateCardinality() / 2;
}

//==============================================================================
// Filter Expression Implementations
//==============================================================================

/**
 * ComparisonExpression: Generic comparison for any Arrow columns
 *
 * Works with any column family - compares values using Arrow compute kernels
 */

ComparisonExpression::ComparisonExpression(
    const std::string& column_name,
    ComparisonOp op,
    std::shared_ptr<arrow::Scalar> value)
    : column_name_(column_name)
    , op_(op)
    , value_(std::move(value)) {}

arrow::Result<std::shared_ptr<arrow::Array>> ComparisonExpression::Evaluate(
    const std::shared_ptr<arrow::Table>& table) const {

    // Get column to filter
    auto column_idx = table->schema()->GetFieldIndex(column_name_);
    if (column_idx == -1) {
        return arrow::Status::Invalid("Column not found: " + column_name_);
    }

    auto column = table->column(column_idx)->chunk(0);

    // Create scalar array for comparison
    ARROW_ASSIGN_OR_RAISE(
        auto scalar_array,
        arrow::MakeArrayFromScalar(*value_, column->length())
    );

    // Apply comparison using Arrow compute CallFunction API
    arrow::compute::ExecContext ctx;
    arrow::Datum result_datum;

    std::string func_name;
    switch (op_) {
        case ComparisonOp::EQ:
            func_name = "equal";
            break;
        case ComparisonOp::NEQ:
            func_name = "not_equal";
            break;
        case ComparisonOp::LT:
            func_name = "less";
            break;
        case ComparisonOp::LTE:
            func_name = "less_equal";
            break;
        case ComparisonOp::GT:
            func_name = "greater";
            break;
        case ComparisonOp::GTE:
            func_name = "greater_equal";
            break;
        default:
            return arrow::Status::Invalid("Unknown comparison operator");
    }

    ARROW_ASSIGN_OR_RAISE(
        result_datum,
        arrow::compute::CallFunction(func_name, {column, scalar_array}, &ctx)
    );

    return result_datum.make_array();
}

std::string ComparisonExpression::ToString() const {
    std::string op_str;
    switch (op_) {
        case ComparisonOp::EQ: op_str = "="; break;
        case ComparisonOp::NEQ: op_str = "!="; break;
        case ComparisonOp::LT: op_str = "<"; break;
        case ComparisonOp::LTE: op_str = "<="; break;
        case ComparisonOp::GT: op_str = ">"; break;
        case ComparisonOp::GTE: op_str = ">="; break;
    }

    return column_name_ + " " + op_str + " " + value_->ToString();
}

/**
 * ColumnComparisonExpression: Generic column-to-column comparison
 *
 * Works with any column family - compares two columns using Arrow compute kernels
 * Used for FILTER(?x != ?y) patterns in SPARQL
 */

ColumnComparisonExpression::ColumnComparisonExpression(
    const std::string& left_column,
    ComparisonOp op,
    const std::string& right_column)
    : left_column_(left_column)
    , op_(op)
    , right_column_(right_column) {}

arrow::Result<std::shared_ptr<arrow::Array>> ColumnComparisonExpression::Evaluate(
    const std::shared_ptr<arrow::Table>& table) const {

    // Get left column
    auto left_idx = table->schema()->GetFieldIndex(left_column_);
    if (left_idx == -1) {
        return arrow::Status::Invalid("Column not found: " + left_column_);
    }

    // Get right column
    auto right_idx = table->schema()->GetFieldIndex(right_column_);
    if (right_idx == -1) {
        return arrow::Status::Invalid("Column not found: " + right_column_);
    }

    auto left_array = table->column(left_idx)->chunk(0);
    auto right_array = table->column(right_idx)->chunk(0);

    // Apply comparison using Arrow compute CallFunction API
    arrow::compute::ExecContext ctx;
    arrow::Datum result_datum;

    std::string func_name;
    switch (op_) {
        case ComparisonOp::EQ:
            func_name = "equal";
            break;
        case ComparisonOp::NEQ:
            func_name = "not_equal";
            break;
        case ComparisonOp::LT:
            func_name = "less";
            break;
        case ComparisonOp::LTE:
            func_name = "less_equal";
            break;
        case ComparisonOp::GT:
            func_name = "greater";
            break;
        case ComparisonOp::GTE:
            func_name = "greater_equal";
            break;
        default:
            return arrow::Status::Invalid("Unknown comparison operator");
    }

    // Compare two columns directly using Arrow compute kernel
    ARROW_ASSIGN_OR_RAISE(
        result_datum,
        arrow::compute::CallFunction(func_name, {left_array, right_array}, &ctx)
    );

    return result_datum.make_array();
}

std::string ColumnComparisonExpression::ToString() const {
    std::string op_str;
    switch (op_) {
        case ComparisonOp::EQ: op_str = "="; break;
        case ComparisonOp::NEQ: op_str = "!="; break;
        case ComparisonOp::LT: op_str = "<"; break;
        case ComparisonOp::LTE: op_str = "<="; break;
        case ComparisonOp::GT: op_str = ">"; break;
        case ComparisonOp::GTE: op_str = ">="; break;
    }

    return left_column_ + " " + op_str + " " + right_column_;
}

/**
 * LogicalExpression: Generic AND/OR/NOT for any filter
 *
 * Works with any column family - combines boolean masks
 */

LogicalExpression::LogicalExpression(
    LogicalOp op,
    std::vector<std::shared_ptr<FilterExpression>> operands)
    : op_(op)
    , operands_(std::move(operands)) {

    if (operands_.empty()) {
        throw std::invalid_argument("Logical expression needs operands");
    }

    if (op == LogicalOp::NOT && operands_.size() != 1) {
        throw std::invalid_argument("NOT operator requires exactly one operand");
    }
}

arrow::Result<std::shared_ptr<arrow::Array>> LogicalExpression::Evaluate(
    const std::shared_ptr<arrow::Table>& table) const {

    // Evaluate all operands
    std::vector<std::shared_ptr<arrow::Array>> operand_results;
    for (const auto& operand : operands_) {
        ARROW_ASSIGN_OR_RAISE(auto result, operand->Evaluate(table));
        operand_results.push_back(result);
    }

    arrow::compute::ExecContext ctx;

    switch (op_) {
        case LogicalOp::AND: {
            // AND all masks together
            auto result = operand_results[0];
            for (size_t i = 1; i < operand_results.size(); ++i) {
                ARROW_ASSIGN_OR_RAISE(
                    auto and_datum,
                    arrow::compute::CallFunction("and", {result, operand_results[i]}, &ctx)
                );
                result = and_datum.make_array();
            }
            return result;
        }

        case LogicalOp::OR: {
            // OR all masks together
            auto result = operand_results[0];
            for (size_t i = 1; i < operand_results.size(); ++i) {
                ARROW_ASSIGN_OR_RAISE(
                    auto or_datum,
                    arrow::compute::CallFunction("or", {result, operand_results[i]}, &ctx)
                );
                result = or_datum.make_array();
            }
            return result;
        }

        case LogicalOp::NOT: {
            // Invert mask
            ARROW_ASSIGN_OR_RAISE(
                auto not_datum,
                arrow::compute::CallFunction("invert", {operand_results[0]}, &ctx)
            );
            return not_datum.make_array();
        }

        default:
            return arrow::Status::Invalid("Unknown logical operator");
    }
}

std::string LogicalExpression::ToString() const {
    if (op_ == LogicalOp::NOT) {
        return "NOT(" + operands_[0]->ToString() + ")";
    }

    std::string op_str = (op_ == LogicalOp::AND) ? " AND " : " OR ";
    std::string result = operands_[0]->ToString();
    for (size_t i = 1; i < operands_.size(); ++i) {
        result += op_str + operands_[i]->ToString();
    }
    return "(" + result + ")";
}

} // namespace sabot_ql
