#pragma once

#include <sabot_ql/sparql/ast.h>
#include <sabot_ql/storage/vocabulary.h>
#include <arrow/api.h>
#include <arrow/compute/api.h>
#include <arrow/result.h>
#include <functional>
#include <memory>
#include <unordered_map>

namespace sabot_ql {
namespace sparql {

// Context for expression evaluation
// Holds information needed to evaluate expressions
struct EvaluationContext {
    std::shared_ptr<Vocabulary> vocab;
    std::unordered_map<std::string, std::string> var_to_column;  // Variable name -> column name

    EvaluationContext(std::shared_ptr<Vocabulary> v,
                     std::unordered_map<std::string, std::string> mapping)
        : vocab(std::move(v)), var_to_column(std::move(mapping)) {}
};

// Expression evaluator: converts SPARQL expressions to Arrow compute operations
// This is the core of FILTER clause implementation
class ExpressionEvaluator {
public:
    explicit ExpressionEvaluator(EvaluationContext ctx)
        : ctx_(std::move(ctx)) {}

    // Main entry point: evaluate an expression on a RecordBatch
    // Returns a BooleanArray indicating which rows pass the filter
    arrow::Result<std::shared_ptr<arrow::BooleanArray>> Evaluate(
        const std::shared_ptr<Expression>& expr,
        const std::shared_ptr<arrow::RecordBatch>& batch);

private:
    // Evaluate different expression types
    arrow::Result<std::shared_ptr<arrow::Array>> EvaluateNode(
        const std::shared_ptr<Expression>& expr,
        const std::shared_ptr<arrow::RecordBatch>& batch);

    // Comparison operators
    arrow::Result<std::shared_ptr<arrow::BooleanArray>> EvaluateComparison(
        ExprOperator op,
        const std::shared_ptr<arrow::Array>& left,
        const std::shared_ptr<arrow::Array>& right);

    // Logical operators
    arrow::Result<std::shared_ptr<arrow::BooleanArray>> EvaluateLogical(
        ExprOperator op,
        const std::vector<std::shared_ptr<Expression>>& args,
        const std::shared_ptr<arrow::RecordBatch>& batch);

    // Arithmetic operators
    arrow::Result<std::shared_ptr<arrow::Array>> EvaluateArithmetic(
        ExprOperator op,
        const std::shared_ptr<arrow::Array>& left,
        const std::shared_ptr<arrow::Array>& right);

    // Built-in functions
    arrow::Result<std::shared_ptr<arrow::BooleanArray>> EvaluateBound(
        const std::shared_ptr<Expression>& arg,
        const std::shared_ptr<arrow::RecordBatch>& batch);

    arrow::Result<std::shared_ptr<arrow::BooleanArray>> EvaluateIsIRI(
        const std::shared_ptr<Expression>& arg,
        const std::shared_ptr<arrow::RecordBatch>& batch);

    arrow::Result<std::shared_ptr<arrow::BooleanArray>> EvaluateIsLiteral(
        const std::shared_ptr<Expression>& arg,
        const std::shared_ptr<arrow::RecordBatch>& batch);

    arrow::Result<std::shared_ptr<arrow::BooleanArray>> EvaluateIsBlank(
        const std::shared_ptr<Expression>& arg,
        const std::shared_ptr<arrow::RecordBatch>& batch);

    arrow::Result<std::shared_ptr<arrow::Array>> EvaluateStr(
        const std::shared_ptr<Expression>& arg,
        const std::shared_ptr<arrow::RecordBatch>& batch);

    arrow::Result<std::shared_ptr<arrow::Array>> EvaluateLang(
        const std::shared_ptr<Expression>& arg,
        const std::shared_ptr<arrow::RecordBatch>& batch);

    arrow::Result<std::shared_ptr<arrow::Array>> EvaluateDatatype(
        const std::shared_ptr<Expression>& arg,
        const std::shared_ptr<arrow::RecordBatch>& batch);

    arrow::Result<std::shared_ptr<arrow::BooleanArray>> EvaluateRegex(
        const std::shared_ptr<Expression>& text_arg,
        const std::shared_ptr<Expression>& pattern_arg,
        const std::shared_ptr<arrow::RecordBatch>& batch);

    // Helper methods
    arrow::Result<std::shared_ptr<arrow::Array>> GetColumnForVariable(
        const Variable& var,
        const std::shared_ptr<arrow::RecordBatch>& batch);

    arrow::Result<std::shared_ptr<arrow::Array>> CreateConstantArray(
        const RDFTerm& term,
        int64_t length);

    arrow::Result<ValueId> TermToValueId(const RDFTerm& term);

    EvaluationContext ctx_;
};

// Factory function to create a predicate function from a SPARQL expression
// This is what QueryPlanner uses to create FilterOperator instances
std::function<arrow::Result<std::shared_ptr<arrow::BooleanArray>>(
    const std::shared_ptr<arrow::RecordBatch>&)>
CreatePredicateFunction(
    const std::shared_ptr<Expression>& expr,
    const EvaluationContext& ctx);

// Helper to get a human-readable description of an expression
// Used for FilterOperator's predicate_description
std::string GetExpressionDescription(const std::shared_ptr<Expression>& expr);

} // namespace sparql
} // namespace sabot_ql
