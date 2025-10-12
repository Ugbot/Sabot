#include <sabot_ql/sparql/expression_evaluator.h>
#include <arrow/compute/api.h>
#include <sstream>

namespace sabot_ql {
namespace sparql {

namespace cp = arrow::compute;

// Main evaluation entry point
arrow::Result<std::shared_ptr<arrow::BooleanArray>> ExpressionEvaluator::Evaluate(
    const std::shared_ptr<Expression>& expr,
    const std::shared_ptr<arrow::RecordBatch>& batch) {

    // Evaluate the expression tree
    ARROW_ASSIGN_OR_RAISE(auto result, EvaluateNode(expr, batch));

    // Result must be a BooleanArray
    if (result->type()->id() != arrow::Type::BOOL) {
        return arrow::Status::Invalid("Expression did not evaluate to boolean");
    }

    return std::static_pointer_cast<arrow::BooleanArray>(result);
}

// Recursive expression evaluation
arrow::Result<std::shared_ptr<arrow::Array>> ExpressionEvaluator::EvaluateNode(
    const std::shared_ptr<Expression>& expr,
    const std::shared_ptr<arrow::RecordBatch>& batch) {

    // Leaf node: constant value
    if (expr->IsConstant()) {
        return CreateConstantArray(expr->constant.value(), batch->num_rows());
    }

    // Branch node: operator with arguments
    switch (expr->op) {
        // Comparison operators
        case ExprOperator::Equal:
        case ExprOperator::NotEqual:
        case ExprOperator::LessThan:
        case ExprOperator::LessThanEqual:
        case ExprOperator::GreaterThan:
        case ExprOperator::GreaterThanEqual: {
            if (expr->arguments.size() != 2) {
                return arrow::Status::Invalid("Comparison operator requires 2 arguments");
            }
            ARROW_ASSIGN_OR_RAISE(auto left, EvaluateNode(expr->arguments[0], batch));
            ARROW_ASSIGN_OR_RAISE(auto right, EvaluateNode(expr->arguments[1], batch));
            return EvaluateComparison(expr->op, left, right);
        }

        // Logical operators
        case ExprOperator::And:
        case ExprOperator::Or:
        case ExprOperator::Not:
            return EvaluateLogical(expr->op, expr->arguments, batch);

        // Arithmetic operators
        case ExprOperator::Plus:
        case ExprOperator::Minus:
        case ExprOperator::Multiply:
        case ExprOperator::Divide: {
            if (expr->arguments.size() != 2) {
                return arrow::Status::Invalid("Arithmetic operator requires 2 arguments");
            }
            ARROW_ASSIGN_OR_RAISE(auto left, EvaluateNode(expr->arguments[0], batch));
            ARROW_ASSIGN_OR_RAISE(auto right, EvaluateNode(expr->arguments[1], batch));
            return EvaluateArithmetic(expr->op, left, right);
        }

        // Built-in functions
        case ExprOperator::Bound:
            return EvaluateBound(expr->arguments[0], batch);

        case ExprOperator::IsIRI:
            return EvaluateIsIRI(expr->arguments[0], batch);

        case ExprOperator::IsLiteral:
            return EvaluateIsLiteral(expr->arguments[0], batch);

        case ExprOperator::IsBlank:
            return EvaluateIsBlank(expr->arguments[0], batch);

        case ExprOperator::Str:
            return EvaluateStr(expr->arguments[0], batch);

        // Not yet implemented
        case ExprOperator::Lang:
        case ExprOperator::Datatype:
        case ExprOperator::Regex:
            return arrow::Status::NotImplemented("Expression operator not yet implemented");

        default:
            return arrow::Status::Invalid("Unknown expression operator");
    }
}

// Comparison operators
arrow::Result<std::shared_ptr<arrow::BooleanArray>> ExpressionEvaluator::EvaluateComparison(
    ExprOperator op,
    const std::shared_ptr<arrow::Array>& left,
    const std::shared_ptr<arrow::Array>& right) {

    // Map SPARQL operators to Arrow compute functions
    cp::CompareOperator arrow_op;
    switch (op) {
        case ExprOperator::Equal:
            arrow_op = cp::CompareOperator::EQUAL;
            break;
        case ExprOperator::NotEqual:
            arrow_op = cp::CompareOperator::NOT_EQUAL;
            break;
        case ExprOperator::LessThan:
            arrow_op = cp::CompareOperator::LESS;
            break;
        case ExprOperator::LessThanEqual:
            arrow_op = cp::CompareOperator::LESS_EQUAL;
            break;
        case ExprOperator::GreaterThan:
            arrow_op = cp::CompareOperator::GREATER;
            break;
        case ExprOperator::GreaterThanEqual:
            arrow_op = cp::CompareOperator::GREATER_EQUAL;
            break;
        default:
            return arrow::Status::Invalid("Not a comparison operator");
    }

    // Use Arrow compute kernel
    ARROW_ASSIGN_OR_RAISE(auto result, cp::Compare(left, right, arrow_op));
    return std::static_pointer_cast<arrow::BooleanArray>(result.make_array());
}

// Logical operators
arrow::Result<std::shared_ptr<arrow::BooleanArray>> ExpressionEvaluator::EvaluateLogical(
    ExprOperator op,
    const std::vector<std::shared_ptr<Expression>>& args,
    const std::shared_ptr<arrow::RecordBatch>& batch) {

    switch (op) {
        case ExprOperator::And: {
            if (args.size() != 2) {
                return arrow::Status::Invalid("AND requires 2 arguments");
            }

            ARROW_ASSIGN_OR_RAISE(auto left, Evaluate(args[0], batch));
            ARROW_ASSIGN_OR_RAISE(auto right, Evaluate(args[1], batch));

            ARROW_ASSIGN_OR_RAISE(auto result, cp::And(left, right));
            return std::static_pointer_cast<arrow::BooleanArray>(result.make_array());
        }

        case ExprOperator::Or: {
            if (args.size() != 2) {
                return arrow::Status::Invalid("OR requires 2 arguments");
            }

            ARROW_ASSIGN_OR_RAISE(auto left, Evaluate(args[0], batch));
            ARROW_ASSIGN_OR_RAISE(auto right, Evaluate(args[1], batch));

            ARROW_ASSIGN_OR_RAISE(auto result, cp::Or(left, right));
            return std::static_pointer_cast<arrow::BooleanArray>(result.make_array());
        }

        case ExprOperator::Not: {
            if (args.size() != 1) {
                return arrow::Status::Invalid("NOT requires 1 argument");
            }

            ARROW_ASSIGN_OR_RAISE(auto arg, Evaluate(args[0], batch));

            ARROW_ASSIGN_OR_RAISE(auto result, cp::Invert(arg));
            return std::static_pointer_cast<arrow::BooleanArray>(result.make_array());
        }

        default:
            return arrow::Status::Invalid("Not a logical operator");
    }
}

// Arithmetic operators
arrow::Result<std::shared_ptr<arrow::Array>> ExpressionEvaluator::EvaluateArithmetic(
    ExprOperator op,
    const std::shared_ptr<arrow::Array>& left,
    const std::shared_ptr<arrow::Array>& right) {

    switch (op) {
        case ExprOperator::Plus: {
            ARROW_ASSIGN_OR_RAISE(auto result, cp::Add(left, right));
            return result.make_array();
        }

        case ExprOperator::Minus: {
            ARROW_ASSIGN_OR_RAISE(auto result, cp::Subtract(left, right));
            return result.make_array();
        }

        case ExprOperator::Multiply: {
            ARROW_ASSIGN_OR_RAISE(auto result, cp::Multiply(left, right));
            return result.make_array();
        }

        case ExprOperator::Divide: {
            ARROW_ASSIGN_OR_RAISE(auto result, cp::Divide(left, right));
            return result.make_array();
        }

        default:
            return arrow::Status::Invalid("Not an arithmetic operator");
    }
}

// BOUND(?var) - Check if variable is bound (non-null)
arrow::Result<std::shared_ptr<arrow::BooleanArray>> ExpressionEvaluator::EvaluateBound(
    const std::shared_ptr<Expression>& arg,
    const std::shared_ptr<arrow::RecordBatch>& batch) {

    // Argument must be a variable
    if (!arg->IsConstant()) {
        return arrow::Status::Invalid("BOUND() requires a variable argument");
    }

    const auto* var = std::get_if<Variable>(&arg->constant.value());
    if (!var) {
        return arrow::Status::Invalid("BOUND() requires a variable argument");
    }

    // Get the column for this variable
    ARROW_ASSIGN_OR_RAISE(auto column, GetColumnForVariable(*var, batch));

    // Check for non-null values
    ARROW_ASSIGN_OR_RAISE(auto result, cp::IsValid(column));
    return std::static_pointer_cast<arrow::BooleanArray>(result.make_array());
}

// isIRI(?var) - Check if value is an IRI
arrow::Result<std::shared_ptr<arrow::BooleanArray>> ExpressionEvaluator::EvaluateIsIRI(
    const std::shared_ptr<Expression>& arg,
    const std::shared_ptr<arrow::RecordBatch>& batch) {

    // Get the column
    ARROW_ASSIGN_OR_RAISE(auto column, EvaluateNode(arg, batch));

    // ValueIds encode type information in the type bits
    // IRI type = 0b00 (bits 62-63)
    // We need to check the type bits of each ValueId

    if (column->type()->id() != arrow::Type::UINT64) {
        return arrow::Status::Invalid("isIRI() requires ValueId column (uint64)");
    }

    auto value_ids = std::static_pointer_cast<arrow::UInt64Array>(column);

    // Build boolean array by checking type bits
    arrow::BooleanBuilder builder;
    ARROW_RETURN_NOT_OK(builder.Reserve(value_ids->length()));

    for (int64_t i = 0; i < value_ids->length(); ++i) {
        if (value_ids->IsNull(i)) {
            ARROW_RETURN_NOT_OK(builder.AppendNull());
        } else {
            ValueId vid = value_ids->Value(i);
            bool is_iri = (vid.GetType() == ValueType::IRI);
            ARROW_RETURN_NOT_OK(builder.Append(is_iri));
        }
    }

    std::shared_ptr<arrow::BooleanArray> result;
    ARROW_RETURN_NOT_OK(builder.Finish(&result));
    return result;
}

// isLiteral(?var) - Check if value is a Literal
arrow::Result<std::shared_ptr<arrow::BooleanArray>> ExpressionEvaluator::EvaluateIsLiteral(
    const std::shared_ptr<Expression>& arg,
    const std::shared_ptr<arrow::RecordBatch>& batch) {

    ARROW_ASSIGN_OR_RAISE(auto column, EvaluateNode(arg, batch));

    if (column->type()->id() != arrow::Type::UINT64) {
        return arrow::Status::Invalid("isLiteral() requires ValueId column (uint64)");
    }

    auto value_ids = std::static_pointer_cast<arrow::UInt64Array>(column);

    arrow::BooleanBuilder builder;
    ARROW_RETURN_NOT_OK(builder.Reserve(value_ids->length()));

    for (int64_t i = 0; i < value_ids->length(); ++i) {
        if (value_ids->IsNull(i)) {
            ARROW_RETURN_NOT_OK(builder.AppendNull());
        } else {
            ValueId vid = value_ids->Value(i);
            bool is_literal = (vid.GetType() == ValueType::Literal);
            ARROW_RETURN_NOT_OK(builder.Append(is_literal));
        }
    }

    std::shared_ptr<arrow::BooleanArray> result;
    ARROW_RETURN_NOT_OK(builder.Finish(&result));
    return result;
}

// isBlank(?var) - Check if value is a Blank Node
arrow::Result<std::shared_ptr<arrow::BooleanArray>> ExpressionEvaluator::EvaluateIsBlank(
    const std::shared_ptr<Expression>& arg,
    const std::shared_ptr<arrow::RecordBatch>& batch) {

    ARROW_ASSIGN_OR_RAISE(auto column, EvaluateNode(arg, batch));

    if (column->type()->id() != arrow::Type::UINT64) {
        return arrow::Status::Invalid("isBlank() requires ValueId column (uint64)");
    }

    auto value_ids = std::static_pointer_cast<arrow::UInt64Array>(column);

    arrow::BooleanBuilder builder;
    ARROW_RETURN_NOT_OK(builder.Reserve(value_ids->length()));

    for (int64_t i = 0; i < value_ids->length(); ++i) {
        if (value_ids->IsNull(i)) {
            ARROW_RETURN_NOT_OK(builder.AppendNull());
        } else {
            ValueId vid = value_ids->Value(i);
            bool is_blank = (vid.GetType() == ValueType::BlankNode);
            ARROW_RETURN_NOT_OK(builder.Append(is_blank));
        }
    }

    std::shared_ptr<arrow::BooleanArray> result;
    ARROW_RETURN_NOT_OK(builder.Finish(&result));
    return result;
}

// STR(?var) - Convert to string representation
arrow::Result<std::shared_ptr<arrow::Array>> ExpressionEvaluator::EvaluateStr(
    const std::shared_ptr<Expression>& arg,
    const std::shared_ptr<arrow::RecordBatch>& batch) {

    ARROW_ASSIGN_OR_RAISE(auto column, EvaluateNode(arg, batch));

    if (column->type()->id() != arrow::Type::UINT64) {
        return arrow::Status::Invalid("STR() requires ValueId column (uint64)");
    }

    auto value_ids = std::static_pointer_cast<arrow::UInt64Array>(column);

    // Convert ValueIds to string representations
    arrow::StringBuilder builder;
    ARROW_RETURN_NOT_OK(builder.Reserve(value_ids->length()));

    for (int64_t i = 0; i < value_ids->length(); ++i) {
        if (value_ids->IsNull(i)) {
            ARROW_RETURN_NOT_OK(builder.AppendNull());
        } else {
            ValueId vid = value_ids->Value(i);

            // Lookup term in vocabulary
            auto term_result = ctx_.vocab->GetTerm(vid);
            if (!term_result.ok()) {
                return arrow::Status::Invalid("Failed to lookup term in vocabulary");
            }

            auto term = term_result.ValueOrDie();
            std::string str_value;

            // Extract string representation based on term type
            if (std::holds_alternative<Term::IRIType>(term.value)) {
                str_value = std::get<Term::IRIType>(term.value);
            } else if (std::holds_alternative<Term::LiteralType>(term.value)) {
                str_value = std::get<Term::LiteralType>(term.value).value;
            } else if (std::holds_alternative<Term::BlankNodeType>(term.value)) {
                str_value = std::get<Term::BlankNodeType>(term.value);
            } else {
                return arrow::Status::Invalid("Unknown term type");
            }

            ARROW_RETURN_NOT_OK(builder.Append(str_value));
        }
    }

    std::shared_ptr<arrow::Array> result;
    ARROW_RETURN_NOT_OK(builder.Finish(&result));
    return result;
}

// Helper: Get column for variable
arrow::Result<std::shared_ptr<arrow::Array>> ExpressionEvaluator::GetColumnForVariable(
    const Variable& var,
    const std::shared_ptr<arrow::RecordBatch>& batch) {

    // Look up column name from variable mapping
    auto it = ctx_.var_to_column.find(var.name);
    if (it == ctx_.var_to_column.end()) {
        return arrow::Status::Invalid("Variable not found in column mapping: " + var.name);
    }

    const std::string& column_name = it->second;

    // Get column from batch
    auto column = batch->GetColumnByName(column_name);
    if (!column) {
        return arrow::Status::Invalid("Column not found in batch: " + column_name);
    }

    return column;
}

// Helper: Create constant array
arrow::Result<std::shared_ptr<arrow::Array>> ExpressionEvaluator::CreateConstantArray(
    const RDFTerm& term,
    int64_t length) {

    // Convert term to ValueId
    ARROW_ASSIGN_OR_RAISE(auto value_id, TermToValueId(term));

    // Create constant array of ValueIds
    arrow::UInt64Builder builder;
    ARROW_RETURN_NOT_OK(builder.Reserve(length));

    for (int64_t i = 0; i < length; ++i) {
        ARROW_RETURN_NOT_OK(builder.Append(value_id.value));
    }

    std::shared_ptr<arrow::Array> result;
    ARROW_RETURN_NOT_OK(builder.Finish(&result));
    return result;
}

// Helper: Convert RDFTerm to ValueId
arrow::Result<ValueId> ExpressionEvaluator::TermToValueId(const RDFTerm& term) {
    // Convert SPARQL term to storage Term
    Term storage_term;

    if (auto* var = std::get_if<Variable>(&term)) {
        return arrow::Status::Invalid("Cannot convert variable to ValueId");
    } else if (auto* iri = std::get_if<IRI>(&term)) {
        storage_term = Term::IRI(iri->iri);
    } else if (auto* lit = std::get_if<Literal>(&term)) {
        storage_term = Term::Literal(lit->value, lit->language, lit->datatype);
    } else if (auto* blank = std::get_if<BlankNode>(&term)) {
        storage_term = Term::BlankNode(blank->id);
    } else {
        return arrow::Status::Invalid("Unknown term type");
    }

    // Add term to vocabulary and get ValueId
    return ctx_.vocab->AddTerm(storage_term);
}

// Factory function to create predicate function
std::function<arrow::Result<std::shared_ptr<arrow::BooleanArray>>(
    const std::shared_ptr<arrow::RecordBatch>&)>
CreatePredicateFunction(
    const std::shared_ptr<Expression>& expr,
    const EvaluationContext& ctx) {

    // Capture expr and ctx by value
    return [expr, ctx](const std::shared_ptr<arrow::RecordBatch>& batch)
        -> arrow::Result<std::shared_ptr<arrow::BooleanArray>> {

        ExpressionEvaluator evaluator(ctx);
        return evaluator.Evaluate(expr, batch);
    };
}

// Get expression description
std::string GetExpressionDescription(const std::shared_ptr<Expression>& expr) {
    if (!expr) {
        return "null";
    }

    return expr->ToString();
}

} // namespace sparql
} // namespace sabot_ql
