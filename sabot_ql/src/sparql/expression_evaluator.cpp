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

        case ExprOperator::Lang:
            return EvaluateLang(expr->arguments[0], batch);

        case ExprOperator::Datatype:
            return EvaluateDatatype(expr->arguments[0], batch);

        case ExprOperator::Regex: {
            if (expr->arguments.size() != 2) {
                return arrow::Status::Invalid("REGEX requires 2 arguments (text, pattern)");
            }
            return EvaluateRegex(expr->arguments[0], expr->arguments[1], batch);
        }

        default:
            return arrow::Status::Invalid("Unknown expression operator");
    }
}

// Comparison operators
arrow::Result<std::shared_ptr<arrow::BooleanArray>> ExpressionEvaluator::EvaluateComparison(
    ExprOperator op,
    const std::shared_ptr<arrow::Array>& left,
    const std::shared_ptr<arrow::Array>& right) {

    // Arrow 22.0 doesn't have a generic Compare() function
    // Use specific comparison functions for each operator
    arrow::Datum result;

    switch (op) {
        case ExprOperator::Equal: {
            ARROW_ASSIGN_OR_RAISE(result, cp::CallFunction("equal", {left, right}));
            break;
        }
        case ExprOperator::NotEqual: {
            ARROW_ASSIGN_OR_RAISE(result, cp::CallFunction("not_equal", {left, right}));
            break;
        }
        case ExprOperator::LessThan: {
            ARROW_ASSIGN_OR_RAISE(result, cp::CallFunction("less", {left, right}));
            break;
        }
        case ExprOperator::LessThanEqual: {
            ARROW_ASSIGN_OR_RAISE(result, cp::CallFunction("less_equal", {left, right}));
            break;
        }
        case ExprOperator::GreaterThan: {
            ARROW_ASSIGN_OR_RAISE(result, cp::CallFunction("greater", {left, right}));
            break;
        }
        case ExprOperator::GreaterThanEqual: {
            ARROW_ASSIGN_OR_RAISE(result, cp::CallFunction("greater_equal", {left, right}));
            break;
        }
        default:
            return arrow::Status::Invalid("Not a comparison operator");
    }

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
            ValueId vid = ValueId::fromBits(value_ids->Value(i));

            // IRIs are stored in vocabulary with VocabIndex datatype
            // We need to look up the term to check its kind
            bool is_iri = false;
            if (vid.getDatatype() == Datatype::VocabIndex) {
                auto term_result = ctx_.vocab->GetTerm(vid);
                if (term_result.ok()) {
                    is_iri = (term_result.ValueOrDie().kind == TermKind::IRI);
                }
            }

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
            ValueId vid = ValueId::fromBits(value_ids->Value(i));

            // Literals are stored in vocabulary with VocabIndex datatype
            // We need to look up the term to check its kind
            bool is_literal = false;
            if (vid.getDatatype() == Datatype::VocabIndex) {
                auto term_result = ctx_.vocab->GetTerm(vid);
                if (term_result.ok()) {
                    is_literal = (term_result.ValueOrDie().kind == TermKind::Literal);
                }
            }

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
            ValueId vid = ValueId::fromBits(value_ids->Value(i));

            // Blank nodes use BlankNodeIndex datatype
            bool is_blank = (vid.getDatatype() == Datatype::BlankNodeIndex);
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
            ValueId vid = ValueId::fromBits(value_ids->Value(i));

            // Lookup term in vocabulary
            auto term_result = ctx_.vocab->GetTerm(vid);
            if (!term_result.ok()) {
                return arrow::Status::Invalid("Failed to lookup term in vocabulary");
            }

            auto term = term_result.ValueOrDie();

            // All term types store their string value in the lexical field
            ARROW_RETURN_NOT_OK(builder.Append(term.lexical));
        }
    }

    std::shared_ptr<arrow::Array> result;
    ARROW_RETURN_NOT_OK(builder.Finish(&result));
    return result;
}

// LANG(?var) - Extract language tag from literal
arrow::Result<std::shared_ptr<arrow::Array>> ExpressionEvaluator::EvaluateLang(
    const std::shared_ptr<Expression>& arg,
    const std::shared_ptr<arrow::RecordBatch>& batch) {

    ARROW_ASSIGN_OR_RAISE(auto column, EvaluateNode(arg, batch));

    if (column->type()->id() != arrow::Type::UINT64) {
        return arrow::Status::Invalid("LANG() requires ValueId column (uint64)");
    }

    auto value_ids = std::static_pointer_cast<arrow::UInt64Array>(column);

    // Extract language tags from literals
    arrow::StringBuilder builder;
    ARROW_RETURN_NOT_OK(builder.Reserve(value_ids->length()));

    for (int64_t i = 0; i < value_ids->length(); ++i) {
        if (value_ids->IsNull(i)) {
            ARROW_RETURN_NOT_OK(builder.AppendNull());
        } else {
            ValueId vid = ValueId::fromBits(value_ids->Value(i));

            // Lookup term in vocabulary
            auto term_result = ctx_.vocab->GetTerm(vid);
            if (!term_result.ok()) {
                return arrow::Status::Invalid("Failed to lookup term in vocabulary");
            }

            auto term = term_result.ValueOrDie();

            // LANG only applies to literals
            if (term.kind == TermKind::Literal) {
                ARROW_RETURN_NOT_OK(builder.Append(term.language));
            } else {
                ARROW_RETURN_NOT_OK(builder.Append(""));  // Empty string for non-literals
            }
        }
    }

    std::shared_ptr<arrow::Array> result;
    ARROW_RETURN_NOT_OK(builder.Finish(&result));
    return result;
}

// DATATYPE(?var) - Extract datatype IRI from literal
arrow::Result<std::shared_ptr<arrow::Array>> ExpressionEvaluator::EvaluateDatatype(
    const std::shared_ptr<Expression>& arg,
    const std::shared_ptr<arrow::RecordBatch>& batch) {

    ARROW_ASSIGN_OR_RAISE(auto column, EvaluateNode(arg, batch));

    if (column->type()->id() != arrow::Type::UINT64) {
        return arrow::Status::Invalid("DATATYPE() requires ValueId column (uint64)");
    }

    auto value_ids = std::static_pointer_cast<arrow::UInt64Array>(column);

    // Extract datatype IRIs from literals
    arrow::StringBuilder builder;
    ARROW_RETURN_NOT_OK(builder.Reserve(value_ids->length()));

    for (int64_t i = 0; i < value_ids->length(); ++i) {
        if (value_ids->IsNull(i)) {
            ARROW_RETURN_NOT_OK(builder.AppendNull());
        } else {
            ValueId vid = ValueId::fromBits(value_ids->Value(i));

            // Lookup term in vocabulary
            auto term_result = ctx_.vocab->GetTerm(vid);
            if (!term_result.ok()) {
                return arrow::Status::Invalid("Failed to lookup term in vocabulary");
            }

            auto term = term_result.ValueOrDie();

            // DATATYPE only applies to literals
            if (term.kind == TermKind::Literal) {
                // If no explicit datatype, use xsd:string as default
                std::string datatype = term.datatype.empty()
                    ? "http://www.w3.org/2001/XMLSchema#string"
                    : term.datatype;

                ARROW_RETURN_NOT_OK(builder.Append(datatype));
            } else {
                ARROW_RETURN_NOT_OK(builder.Append(""));  // Empty string for non-literals
            }
        }
    }

    std::shared_ptr<arrow::Array> result;
    ARROW_RETURN_NOT_OK(builder.Finish(&result));
    return result;
}

// REGEX(?text, pattern) - Regular expression matching
arrow::Result<std::shared_ptr<arrow::BooleanArray>> ExpressionEvaluator::EvaluateRegex(
    const std::shared_ptr<Expression>& text_arg,
    const std::shared_ptr<Expression>& pattern_arg,
    const std::shared_ptr<arrow::RecordBatch>& batch) {

    // Evaluate text argument (typically a variable)
    ARROW_ASSIGN_OR_RAISE(auto text_column, EvaluateNode(text_arg, batch));

    // Pattern must be a constant string literal
    if (!pattern_arg->IsConstant()) {
        return arrow::Status::Invalid("REGEX pattern must be a constant");
    }

    // Extract pattern string
    const auto* lit = std::get_if<Literal>(&pattern_arg->constant.value());
    if (!lit) {
        return arrow::Status::Invalid("REGEX pattern must be a string literal");
    }
    std::string pattern = lit->value;

    // Convert ValueIds to strings for regex matching
    if (text_column->type()->id() != arrow::Type::UINT64) {
        return arrow::Status::Invalid("REGEX text argument must be ValueId column (uint64)");
    }

    auto value_ids = std::static_pointer_cast<arrow::UInt64Array>(text_column);

    // Build string array from ValueIds
    arrow::StringBuilder string_builder;
    ARROW_RETURN_NOT_OK(string_builder.Reserve(value_ids->length()));

    for (int64_t i = 0; i < value_ids->length(); ++i) {
        if (value_ids->IsNull(i)) {
            ARROW_RETURN_NOT_OK(string_builder.AppendNull());
        } else {
            ValueId vid = ValueId::fromBits(value_ids->Value(i));

            // Lookup term in vocabulary
            auto term_result = ctx_.vocab->GetTerm(vid);
            if (!term_result.ok()) {
                return arrow::Status::Invalid("Failed to lookup term in vocabulary");
            }

            auto term = term_result.ValueOrDie();

            // All term types store their string value in the lexical field
            ARROW_RETURN_NOT_OK(string_builder.Append(term.lexical));
        }
    }

    std::shared_ptr<arrow::Array> string_array;
    ARROW_RETURN_NOT_OK(string_builder.Finish(&string_array));

    // Apply regex matching using Arrow compute
    cp::MatchSubstringOptions options(pattern);
    options.ignore_case = false;  // Case-sensitive by default

    ARROW_ASSIGN_OR_RAISE(auto match_result,
                          cp::CallFunction("match_substring", {string_array}, &options));
    return std::static_pointer_cast<arrow::BooleanArray>(match_result.make_array());
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

    uint64_t bits = value_id.getBits();
    for (int64_t i = 0; i < length; ++i) {
        ARROW_RETURN_NOT_OK(builder.Append(bits));
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
