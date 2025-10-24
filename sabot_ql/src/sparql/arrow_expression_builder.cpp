#include <sabot_ql/sparql/arrow_expression_builder.h>
#include <sabot_ql/sparql/arrow_function_registry.h>
#include <arrow/compute/api.h>

namespace sabot_ql {
namespace sparql {

namespace compute = arrow::compute;

arrow::Result<compute::Expression> ArrowExpressionBuilder::Build(
    const Expression& sparql_expr,
    const std::unordered_map<std::string, std::string>& var_to_field) {

    ArrowExpressionBuilder builder(var_to_field);
    return builder.BuildExpression(sparql_expr);
}

arrow::Result<compute::Expression> ArrowExpressionBuilder::BuildExpression(
    const Expression& expr) {

    // Check if it's a constant (variable, literal, or IRI)
    if (expr.IsConstant()) {
        if (std::holds_alternative<Variable>(*expr.constant)) {
            return BuildVariable(std::get<Variable>(*expr.constant));
        }
        if (std::holds_alternative<Literal>(*expr.constant)) {
            return BuildLiteral(std::get<Literal>(*expr.constant));
        }
        if (std::holds_alternative<IRI>(*expr.constant)) {
            return BuildIRI(std::get<IRI>(*expr.constant));
        }
        return arrow::Status::Invalid("Unknown constant type");
    }

    // It's an operator expression
    return BuildFunctionCall(expr.op, expr.arguments);
}

arrow::Result<compute::Expression> ArrowExpressionBuilder::BuildVariable(
    const Variable& var) {

    // Look up field name for this variable
    auto it = var_to_field_.find(var.name);
    if (it == var_to_field_.end()) {
        return arrow::Status::Invalid("Variable not found in schema: " + var.name);
    }

    // Create field reference
    return compute::field_ref(it->second);
}

arrow::Result<compute::Expression> ArrowExpressionBuilder::BuildLiteral(
    const Literal& lit) {

    // Convert SPARQL literal to Arrow literal
    // TODO: Handle datatypes properly
    // For now, try to parse as int64, double, or string

    // Try integer
    try {
        int64_t int_val = std::stoll(lit.value);
        return compute::literal(int_val);
    } catch (...) {}

    // Try double
    try {
        double double_val = std::stod(lit.value);
        return compute::literal(double_val);
    } catch (...) {}

    // Default to string
    return compute::literal(lit.value);
}

arrow::Result<compute::Expression> ArrowExpressionBuilder::BuildIRI(
    const IRI& iri) {

    // IRIs become string literals in expressions
    return compute::literal(iri.iri);
}

arrow::Result<compute::Expression> ArrowExpressionBuilder::BuildFunctionCall(
    ExprOperator op,
    const std::vector<std::shared_ptr<Expression>>& args) {

    // Get Arrow function name for this operator
    std::string func_name = OperatorToFunctionName(op);
    if (func_name.empty()) {
        return arrow::Status::NotImplemented(
            "Operator not yet supported: " + std::to_string(static_cast<int>(op)));
    }

    // Build argument expressions
    std::vector<compute::Expression> arrow_args;
    arrow_args.reserve(args.size());

    for (const auto& arg : args) {
        ARROW_ASSIGN_OR_RAISE(auto arrow_arg, BuildExpression(*arg));
        arrow_args.push_back(std::move(arrow_arg));
    }

    // Create function call expression
    return compute::call(func_name, std::move(arrow_args));
}

std::string ArrowExpressionBuilder::OperatorToFunctionName(ExprOperator op) const {
    switch (op) {
        // Comparison operators
        case ExprOperator::Equal:           return "equal";
        case ExprOperator::NotEqual:        return "not_equal";
        case ExprOperator::LessThan:        return "less";
        case ExprOperator::LessThanEqual:   return "less_equal";
        case ExprOperator::GreaterThan:     return "greater";
        case ExprOperator::GreaterThanEqual: return "greater_equal";

        // Logical operators
        case ExprOperator::And:             return "and";
        case ExprOperator::Or:              return "or";
        case ExprOperator::Not:             return "invert";  // Logical NOT

        // Arithmetic operators
        case ExprOperator::Plus:            return "add";
        case ExprOperator::Minus:           return "subtract";
        case ExprOperator::Multiply:        return "multiply";
        case ExprOperator::Divide:          return "divide";

        // String functions (via registry)
        case ExprOperator::Regex:           return "match_substring_regex";
        case ExprOperator::Str:             return "cast_string";  // Custom handling needed

        // Aggregate functions (not used in expressions)
        case ExprOperator::Count:
        case ExprOperator::Sum:
        case ExprOperator::Min:
        case ExprOperator::Max:
        case ExprOperator::Avg:
        case ExprOperator::GroupConcat:
            return "";  // These are handled separately by aggregate operators

        default:
            return "";  // Unsupported
    }
}

} // namespace sparql
} // namespace sabot_ql
