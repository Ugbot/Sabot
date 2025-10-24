#pragma once

#include <sabot_ql/sparql/ast.h>
#include <arrow/compute/expression.h>
#include <arrow/result.h>
#include <memory>

namespace sabot_ql {
namespace sparql {

/**
 * Arrow Expression Builder
 *
 * Converts SPARQL expressions (from AST) to Arrow compute expressions.
 * This enables direct use of Arrow's optimized expression evaluation engine.
 *
 * Features:
 * - Automatic function name mapping (via ArrowFunctionRegistry)
 * - Type inference and validation
 * - Nested expression support
 * - Variable to field reference mapping
 *
 * Example:
 *   SPARQL: FILTER(?age > 25 && UCASE(?name) = "ALICE")
 *   Arrow:  and(greater(field_ref("age"), literal(25)),
 *               equal(call("utf8_upper", field_ref("name")), literal("ALICE")))
 */
class ArrowExpressionBuilder {
public:
    /**
     * Build Arrow expression from SPARQL expression AST
     *
     * @param sparql_expr SPARQL expression from parser
     * @param var_to_field Mapping from SPARQL variables to Arrow field names
     * @return Arrow Expression tree
     */
    static arrow::Result<arrow::compute::Expression> Build(
        const Expression& sparql_expr,
        const std::unordered_map<std::string, std::string>& var_to_field);

private:
    ArrowExpressionBuilder(const std::unordered_map<std::string, std::string>& var_to_field)
        : var_to_field_(var_to_field) {}

    arrow::Result<arrow::compute::Expression> BuildExpression(const Expression& expr);

    // Build specific expression types
    arrow::Result<arrow::compute::Expression> BuildVariable(const Variable& var);
    arrow::Result<arrow::compute::Expression> BuildLiteral(const Literal& lit);
    arrow::Result<arrow::compute::Expression> BuildIRI(const IRI& iri);
    arrow::Result<arrow::compute::Expression> BuildFunctionCall(
        ExprOperator op,
        const std::vector<std::shared_ptr<Expression>>& args);

    // Convert SPARQL operator to Arrow function name
    std::string OperatorToFunctionName(ExprOperator op) const;

    const std::unordered_map<std::string, std::string>& var_to_field_;
};

} // namespace sparql
} // namespace sabot_ql
