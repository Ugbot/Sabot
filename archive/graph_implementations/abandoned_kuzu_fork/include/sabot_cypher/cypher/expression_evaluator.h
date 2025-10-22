#pragma once

#include <arrow/api.h>
#include <arrow/compute/api.h>
#include <memory>
#include <string>
#include <map>

namespace sabot_cypher {
namespace cypher {

// Expression types for filter evaluation
enum class ExprType {
    LITERAL,
    COLUMN,
    BINARY_OP,
    UNARY_OP,
    FUNCTION_CALL,
};

// Expression node for building filter predicates
struct Expression {
    ExprType type;
    std::string value;  // For LITERAL or COLUMN name
    std::string op;     // For operators: "=", "<", ">", "AND", "OR", etc.
    std::shared_ptr<Expression> left;
    std::shared_ptr<Expression> right;
    std::vector<std::shared_ptr<Expression>> args;  // For function calls
    
    Expression(ExprType t) : type(t) {}
    
    // Factory methods
    static std::shared_ptr<Expression> Literal(const std::string& val);
    static std::shared_ptr<Expression> Column(const std::string& col);
    static std::shared_ptr<Expression> BinaryOp(const std::string& op,
                                                std::shared_ptr<Expression> l,
                                                std::shared_ptr<Expression> r);
    static std::shared_ptr<Expression> Function(const std::string& name,
                                               std::vector<std::shared_ptr<Expression>> args);
};

// Evaluates filter expressions to Arrow boolean arrays
class ExpressionEvaluator {
public:
    static arrow::Result<std::shared_ptr<ExpressionEvaluator>> Create();
    
    // Evaluate expression on table, return boolean mask
    arrow::Result<std::shared_ptr<arrow::BooleanArray>> Evaluate(
        const Expression& expr,
        std::shared_ptr<arrow::Table> table);
    
private:
    ExpressionEvaluator() = default;
    
    // Recursive evaluation
    arrow::Result<arrow::Datum> EvaluateNode(
        const Expression& expr,
        std::shared_ptr<arrow::Table> table);
    
    // Specific evaluators
    arrow::Result<arrow::Datum> EvaluateLiteral(const std::string& value);
    arrow::Result<arrow::Datum> EvaluateColumn(const std::string& col_name,
                                               std::shared_ptr<arrow::Table> table);
    arrow::Result<arrow::Datum> EvaluateBinaryOp(const std::string& op,
                                                  arrow::Datum left,
                                                  arrow::Datum right);
};

// Parse simple predicate strings
// Examples: "age > 25", "name = 'Alice'", "age >= 18 AND age <= 65"
arrow::Result<std::shared_ptr<Expression>> ParsePredicate(const std::string& predicate_str);

}}  // namespace sabot_cypher::cypher

