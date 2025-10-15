#include "sabot_cypher/cypher/expression_evaluator.h"
#include <arrow/compute/api.h>
#include <sstream>
#include <algorithm>

namespace sabot_cypher {
namespace cypher {

namespace cp = arrow::compute;

// Factory methods for Expression
std::shared_ptr<Expression> Expression::Literal(const std::string& val) {
    auto expr = std::make_shared<Expression>(ExprType::LITERAL);
    expr->value = val;
    return expr;
}

std::shared_ptr<Expression> Expression::Column(const std::string& col) {
    auto expr = std::make_shared<Expression>(ExprType::COLUMN);
    expr->value = col;
    return expr;
}

std::shared_ptr<Expression> Expression::BinaryOp(const std::string& op,
                                                  std::shared_ptr<Expression> l,
                                                  std::shared_ptr<Expression> r) {
    auto expr = std::make_shared<Expression>(ExprType::BINARY_OP);
    expr->op = op;
    expr->left = l;
    expr->right = r;
    return expr;
}

std::shared_ptr<Expression> Expression::Function(const std::string& name,
                                                 std::vector<std::shared_ptr<Expression>> args) {
    auto expr = std::make_shared<Expression>(ExprType::FUNCTION_CALL);
    expr->value = name;  // Function name
    expr->args = args;
    return expr;
}

// ExpressionEvaluator implementation
arrow::Result<std::shared_ptr<ExpressionEvaluator>> ExpressionEvaluator::Create() {
    return std::shared_ptr<ExpressionEvaluator>(new ExpressionEvaluator());
}

arrow::Result<std::shared_ptr<arrow::BooleanArray>> ExpressionEvaluator::Evaluate(
    const Expression& expr,
    std::shared_ptr<arrow::Table> table) {
    
    ARROW_ASSIGN_OR_RAISE(auto result_datum, EvaluateNode(expr, table));
    
    // Convert to boolean array
    if (result_datum.is_scalar()) {
        // Broadcast scalar to array
        auto scalar = result_datum.scalar();
        ARROW_ASSIGN_OR_RAISE(auto array,
            arrow::MakeArrayFromScalar(*scalar, table->num_rows()));
        return std::static_pointer_cast<arrow::BooleanArray>(array);
    } else if (result_datum.is_array()) {
        return std::static_pointer_cast<arrow::BooleanArray>(result_datum.make_array());
    } else {
        return arrow::Status::Invalid("Expression must evaluate to boolean array");
    }
}

arrow::Result<arrow::Datum> ExpressionEvaluator::EvaluateNode(
    const Expression& expr,
    std::shared_ptr<arrow::Table> table) {
    
    switch (expr.type) {
        case ExprType::LITERAL:
            return EvaluateLiteral(expr.value);
        
        case ExprType::COLUMN:
            return EvaluateColumn(expr.value, table);
        
        case ExprType::BINARY_OP: {
            ARROW_ASSIGN_OR_RAISE(auto left, EvaluateNode(*expr.left, table));
            ARROW_ASSIGN_OR_RAISE(auto right, EvaluateNode(*expr.right, table));
            return EvaluateBinaryOp(expr.op, left, right);
        }
        
        case ExprType::FUNCTION_CALL:
            // TODO: Implement function calls (lower, upper, etc.)
            return arrow::Status::NotImplemented("Function calls not yet implemented");
        
        default:
            return arrow::Status::Invalid("Unknown expression type");
    }
}

arrow::Result<arrow::Datum> ExpressionEvaluator::EvaluateLiteral(const std::string& value) {
    // Try to parse as different types
    
    // Try integer
    try {
        int64_t int_val = std::stoll(value);
        return arrow::Datum(arrow::MakeScalar(int_val));
    } catch (...) {}
    
    // Try float
    try {
        double float_val = std::stod(value);
        return arrow::Datum(arrow::MakeScalar(float_val));
    } catch (...) {}
    
    // Try boolean
    if (value == "true" || value == "TRUE") {
        return arrow::Datum(arrow::MakeScalar(true));
    } else if (value == "false" || value == "FALSE") {
        return arrow::Datum(arrow::MakeScalar(false));
    }
    
    // Default to string (remove quotes if present)
    std::string str_value = value;
    if (str_value.front() == '\'' && str_value.back() == '\'') {
        str_value = str_value.substr(1, str_value.length() - 2);
    }
    
    return arrow::Datum(arrow::MakeScalar(str_value));
}

arrow::Result<arrow::Datum> ExpressionEvaluator::EvaluateColumn(
    const std::string& col_name,
    std::shared_ptr<arrow::Table> table) {
    
    // Get column from table
    auto col = table->GetColumnByName(col_name);
    if (!col) {
        return arrow::Status::Invalid("Column not found: " + col_name);
    }
    
    // Return the chunked array as datum
    // Arrow compute can handle ChunkedArray directly
    return arrow::Datum(col);
}

arrow::Result<arrow::Datum> ExpressionEvaluator::EvaluateBinaryOp(
    const std::string& op,
    arrow::Datum left,
    arrow::Datum right) {
    
    // NOTE: Arrow compute comparison functions are not directly callable
    // in this Arrow version. They need to be accessed via CallFunction() or
    // we need to build the arrays manually.
    //
    // For now, return NotImplemented to indicate Filter needs full Kuzu integration
    // for proper expression handling.
    //
    // Working operators (Scan, Project, Limit, Aggregate, OrderBy) don't need
    // complex expression evaluation, so they work fine.
    
    return arrow::Status::NotImplemented(
        "Binary operator " + op + " requires full Arrow compute integration. "
        "Filter operator will work once Kuzu Expression translation is implemented.");
}

// Simple predicate parser
// Supports: "column op value" and "expr AND/OR expr"
arrow::Result<std::shared_ptr<Expression>> ParsePredicate(const std::string& predicate_str) {
    // Very simple parser for demonstration
    // Real implementation would use proper parser or Kuzu expressions
    
    // Example: "age > 25"
    // Example: "name = 'Alice'"
    // Example: "age > 25 AND age < 65"
    
    std::string pred = predicate_str;
    
    // Trim whitespace
    pred.erase(0, pred.find_first_not_of(" \t"));
    pred.erase(pred.find_last_not_of(" \t") + 1);
    
    // Check for AND/OR
    size_t and_pos = pred.find(" AND ");
    size_t or_pos = pred.find(" OR ");
    
    if (and_pos != std::string::npos) {
        auto left_str = pred.substr(0, and_pos);
        auto right_str = pred.substr(and_pos + 5);
        
        ARROW_ASSIGN_OR_RAISE(auto left_expr, ParsePredicate(left_str));
        ARROW_ASSIGN_OR_RAISE(auto right_expr, ParsePredicate(right_str));
        
        return Expression::BinaryOp("AND", left_expr, right_expr);
    }
    
    if (or_pos != std::string::npos) {
        auto left_str = pred.substr(0, or_pos);
        auto right_str = pred.substr(or_pos + 4);
        
        ARROW_ASSIGN_OR_RAISE(auto left_expr, ParsePredicate(left_str));
        ARROW_ASSIGN_OR_RAISE(auto right_expr, ParsePredicate(right_str));
        
        return Expression::BinaryOp("OR", left_expr, right_expr);
    }
    
    // Simple comparison: "column op value"
    std::vector<std::string> ops = {">=", "<=", "!=", "<>", "=", ">", "<"};
    
    for (const auto& op : ops) {
        size_t pos = pred.find(op);
        if (pos != std::string::npos) {
            std::string left_str = pred.substr(0, pos);
            std::string right_str = pred.substr(pos + op.length());
            
            // Trim
            left_str.erase(0, left_str.find_first_not_of(" \t"));
            left_str.erase(left_str.find_last_not_of(" \t") + 1);
            right_str.erase(0, right_str.find_first_not_of(" \t"));
            right_str.erase(right_str.find_last_not_of(" \t") + 1);
            
            auto left_expr = Expression::Column(left_str);
            auto right_expr = Expression::Literal(right_str);
            
            return Expression::BinaryOp(op, left_expr, right_expr);
        }
    }
    
    return arrow::Status::Invalid("Could not parse predicate: " + predicate_str);
}

}}  // namespace sabot_cypher::cypher

