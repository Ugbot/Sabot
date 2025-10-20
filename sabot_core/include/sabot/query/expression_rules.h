#pragma once

#include "logical_plan.h"
#include <memory>
#include <string>
#include <unordered_map>

namespace sabot {
namespace query {

/**
 * @brief Base class for expression rewrite rules
 * 
 * Based on DuckDB's expression rewriter framework.
 * Rules simplify and optimize expressions at compile time.
 */
class ExpressionRule {
public:
    virtual ~ExpressionRule() = default;
    
    /**
     * @brief Apply rule to expression
     * 
     * @param expr Expression string
     * @return Optimized expression or original if no optimization
     */
    virtual std::string Rewrite(const std::string& expr) = 0;
    
    /**
     * @brief Get rule name
     */
    virtual std::string GetName() const = 0;
    
    /**
     * @brief Check if rule is applicable
     */
    virtual bool IsApplicable(const std::string& expr) const = 0;
};

/**
 * @brief Constant folding rule
 * 
 * Evaluates constant expressions at compile time.
 * Based on DuckDB's constant_folding.cpp
 * 
 * Examples:
 *   1 + 2 => 3
 *   'hello' || ' world' => 'hello world'
 *   2 * 3 + 4 => 10
 */
class ConstantFoldingRule : public ExpressionRule {
public:
    std::string Rewrite(const std::string& expr) override;
    std::string GetName() const override { return "ConstantFolding"; }
    bool IsApplicable(const std::string& expr) const override;
    
private:
    /**
     * @brief Check if expression contains only constants
     */
    bool IsConstantExpression(const std::string& expr) const;
    
    /**
     * @brief Evaluate constant expression
     */
    std::string EvaluateConstant(const std::string& expr);
};

/**
 * @brief Arithmetic simplification rule
 * 
 * Simplifies arithmetic expressions.
 * Based on DuckDB's arithmetic_simplification.cpp
 * 
 * Examples:
 *   x + 0 => x
 *   x * 1 => x
 *   x * 0 => 0
 *   x - x => 0
 */
class ArithmeticSimplificationRule : public ExpressionRule {
public:
    std::string Rewrite(const std::string& expr) override;
    std::string GetName() const override { return "ArithmeticSimplification"; }
    bool IsApplicable(const std::string& expr) const override;
};

/**
 * @brief Comparison simplification rule
 * 
 * Simplifies comparison expressions.
 * Based on DuckDB's comparison_simplification.cpp
 * 
 * Examples:
 *   x > x => FALSE
 *   x = x => TRUE (if x not nullable)
 *   x <> x => FALSE (if x not nullable)
 */
class ComparisonSimplificationRule : public ExpressionRule {
public:
    std::string Rewrite(const std::string& expr) override;
    std::string GetName() const override { return "ComparisonSimplification"; }
    bool IsApplicable(const std::string& expr) const override;
};

/**
 * @brief Conjunction simplification rule
 * 
 * Simplifies AND/OR expressions.
 * Based on DuckDB's conjunction_simplification.cpp
 * 
 * Examples:
 *   x AND TRUE => x
 *   x OR FALSE => x
 *   x AND FALSE => FALSE
 *   x OR TRUE => TRUE
 */
class ConjunctionSimplificationRule : public ExpressionRule {
public:
    std::string Rewrite(const std::string& expr) override;
    std::string GetName() const override { return "ConjunctionSimplification"; }
    bool IsApplicable(const std::string& expr) const override;
};

/**
 * @brief Expression rewriter
 * 
 * Applies multiple expression rules to optimize predicates and computations.
 * Based on DuckDB's ExpressionRewriter
 */
class ExpressionRewriter {
public:
    ExpressionRewriter();
    ~ExpressionRewriter() = default;
    
    /**
     * @brief Rewrite expression using all rules
     * 
     * @param expr Input expression
     * @return Optimized expression
     */
    std::string Rewrite(const std::string& expr);
    
    /**
     * @brief Add custom rule
     */
    void AddRule(std::unique_ptr<ExpressionRule> rule);
    
    /**
     * @brief Get statistics
     */
    struct Stats {
        int64_t expressions_rewritten = 0;
        int64_t rules_applied = 0;
    };
    
    Stats GetStats() const { return stats_; }
    
private:
    std::vector<std::unique_ptr<ExpressionRule>> rules_;
    Stats stats_;
};

} // namespace query
} // namespace sabot

