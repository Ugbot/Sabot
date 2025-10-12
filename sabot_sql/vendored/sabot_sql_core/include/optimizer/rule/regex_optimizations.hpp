//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/optimizer/rule/like_optimizations.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/optimizer/rule.hpp"
#include "sabot_sql/function/scalar/string_functions.hpp"

namespace sabot_sql {

class RegexOptimizationRule : public Rule {
public:
	explicit RegexOptimizationRule(ExpressionRewriter &rewriter);

	unique_ptr<Expression> Apply(LogicalOperator &op, vector<reference<Expression>> &bindings, bool &changes_made,
	                             bool is_root) override;

	unique_ptr<Expression> ApplyRule(BoundFunctionExpression *expr, ScalarFunction function, string pattern,
	                                 bool is_not_like);
};

} // namespace sabot_sql
