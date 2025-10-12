//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/optimizer/rule/arithmetic_simplification.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/optimizer/rule.hpp"

namespace sabot_sql {

// The Arithmetic Simplification rule applies arithmetic expressions to which the answer is known (e.g. X + 0 => X, X *
// 0 => 0)
class ArithmeticSimplificationRule : public Rule {
public:
	explicit ArithmeticSimplificationRule(ExpressionRewriter &rewriter);

	unique_ptr<Expression> Apply(LogicalOperator &op, vector<reference<Expression>> &bindings, bool &changes_made,
	                             bool is_root) override;
};

} // namespace sabot_sql
