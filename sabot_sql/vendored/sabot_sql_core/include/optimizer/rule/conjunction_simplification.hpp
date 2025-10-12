//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/optimizer/rule/conjunction_simplification.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/optimizer/rule.hpp"

namespace sabot_sql {

// The Conjunction Simplification rule rewrites conjunctions with a constant
class ConjunctionSimplificationRule : public Rule {
public:
	explicit ConjunctionSimplificationRule(ExpressionRewriter &rewriter);

	unique_ptr<Expression> Apply(LogicalOperator &op, vector<reference<Expression>> &bindings, bool &changes_made,
	                             bool is_root) override;

	unique_ptr<Expression> RemoveExpression(BoundConjunctionExpression &conj, const Expression &expr);
};

} // namespace sabot_sql
