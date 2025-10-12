//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/optimizer/rule/distributivity.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/optimizer/rule.hpp"
#include "sabot_sql/parser/expression_map.hpp"

namespace sabot_sql {

// (X AND B) OR (X AND C) OR (X AND D) = X AND (B OR C OR D)
class DistributivityRule : public Rule {
public:
	explicit DistributivityRule(ExpressionRewriter &rewriter);

	unique_ptr<Expression> Apply(LogicalOperator &op, vector<reference<Expression>> &bindings, bool &changes_made,
	                             bool is_root) override;

private:
	void AddExpressionSet(Expression &expr, expression_set_t &set);
	unique_ptr<Expression> ExtractExpression(BoundConjunctionExpression &conj, idx_t idx, Expression &expr);
};

} // namespace sabot_sql
