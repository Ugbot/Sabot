//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/optimizer/rule.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/optimizer/matcher/expression_matcher.hpp"
#include "sabot_sql/optimizer/matcher/logical_operator_matcher.hpp"

namespace sabot_sql {
class ExpressionRewriter;

class Rule {
public:
	explicit Rule(ExpressionRewriter &rewriter) : rewriter(rewriter) {
	}
	virtual ~Rule() {
	}

	//! The expression rewriter this rule belongs to
	ExpressionRewriter &rewriter;
	//! The expression matcher of the rule
	unique_ptr<ExpressionMatcher> root;

	ClientContext &GetContext() const;
	virtual unique_ptr<Expression> Apply(LogicalOperator &op, vector<reference<Expression>> &bindings,
	                                     bool &fixed_point, bool is_root) = 0;
};

} // namespace sabot_sql
