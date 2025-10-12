//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/optimizer/rule/in_clause_simplification.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/optimizer/rule.hpp"

namespace sabot_sql {

// The in clause simplification rule rewrites cases where left is a column ref with a cast and right are constant values
class InClauseSimplificationRule : public Rule {
public:
	explicit InClauseSimplificationRule(ExpressionRewriter &rewriter);

	unique_ptr<Expression> Apply(LogicalOperator &op, vector<reference<Expression>> &bindings, bool &changes_made,
	                             bool is_root) override;
};

} // namespace sabot_sql
