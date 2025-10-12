//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/optimizer/rule/date_part_simplification.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/optimizer/rule.hpp"

namespace sabot_sql {

// The DatePart Simplification rule rewrites date_part with a constant specifier into a specialized function (e.g.
// date_part('year', x) => year(x))
class DatePartSimplificationRule : public Rule {
public:
	explicit DatePartSimplificationRule(ExpressionRewriter &rewriter);

	unique_ptr<Expression> Apply(LogicalOperator &op, vector<reference<Expression>> &bindings, bool &changes_made,
	                             bool is_root) override;
};

} // namespace sabot_sql
