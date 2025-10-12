//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/planner/subquery/recursive_dependent_join_planner.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/planner/logical_operator_visitor.hpp"

namespace sabot_sql {

class Binder;

/*
 * Recursively plan subqueries and flatten dependent joins from outermost to innermost (like peeling an onion).
 */
class RecursiveDependentJoinPlanner : public LogicalOperatorVisitor {
public:
	explicit RecursiveDependentJoinPlanner(Binder &binder) : binder(binder) {
	}
	void VisitOperator(LogicalOperator &op) override;
	unique_ptr<Expression> VisitReplace(BoundSubqueryExpression &expr, unique_ptr<Expression> *expr_ptr) override;

private:
	unique_ptr<LogicalOperator> root;
	Binder &binder;
};
} // namespace sabot_sql
