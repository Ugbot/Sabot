//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/parser/statement/logical_plan_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/parser/sql_statement.hpp"
#include "sabot_sql/planner/logical_operator.hpp"

namespace sabot_sql {

class LogicalPlanStatement : public SQLStatement {
public:
	static constexpr const StatementType TYPE = StatementType::LOGICAL_PLAN_STATEMENT;

public:
	explicit LogicalPlanStatement(unique_ptr<LogicalOperator> plan_p)
	    : SQLStatement(StatementType::LOGICAL_PLAN_STATEMENT), plan(std::move(plan_p)) {};

	unique_ptr<LogicalOperator> plan;

public:
	unique_ptr<SQLStatement> Copy() const override {
		throw NotImplementedException("PLAN_STATEMENT");
	}
	string ToString() const override {
		throw NotImplementedException("PLAN STATEMENT");
	}
};

} // namespace sabot_sql
