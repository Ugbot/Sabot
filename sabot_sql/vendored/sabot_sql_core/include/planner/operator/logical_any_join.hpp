//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/planner/operator/logical_any_join.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/parser/parsed_expression.hpp"
#include "sabot_sql/planner/operator/logical_join.hpp"

namespace sabot_sql {

//! LogicalAnyJoin represents a join with an arbitrary expression as JoinCondition
class LogicalAnyJoin : public LogicalJoin {
public:
	static constexpr const LogicalOperatorType TYPE = LogicalOperatorType::LOGICAL_ANY_JOIN;

public:
	explicit LogicalAnyJoin(JoinType type);

	//! The JoinCondition on which this join is performed
	unique_ptr<Expression> condition;

public:
	InsertionOrderPreservingMap<string> ParamsToString() const override;

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<LogicalOperator> Deserialize(Deserializer &deserializer);
};

} // namespace sabot_sql
