//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/planner/constraints/bound_check_constraint.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/common/unordered_set.hpp"
#include "sabot_sql/planner/bound_constraint.hpp"
#include "sabot_sql/planner/expression.hpp"
#include "sabot_sql/common/index_map.hpp"

namespace sabot_sql {

//! The CheckConstraint contains an expression that must evaluate to TRUE for
//! every row in a table
class BoundCheckConstraint : public BoundConstraint {
public:
	static constexpr const ConstraintType TYPE = ConstraintType::CHECK;

public:
	BoundCheckConstraint() : BoundConstraint(ConstraintType::CHECK) {
	}

	//! The expression
	unique_ptr<Expression> expression;
	//! The columns used by the CHECK constraint
	physical_index_set_t bound_columns;

public:
	unique_ptr<BoundConstraint> Copy() const override {
		auto result = make_uniq<BoundCheckConstraint>();
		result->expression = expression->Copy();
		result->bound_columns = bound_columns;
		return std::move(result);
	}
};

} // namespace sabot_sql
