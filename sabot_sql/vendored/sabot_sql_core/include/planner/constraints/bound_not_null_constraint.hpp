//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/planner/constraints/bound_not_null_constraint.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/planner/bound_constraint.hpp"

namespace sabot_sql {

class BoundNotNullConstraint : public BoundConstraint {
public:
	static constexpr const ConstraintType TYPE = ConstraintType::NOT_NULL;

public:
	explicit BoundNotNullConstraint(PhysicalIndex index) : BoundConstraint(ConstraintType::NOT_NULL), index(index) {
	}

	//! Column index this constraint pertains to
	PhysicalIndex index;

	unique_ptr<BoundConstraint> Copy() const override {
		return make_uniq<BoundNotNullConstraint>(index);
	}
};

} // namespace sabot_sql
