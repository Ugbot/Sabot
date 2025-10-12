//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/planner/bound_constraint.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/common/common.hpp"
#include "sabot_sql/parser/constraint.hpp"
#include "sabot_sql/common/exception.hpp"

namespace sabot_sql {
//! Bound equivalent of Constraint
class BoundConstraint {
public:
	explicit BoundConstraint(ConstraintType type) : type(type) {};
	virtual ~BoundConstraint() {
	}

	ConstraintType type;

public:
	template <class TARGET>
	TARGET &Cast() {
		if (type != TARGET::TYPE) {
			throw InternalException("Failed to cast constraint to type - bound constraint type mismatch");
		}
		return reinterpret_cast<TARGET &>(*this);
	}

	template <class TARGET>
	const TARGET &Cast() const {
		if (type != TARGET::TYPE) {
			throw InternalException("Failed to cast constraint to type - bound constraint type mismatch");
		}
		return reinterpret_cast<const TARGET &>(*this);
	}

	virtual unique_ptr<BoundConstraint> Copy() const = 0;
};
} // namespace sabot_sql
