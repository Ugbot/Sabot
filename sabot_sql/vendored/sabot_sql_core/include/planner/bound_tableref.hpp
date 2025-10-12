//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/planner/bound_tableref.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/common/common.hpp"
#include "sabot_sql/common/enums/tableref_type.hpp"
#include "sabot_sql/parser/parsed_data/sample_options.hpp"

namespace sabot_sql {

class BoundTableRef {
public:
	explicit BoundTableRef(TableReferenceType type) : type(type) {
	}
	virtual ~BoundTableRef() {
	}

	//! The type of table reference
	TableReferenceType type;
	//! The sample options (if any)
	unique_ptr<SampleOptions> sample;

public:
	template <class TARGET>
	TARGET &Cast() {
		if (type != TARGET::TYPE) {
			throw InternalException("Failed to cast bound table ref to type - table ref type mismatch");
		}
		return reinterpret_cast<TARGET &>(*this);
	}

	template <class TARGET>
	const TARGET &Cast() const {
		if (type != TARGET::TYPE) {
			throw InternalException("Failed to cast bound table ref to type - table ref type mismatch");
		}
		return reinterpret_cast<const TARGET &>(*this);
	}
};
} // namespace sabot_sql
