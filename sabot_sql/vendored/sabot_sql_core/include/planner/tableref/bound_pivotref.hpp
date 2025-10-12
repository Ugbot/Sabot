//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/planner/tableref/bound_pivotref.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/planner/binder.hpp"
#include "sabot_sql/planner/bound_tableref.hpp"
#include "sabot_sql/planner/expression.hpp"
#include "sabot_sql/parser/tableref/pivotref.hpp"
#include "sabot_sql/function/aggregate_function.hpp"

namespace sabot_sql {

struct BoundPivotInfo {
	//! The number of group columns
	idx_t group_count;
	//! The set of types
	vector<LogicalType> types;
	//! The set of values to pivot on
	vector<string> pivot_values;
	//! The set of aggregate functions that is being executed
	vector<unique_ptr<Expression>> aggregates;

	void Serialize(Serializer &serializer) const;
	static BoundPivotInfo Deserialize(Deserializer &deserializer);
};

class BoundPivotRef : public BoundTableRef {
public:
	static constexpr const TableReferenceType TYPE = TableReferenceType::PIVOT;

public:
	explicit BoundPivotRef() : BoundTableRef(TableReferenceType::PIVOT) {
	}

	idx_t bind_index;
	//! The binder used to bind the child of the pivot
	shared_ptr<Binder> child_binder;
	//! The child node of the pivot
	unique_ptr<BoundTableRef> child;
	//! The bound pivot info
	BoundPivotInfo bound_pivot;
};
} // namespace sabot_sql
