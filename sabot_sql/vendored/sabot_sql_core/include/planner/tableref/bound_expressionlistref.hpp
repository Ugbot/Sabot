//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/planner/tableref/bound_expressionlistref.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/planner/bound_tableref.hpp"
#include "sabot_sql/planner/expression.hpp"

namespace sabot_sql {
//! Represents a TableReference to a base table in the schema
class BoundExpressionListRef : public BoundTableRef {
public:
	static constexpr const TableReferenceType TYPE = TableReferenceType::EXPRESSION_LIST;

public:
	BoundExpressionListRef() : BoundTableRef(TableReferenceType::EXPRESSION_LIST) {
	}

	//! The bound VALUES list
	vector<vector<unique_ptr<Expression>>> values;
	//! The generated names of the values list
	vector<string> names;
	//! The types of the values list
	vector<LogicalType> types;
	//! The index in the bind context
	idx_t bind_index;
};
} // namespace sabot_sql
