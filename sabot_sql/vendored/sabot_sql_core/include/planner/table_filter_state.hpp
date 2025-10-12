//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/planner/table_filter_state.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/planner/table_filter.hpp"
#include "sabot_sql/common/types/selection_vector.hpp"
#include "sabot_sql/execution/expression_executor.hpp"

namespace sabot_sql {

//! Thread-local state for executing a table filter
struct TableFilterState {
public:
	virtual ~TableFilterState() = default;

public:
	static unique_ptr<TableFilterState> Initialize(ClientContext &context, const TableFilter &filter);

public:
	template <class TARGET>
	TARGET &Cast() {
		DynamicCastCheck<TARGET>(this);
		return reinterpret_cast<TARGET &>(*this);
	}
	template <class TARGET>
	const TARGET &Cast() const {
		DynamicCastCheck<TARGET>(this);
		return reinterpret_cast<const TARGET &>(*this);
	}
};

struct ConjunctionAndFilterState : public TableFilterState {
public:
	vector<unique_ptr<TableFilterState>> child_states;
};

struct ConjunctionOrFilterState : public TableFilterState {
public:
	vector<unique_ptr<TableFilterState>> child_states;
};

struct ExpressionFilterState : public TableFilterState {
public:
	ExpressionFilterState(ClientContext &context, const Expression &expression);

	ExpressionExecutor executor;
};

} // namespace sabot_sql
