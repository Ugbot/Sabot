//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/planner/expression_binder/order_binder.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/common/case_insensitive_map.hpp"
#include "sabot_sql/parser/expression_map.hpp"
#include "sabot_sql/parser/parsed_expression.hpp"
#include "sabot_sql/planner/expression/bound_columnref_expression.hpp"

namespace sabot_sql {
class Binder;
class Expression;
class SelectNode;
struct SelectBindState;

//! The ORDER binder is responsible for binding an expression within the ORDER BY clause of a SQL statement
class OrderBinder {
public:
	OrderBinder(vector<reference<Binder>> binders, SelectBindState &bind_state);
	OrderBinder(vector<reference<Binder>> binders, SelectNode &node, SelectBindState &bind_state);

public:
	unique_ptr<Expression> Bind(unique_ptr<ParsedExpression> expr);

	bool HasExtraList() const {
		return extra_list;
	}
	const vector<reference<Binder>> &GetBinders() const {
		return binders;
	}

	unique_ptr<Expression> CreateExtraReference(unique_ptr<ParsedExpression> expr);

	//! Sets the query component, for error messages
	void SetQueryComponent(string component = string());

private:
	unique_ptr<Expression> CreateProjectionReference(ParsedExpression &expr, const idx_t index);
	unique_ptr<Expression> BindConstant(ParsedExpression &expr);
	optional_idx TryGetProjectionReference(ParsedExpression &expr) const;

private:
	vector<reference<Binder>> binders;
	optional_ptr<vector<unique_ptr<ParsedExpression>>> extra_list;
	SelectBindState &bind_state;
	string query_component = "ORDER BY";
};

} // namespace sabot_sql
