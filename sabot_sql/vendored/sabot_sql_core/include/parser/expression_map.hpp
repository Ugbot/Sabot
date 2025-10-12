//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/parser/expression_map.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/common/unordered_map.hpp"
#include "sabot_sql/common/unordered_set.hpp"
#include "sabot_sql/parser/base_expression.hpp"
#include "sabot_sql/parser/parsed_expression.hpp"
#include "sabot_sql/planner/expression.hpp"

namespace sabot_sql {
class Expression;

template <class T>
struct ExpressionHashFunction {
	uint64_t operator()(const reference<T> &expr) const {
		return (uint64_t)expr.get().Hash();
	}
};

template <class T>
struct ExpressionEquality {
	bool operator()(const reference<T> &a, const reference<T> &b) const {
		return a.get().Equals(b.get());
	}
};

template <typename T>
using expression_map_t =
    unordered_map<reference<Expression>, T, ExpressionHashFunction<Expression>, ExpressionEquality<Expression>>;

using expression_set_t =
    unordered_set<reference<Expression>, ExpressionHashFunction<Expression>, ExpressionEquality<Expression>>;

template <typename T>
using parsed_expression_map_t = unordered_map<reference<ParsedExpression>, T, ExpressionHashFunction<ParsedExpression>,
                                              ExpressionEquality<ParsedExpression>>;

using parsed_expression_set_t = unordered_set<reference<ParsedExpression>, ExpressionHashFunction<ParsedExpression>,
                                              ExpressionEquality<ParsedExpression>>;

} // namespace sabot_sql
