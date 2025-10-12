//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/function/aggregate/distributive_function_utils.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/function/function_set.hpp"

namespace sabot_sql {

struct CountFunctionBase {
	static AggregateFunction GetFunction();
};

struct FirstFunctionGetter {
	static AggregateFunction GetFunction(const LogicalType &type);
};

struct LastFunctionGetter {
	static AggregateFunction GetFunction(const LogicalType &type);
};

struct MinFunction {
	static AggregateFunction GetFunction();
};

struct MaxFunction {
	static AggregateFunction GetFunction();
};

} // namespace sabot_sql
