//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/function/scalar/operators.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/function/function_set.hpp"
#include "sabot_sql/function/scalar_function.hpp"
#include "sabot_sql/function/built_in_functions.hpp"

namespace sabot_sql {

struct AddFunction {
	static ScalarFunction GetFunction(const LogicalType &type);
	static ScalarFunction GetFunction(const LogicalType &left_type, const LogicalType &right_type);
};

struct SubtractFunction {
	static ScalarFunction GetFunction(const LogicalType &type);
	static ScalarFunction GetFunction(const LogicalType &left_type, const LogicalType &right_type);
};

} // namespace sabot_sql
