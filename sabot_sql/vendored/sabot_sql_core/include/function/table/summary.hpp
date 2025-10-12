//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/function/table/summary.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/function/table_function.hpp"
#include "sabot_sql/function/built_in_functions.hpp"

namespace sabot_sql {

struct SummaryTableFunction {
	static void RegisterFunction(BuiltinFunctions &set);
};

} // namespace sabot_sql
