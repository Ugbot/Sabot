//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/function/table/range.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/function/table_function.hpp"
#include "sabot_sql/function/built_in_functions.hpp"

namespace sabot_sql {

struct CheckpointFunction {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct GlobTableFunction {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct RangeTableFunction {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct RepeatTableFunction {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct RepeatRowTableFunction {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct UnnestTableFunction {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct CSVSnifferFunction {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct ReadBlobFunction {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct ReadTextFunction {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct QueryTableFunction {
	static void RegisterFunction(BuiltinFunctions &set);
};

} // namespace sabot_sql
