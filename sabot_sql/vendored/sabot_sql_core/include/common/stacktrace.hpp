//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/common/stacktrace.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/common/common.hpp"

namespace sabot_sql {

class StackTrace {
public:
	static string GetStacktracePointers(idx_t max_depth = 120);
	static string ResolveStacktraceSymbols(const string &pointers);

	inline static string GetStackTrace(idx_t max_depth = 120) {
		return ResolveStacktraceSymbols(GetStacktracePointers(max_depth));
	}
};

} // namespace sabot_sql
