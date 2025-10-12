//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/function/cast_rules.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/common/types.hpp"

namespace sabot_sql {
//! Contains a list of rules for casting
class CastRules {
public:
	//! Returns the cost of performing an implicit cost from "from" to "to", or -1 if an implicit cast is not possible
	static int64_t ImplicitCast(const LogicalType &from, const LogicalType &to);
};

} // namespace sabot_sql
