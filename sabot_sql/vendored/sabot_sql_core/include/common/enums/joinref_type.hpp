//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/common/enums/joinref_type.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/common/constants.hpp"

namespace sabot_sql {

//===--------------------------------------------------------------------===//
// Join Reference Types
//===--------------------------------------------------------------------===//
enum class JoinRefType : uint8_t {
	REGULAR,    // Explicit conditions
	NATURAL,    // Implied conditions
	CROSS,      // No condition
	POSITIONAL, // Positional condition
	ASOF,       // AsOf conditions
	DEPENDENT,  // Dependent join conditions
};

const char *ToString(JoinRefType value);

} // namespace sabot_sql
