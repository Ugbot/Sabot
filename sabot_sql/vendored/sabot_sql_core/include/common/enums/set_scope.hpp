//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/common/enums/set_scope.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/common/constants.hpp"

namespace sabot_sql {

enum class SetScope : uint8_t {
	AUTOMATIC = 0,
	LOCAL = 1, /* unused */
	SESSION = 2,
	GLOBAL = 3,
	VARIABLE = 4
};

} // namespace sabot_sql
