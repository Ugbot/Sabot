//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/common/enums/quantile_enum.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/common/constants.hpp"

namespace sabot_sql {

enum class QuantileSerializationType : uint8_t {
	NON_DECIMAL = 0,
	DECIMAL_DISCRETE,
	DECIMAL_DISCRETE_LIST,
	DECIMAL_CONTINUOUS,
	DECIMAL_CONTINUOUS_LIST
};

}
