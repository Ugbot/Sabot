//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/common/enums/tuple_data_layout_enums.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/common/constants.hpp"

namespace sabot_sql {

enum class TupleDataValidityType : uint8_t {
	CAN_HAVE_NULL_VALUES = 0,
	CANNOT_HAVE_NULL_VALUES = 1,
};

enum class TupleDataNestednessType : uint8_t {
	TOP_LEVEL_LAYOUT = 0,
	NESTED_STRUCT_LAYOUT = 1,
};

} // namespace sabot_sql
