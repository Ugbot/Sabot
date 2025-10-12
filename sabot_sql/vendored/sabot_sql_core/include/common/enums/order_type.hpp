//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/common/enums/order_type.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/common/constants.hpp"
#include "sabot_sql/common/exception.hpp"

namespace sabot_sql {

enum class OrderType : uint8_t { INVALID = 0, ORDER_DEFAULT = 1, ASCENDING = 2, DESCENDING = 3 };

enum class OrderByNullType : uint8_t { INVALID = 0, ORDER_DEFAULT = 1, NULLS_FIRST = 2, NULLS_LAST = 3 };

enum class DefaultOrderByNullType : uint8_t {
	INVALID = 0,
	NULLS_FIRST = 2,
	NULLS_LAST = 3,
	NULLS_FIRST_ON_ASC_LAST_ON_DESC = 4,
	NULLS_LAST_ON_ASC_FIRST_ON_DESC = 5
};

} // namespace sabot_sql
