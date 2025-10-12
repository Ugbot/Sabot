//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/common/enums/cte_materialize.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/common/constants.hpp"

namespace sabot_sql {

enum class CTEMaterialize : uint8_t {
	CTE_MATERIALIZE_DEFAULT = 1, /* no option specified */
	CTE_MATERIALIZE_ALWAYS = 2,  /* MATERIALIZED */
	CTE_MATERIALIZE_NEVER = 3    /* NOT MATERIALIZED */
};

} // namespace sabot_sql
