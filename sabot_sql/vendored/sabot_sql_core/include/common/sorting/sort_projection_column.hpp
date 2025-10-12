//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/common/sorting/sort_projection_column.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/common/typedefs.hpp"

namespace sabot_sql {

struct SortProjectionColumn {
	bool is_payload;
	idx_t layout_col_idx;
	idx_t output_col_idx;
};

} // namespace sabot_sql
