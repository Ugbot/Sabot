//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/transaction/append_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/common/constants.hpp"

namespace sabot_sql {
class DataTable;

struct AppendInfo {
	DataTable *table;
	idx_t start_row;
	idx_t count;
};

} // namespace sabot_sql
