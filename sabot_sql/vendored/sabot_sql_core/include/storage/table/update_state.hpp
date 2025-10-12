//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/storage/table/update_state.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/storage/table/append_state.hpp"

namespace sabot_sql {
class TableCatalogEntry;

struct TableUpdateState {
	unique_ptr<ConstraintState> constraint_state;
};

} // namespace sabot_sql
