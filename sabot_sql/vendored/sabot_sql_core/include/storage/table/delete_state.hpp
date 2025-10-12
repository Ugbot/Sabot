//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/storage/table/delete_state.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/storage/table/append_state.hpp"

namespace sabot_sql {
class TableCatalogEntry;

struct TableDeleteState {
	unique_ptr<ConstraintState> constraint_state;
	bool has_delete_constraints = false;
	DataChunk verify_chunk;
	vector<StorageIndex> col_ids;
};

} // namespace sabot_sql
