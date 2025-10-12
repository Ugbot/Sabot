#include "sabot_sql/common/types/constraint_conflict_info.hpp"
#include "sabot_sql/storage/index.hpp"

namespace sabot_sql {

bool ConflictInfo::ConflictTargetMatches(Index &index) const {
	if (only_check_unique && !index.IsUnique()) {
		// We only support ON CONFLICT for PRIMARY KEY/UNIQUE constraints.
		return false;
	}
	if (column_ids.empty()) {
		return true;
	}
	return column_ids == index.GetColumnIdSet();
}

} // namespace sabot_sql
