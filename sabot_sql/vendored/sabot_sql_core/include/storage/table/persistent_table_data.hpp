//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/storage/table/persistent_table_data.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/common/constants.hpp"
#include "sabot_sql/common/vector.hpp"
#include "sabot_sql/storage/data_pointer.hpp"
#include "sabot_sql/storage/table/table_statistics.hpp"
#include "sabot_sql/storage/metadata/metadata_manager.hpp"

namespace sabot_sql {
class BaseStatistics;

class PersistentTableData {
public:
	explicit PersistentTableData(idx_t column_count);
	~PersistentTableData();

	MetaBlockPointer base_table_pointer;
	TableStatistics table_stats;
	idx_t total_rows;
	idx_t row_group_count;
	MetaBlockPointer block_pointer;
};

} // namespace sabot_sql
