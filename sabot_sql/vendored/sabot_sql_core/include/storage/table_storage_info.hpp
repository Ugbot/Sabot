//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/storage/table_storage_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/common/types/value.hpp"
#include "sabot_sql/common/unordered_map.hpp"
#include "sabot_sql/storage/block.hpp"
#include "sabot_sql/storage/index_storage_info.hpp"
#include "sabot_sql/storage/storage_info.hpp"
#include "sabot_sql/common/optional_idx.hpp"

namespace sabot_sql {

//! Column segment information
struct ColumnSegmentInfo {
	idx_t row_group_index;
	idx_t column_id;
	string column_path;
	idx_t segment_idx;
	string segment_type;
	idx_t segment_start;
	idx_t segment_count;
	string compression_type;
	string segment_stats;
	bool has_updates;
	bool persistent;
	block_id_t block_id;
	vector<block_id_t> additional_blocks;
	idx_t block_offset;
	string segment_info;
};

//! Table storage information
class TableStorageInfo {
public:
	//! The (estimated) cardinality of the table
	optional_idx cardinality;
	//! Info of the indexes of a table
	vector<IndexInfo> index_info;
};

} // namespace sabot_sql
