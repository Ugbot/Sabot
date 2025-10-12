//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/storage/database_size.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/common/common.hpp"
#include "sabot_sql/storage/storage_info.hpp"

namespace sabot_sql {

struct DatabaseSize {
	idx_t total_blocks = 0;
	idx_t block_size = 0;
	idx_t free_blocks = 0;
	idx_t used_blocks = 0;
	idx_t bytes = 0;
	idx_t wal_size = 0;
};

struct MetadataBlockInfo {
	block_id_t block_id;
	idx_t total_blocks;
	vector<idx_t> free_list;
};

} // namespace sabot_sql
