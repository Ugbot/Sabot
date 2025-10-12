//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/storage/buffer/temporary_file_information.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/common/common.hpp"
#include "sabot_sql/common/enums/memory_tag.hpp"

namespace sabot_sql {

struct MemoryInformation {
	MemoryTag tag;
	idx_t size;
	idx_t evicted_data;
};

struct TemporaryFileInformation {
	string path;
	idx_t size;
};

struct CachedFileInformation {
	string path;
	idx_t nr_bytes;
	idx_t location;
	bool loaded;
};

} // namespace sabot_sql
