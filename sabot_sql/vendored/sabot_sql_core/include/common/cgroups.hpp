//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/common/cgroups.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/common/common.hpp"
#include "sabot_sql/common/optional_idx.hpp"
#include "sabot_sql/common/file_system.hpp"

namespace sabot_sql {

class CGroups {
public:
	static optional_idx GetMemoryLimit(FileSystem &fs);
	static idx_t GetCPULimit(FileSystem &fs, idx_t physical_cores);
};

} // namespace sabot_sql
