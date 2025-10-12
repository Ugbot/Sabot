//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/execution/index/index_type_set.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/execution/index/index_type.hpp"
#include "sabot_sql/common/mutex.hpp"
#include "sabot_sql/common/case_insensitive_map.hpp"
#include "sabot_sql/common/string.hpp"
#include "sabot_sql/common/optional_ptr.hpp"

namespace sabot_sql {

class IndexTypeSet {
	mutex lock;
	case_insensitive_map_t<IndexType> functions;

public:
	IndexTypeSet();
	SABOT_SQL_API optional_ptr<IndexType> FindByName(const string &name);
	SABOT_SQL_API void RegisterIndexType(const IndexType &index_type);
};

} // namespace sabot_sql
