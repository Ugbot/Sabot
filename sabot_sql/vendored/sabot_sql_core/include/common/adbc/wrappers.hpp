//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/common/adbc/wrappers.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql.h"
#include "sabot_sql/common/string.hpp"
#include "sabot_sql/common/unordered_map.hpp"

namespace sabot_sql {

struct SabotSQLAdbcConnectionWrapper {
	sabot_sql_connection connection;
	unordered_map<string, string> options;
};
} // namespace sabot_sql
