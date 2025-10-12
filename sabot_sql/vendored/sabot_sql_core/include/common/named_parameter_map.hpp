//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/common/named_parameter_map.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/common/case_insensitive_map.hpp"
#include "sabot_sql/common/types.hpp"
namespace sabot_sql {

using named_parameter_type_map_t = case_insensitive_map_t<LogicalType>;
using named_parameter_map_t = case_insensitive_map_t<Value>;

} // namespace sabot_sql
