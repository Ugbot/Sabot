//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/common/enums/profiler_format.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/common/constants.hpp"

namespace sabot_sql {

enum class ProfilerPrintFormat : uint8_t { QUERY_TREE, JSON, QUERY_TREE_OPTIMIZER, NO_OUTPUT, HTML, GRAPHVIZ };

} // namespace sabot_sql
