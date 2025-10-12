//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/common/enums/explain_format.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/common/typedefs.hpp"

namespace sabot_sql {

enum class ExplainFormat : uint8_t { DEFAULT, TEXT, JSON, HTML, GRAPHVIZ, YAML };

} // namespace sabot_sql
