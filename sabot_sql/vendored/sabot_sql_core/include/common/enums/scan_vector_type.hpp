//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/common/enums/scan_vector_type.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/common/constants.hpp"

namespace sabot_sql {

enum class ScanVectorType { SCAN_ENTIRE_VECTOR, SCAN_FLAT_VECTOR };

enum class ScanVectorMode { REGULAR_SCAN, SCAN_COMMITTED, SCAN_COMMITTED_NO_UPDATES };

} // namespace sabot_sql
