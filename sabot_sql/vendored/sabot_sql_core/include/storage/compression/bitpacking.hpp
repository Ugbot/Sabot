//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/storage/compression/bitpacking.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/common/string_util.hpp"

namespace sabot_sql {

enum class BitpackingMode : uint8_t { INVALID, AUTO, CONSTANT, CONSTANT_DELTA, DELTA_FOR, FOR };

BitpackingMode BitpackingModeFromString(const string &str);
string BitpackingModeToString(const BitpackingMode &mode);

} // namespace sabot_sql
