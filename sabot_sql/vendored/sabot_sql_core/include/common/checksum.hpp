//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/common/checksum.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/common/constants.hpp"

namespace sabot_sql {

//! Compute a checksum over a buffer of size size
uint64_t Checksum(uint8_t *buffer, size_t size);

} // namespace sabot_sql
