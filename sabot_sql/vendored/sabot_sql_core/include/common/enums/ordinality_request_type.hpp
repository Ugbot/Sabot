//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/common/enums/ordinality_request_type.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once
#include "sabot_sql/common/typedefs.hpp"

namespace sabot_sql {

enum class OrdinalityType : uint8_t { WITHOUT_ORDINALITY = 0, WITH_ORDINALITY = 1 };

} // namespace sabot_sql
