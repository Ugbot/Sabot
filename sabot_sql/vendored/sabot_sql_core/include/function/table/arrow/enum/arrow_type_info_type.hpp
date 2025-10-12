#pragma once

#include <cstdint>

namespace sabot_sql {

enum class ArrowTypeInfoType : uint8_t { LIST, STRUCT, DATE_TIME, STRING, ARRAY, DECIMAL };

} // namespace sabot_sql
