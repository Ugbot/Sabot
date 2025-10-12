//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/common/enums/prepared_statement_mode.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/common/constants.hpp"

namespace sabot_sql {

enum class PreparedStatementMode : uint8_t {
	PREPARE_ONLY,
	PREPARE_AND_EXECUTE,
};

} // namespace sabot_sql
