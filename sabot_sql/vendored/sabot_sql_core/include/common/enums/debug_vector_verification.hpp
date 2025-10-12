//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/common/enums/debug_vector_verification.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/common/constants.hpp"

namespace sabot_sql {

enum class DebugVectorVerification : uint8_t {
	NONE,
	DICTIONARY_EXPRESSION,
	DICTIONARY_OPERATOR,
	CONSTANT_OPERATOR,
	SEQUENCE_OPERATOR,
	NESTED_SHUFFLE,
	VARIANT_VECTOR
};

} // namespace sabot_sql
