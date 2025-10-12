//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/storage/compression/chimp/algorithm/bit_utils.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/common/numeric_utils.hpp"

namespace sabot_sql {

template <class R>
struct BitUtils {
	static constexpr R Mask(unsigned int const bits) {
		return UnsafeNumericCast<R>((((uint64_t)(bits < (sizeof(R) * 8))) << (bits & ((sizeof(R) * 8) - 1))) - 1U);
	}
};

} // namespace sabot_sql
