//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/common/operator/convert_to_string.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/common/common.hpp"
#include "sabot_sql/common/types.hpp"
#include "sabot_sql/common/type_util.hpp"
#include "sabot_sql/common/exception.hpp"

namespace sabot_sql {

struct ConvertToString {
	template <class SRC>
	static inline string Operation(SRC input) {
		throw InternalException("Unrecognized type for ConvertToString %s", GetTypeId<SRC>());
	}
};

template <>
SABOT_SQL_API string ConvertToString::Operation(bool input);
template <>
SABOT_SQL_API string ConvertToString::Operation(int8_t input);
template <>
SABOT_SQL_API string ConvertToString::Operation(int16_t input);
template <>
SABOT_SQL_API string ConvertToString::Operation(int32_t input);
template <>
SABOT_SQL_API string ConvertToString::Operation(int64_t input);
template <>
SABOT_SQL_API string ConvertToString::Operation(uint8_t input);
template <>
SABOT_SQL_API string ConvertToString::Operation(uint16_t input);
template <>
SABOT_SQL_API string ConvertToString::Operation(uint32_t input);
template <>
SABOT_SQL_API string ConvertToString::Operation(uint64_t input);
template <>
SABOT_SQL_API string ConvertToString::Operation(hugeint_t input);
template <>
SABOT_SQL_API string ConvertToString::Operation(uhugeint_t input);
template <>
SABOT_SQL_API string ConvertToString::Operation(float input);
template <>
SABOT_SQL_API string ConvertToString::Operation(double input);
template <>
SABOT_SQL_API string ConvertToString::Operation(interval_t input);
template <>
SABOT_SQL_API string ConvertToString::Operation(date_t input);
template <>
SABOT_SQL_API string ConvertToString::Operation(dtime_t input);
template <>
SABOT_SQL_API string ConvertToString::Operation(timestamp_t input);
template <>
SABOT_SQL_API string ConvertToString::Operation(string_t input);

} // namespace sabot_sql
