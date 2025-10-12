//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/common/enums/date_part_specifier.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/common/constants.hpp"

namespace sabot_sql {

enum class DatePartSpecifier : uint8_t {
	//	BIGINT values
	YEAR,
	MONTH,
	DAY,
	DECADE,
	CENTURY,
	MILLENNIUM,
	MICROSECONDS,
	MILLISECONDS,
	SECOND,
	MINUTE,
	HOUR,
	DOW,
	ISODOW,
	WEEK,
	ISOYEAR,
	QUARTER,
	DOY,
	YEARWEEK,
	ERA,
	TIMEZONE,
	TIMEZONE_HOUR,
	TIMEZONE_MINUTE,

	//	DOUBLE values
	EPOCH,
	JULIAN_DAY,

	//	Invalid
	INVALID,

	//	Type ranges
	BEGIN_BIGINT = YEAR,
	BEGIN_DOUBLE = EPOCH,
	BEGIN_INVALID = INVALID,
};

inline bool IsBigintDatepart(DatePartSpecifier part_code) {
	return size_t(part_code) < size_t(DatePartSpecifier::BEGIN_DOUBLE);
}

SABOT_SQL_API bool TryGetDatePartSpecifier(const string &specifier, DatePartSpecifier &result);
SABOT_SQL_API DatePartSpecifier GetDatePartSpecifier(const string &specifier);

} // namespace sabot_sql
