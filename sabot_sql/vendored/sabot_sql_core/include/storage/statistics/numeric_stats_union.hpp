//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/storage/statistics/numeric_stats_union.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/common/common.hpp"
#include "sabot_sql/common/exception.hpp"
#include "sabot_sql/common/types/hugeint.hpp"

namespace sabot_sql {

struct NumericValueUnion {
	union Val {
		bool boolean;
		int8_t tinyint;
		int16_t smallint;
		int32_t integer;
		int64_t bigint;
		uint8_t utinyint;
		uint16_t usmallint;
		uint32_t uinteger;
		uint64_t ubigint;
		hugeint_t hugeint;
		uhugeint_t uhugeint;
		float float_;   // NOLINT
		double double_; // NOLINT
	} value_;           // NOLINT

	template <class T>
	T &GetReferenceUnsafe();
};

template <>
SABOT_SQL_API inline bool &NumericValueUnion::GetReferenceUnsafe() {
	return value_.boolean;
}

template <>
SABOT_SQL_API inline int8_t &NumericValueUnion::GetReferenceUnsafe() {
	return value_.tinyint;
}

template <>
SABOT_SQL_API inline int16_t &NumericValueUnion::GetReferenceUnsafe() {
	return value_.smallint;
}

template <>
SABOT_SQL_API inline int32_t &NumericValueUnion::GetReferenceUnsafe() {
	return value_.integer;
}

template <>
SABOT_SQL_API inline int64_t &NumericValueUnion::GetReferenceUnsafe() {
	return value_.bigint;
}

template <>
SABOT_SQL_API inline hugeint_t &NumericValueUnion::GetReferenceUnsafe() {
	return value_.hugeint;
}

template <>
SABOT_SQL_API inline uhugeint_t &NumericValueUnion::GetReferenceUnsafe() {
	return value_.uhugeint;
}

template <>
SABOT_SQL_API inline uint8_t &NumericValueUnion::GetReferenceUnsafe() {
	return value_.utinyint;
}

template <>
SABOT_SQL_API inline uint16_t &NumericValueUnion::GetReferenceUnsafe() {
	return value_.usmallint;
}

template <>
SABOT_SQL_API inline uint32_t &NumericValueUnion::GetReferenceUnsafe() {
	return value_.uinteger;
}

template <>
SABOT_SQL_API inline uint64_t &NumericValueUnion::GetReferenceUnsafe() {
	return value_.ubigint;
}

template <>
SABOT_SQL_API inline float &NumericValueUnion::GetReferenceUnsafe() {
	return value_.float_;
}

template <>
SABOT_SQL_API inline double &NumericValueUnion::GetReferenceUnsafe() {
	return value_.double_;
}

} // namespace sabot_sql
