#pragma once

#include "sabot_sql/common/winapi.hpp"
#include "sabot_sql/common/string.hpp"
#include <stdint.h>
#include "sabot_sql/common/typedefs.hpp"

namespace sabot_sql {

// Forward declaration to allow conversion between hugeint and uhugeint
struct uhugeint_t; // NOLINT: use numeric casing

struct hugeint_t { // NOLINT: use numeric casing
public:
	uint64_t lower;
	int64_t upper;

public:
	hugeint_t() = default;
	SABOT_SQL_API hugeint_t(int64_t value); // NOLINT: Allow implicit conversion from `int64_t`
	constexpr hugeint_t(int64_t upper, uint64_t lower) : lower(lower), upper(upper) {
	}
	constexpr hugeint_t(const hugeint_t &rhs) = default;
	constexpr hugeint_t(hugeint_t &&rhs) = default;
	hugeint_t &operator=(const hugeint_t &rhs) = default;
	hugeint_t &operator=(hugeint_t &&rhs) = default;

	SABOT_SQL_API string ToString() const;

	// comparison operators
	SABOT_SQL_API bool operator==(const hugeint_t &rhs) const;
	SABOT_SQL_API bool operator!=(const hugeint_t &rhs) const;
	SABOT_SQL_API bool operator<=(const hugeint_t &rhs) const;
	SABOT_SQL_API bool operator<(const hugeint_t &rhs) const;
	SABOT_SQL_API bool operator>(const hugeint_t &rhs) const;
	SABOT_SQL_API bool operator>=(const hugeint_t &rhs) const;

	// arithmetic operators
	SABOT_SQL_API hugeint_t operator+(const hugeint_t &rhs) const;
	SABOT_SQL_API hugeint_t operator-(const hugeint_t &rhs) const;
	SABOT_SQL_API hugeint_t operator*(const hugeint_t &rhs) const;
	SABOT_SQL_API hugeint_t operator/(const hugeint_t &rhs) const;
	SABOT_SQL_API hugeint_t operator%(const hugeint_t &rhs) const;
	SABOT_SQL_API hugeint_t operator-() const;

	// bitwise operators
	SABOT_SQL_API hugeint_t operator>>(const hugeint_t &rhs) const;
	SABOT_SQL_API hugeint_t operator<<(const hugeint_t &rhs) const;
	SABOT_SQL_API hugeint_t operator&(const hugeint_t &rhs) const;
	SABOT_SQL_API hugeint_t operator|(const hugeint_t &rhs) const;
	SABOT_SQL_API hugeint_t operator^(const hugeint_t &rhs) const;
	SABOT_SQL_API hugeint_t operator~() const;

	// in-place operators
	SABOT_SQL_API hugeint_t &operator+=(const hugeint_t &rhs);
	SABOT_SQL_API hugeint_t &operator-=(const hugeint_t &rhs);
	SABOT_SQL_API hugeint_t &operator*=(const hugeint_t &rhs);
	SABOT_SQL_API hugeint_t &operator/=(const hugeint_t &rhs);
	SABOT_SQL_API hugeint_t &operator%=(const hugeint_t &rhs);
	SABOT_SQL_API hugeint_t &operator>>=(const hugeint_t &rhs);
	SABOT_SQL_API hugeint_t &operator<<=(const hugeint_t &rhs);
	SABOT_SQL_API hugeint_t &operator&=(const hugeint_t &rhs);
	SABOT_SQL_API hugeint_t &operator|=(const hugeint_t &rhs);
	SABOT_SQL_API hugeint_t &operator^=(const hugeint_t &rhs);

	// boolean operators
	SABOT_SQL_API explicit operator bool() const;
	SABOT_SQL_API bool operator!() const;

	// cast operators -- doesn't check bounds/overflow/underflow
	SABOT_SQL_API explicit operator uint8_t() const;
	SABOT_SQL_API explicit operator uint16_t() const;
	SABOT_SQL_API explicit operator uint32_t() const;
	SABOT_SQL_API explicit operator uint64_t() const;
	SABOT_SQL_API explicit operator int8_t() const;
	SABOT_SQL_API explicit operator int16_t() const;
	SABOT_SQL_API explicit operator int32_t() const;
	SABOT_SQL_API explicit operator int64_t() const;
	SABOT_SQL_API operator uhugeint_t() const; // NOLINT: Allow implicit conversion from `hugeint_t`
};

} // namespace sabot_sql

namespace std {
template <>
struct hash<sabot_sql::hugeint_t> {
	size_t operator()(const sabot_sql::hugeint_t &val) const {
		using std::hash;
		return hash<int64_t> {}(val.upper) ^ hash<uint64_t> {}(val.lower);
	}
};
} // namespace std
