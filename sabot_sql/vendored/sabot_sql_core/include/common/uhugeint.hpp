#pragma once

#include "sabot_sql/common/winapi.hpp"
#include "sabot_sql/common/string.hpp"
#include <stdint.h>

namespace sabot_sql {

// Forward declaration to allow conversion between hugeint and uhugeint
struct hugeint_t; // NOLINT

struct uhugeint_t { // NOLINT
public:
	uint64_t lower;
	uint64_t upper;

public:
	uhugeint_t() = default;
	SABOT_SQL_API uhugeint_t(uint64_t value); // NOLINT: Allow implicit conversion from `uint64_t`
	constexpr uhugeint_t(uint64_t upper, uint64_t lower) : lower(lower), upper(upper) {
	}
	constexpr uhugeint_t(const uhugeint_t &rhs) = default;
	constexpr uhugeint_t(uhugeint_t &&rhs) = default;
	uhugeint_t &operator=(const uhugeint_t &rhs) = default;
	uhugeint_t &operator=(uhugeint_t &&rhs) = default;

	SABOT_SQL_API string ToString() const;

	// comparison operators
	SABOT_SQL_API bool operator==(const uhugeint_t &rhs) const;
	SABOT_SQL_API bool operator!=(const uhugeint_t &rhs) const;
	SABOT_SQL_API bool operator<=(const uhugeint_t &rhs) const;
	SABOT_SQL_API bool operator<(const uhugeint_t &rhs) const;
	SABOT_SQL_API bool operator>(const uhugeint_t &rhs) const;
	SABOT_SQL_API bool operator>=(const uhugeint_t &rhs) const;

	// arithmetic operators
	SABOT_SQL_API uhugeint_t operator+(const uhugeint_t &rhs) const;
	SABOT_SQL_API uhugeint_t operator-(const uhugeint_t &rhs) const;
	SABOT_SQL_API uhugeint_t operator*(const uhugeint_t &rhs) const;
	SABOT_SQL_API uhugeint_t operator/(const uhugeint_t &rhs) const;
	SABOT_SQL_API uhugeint_t operator%(const uhugeint_t &rhs) const;
	SABOT_SQL_API uhugeint_t operator-() const;

	// bitwise operators
	SABOT_SQL_API uhugeint_t operator>>(const uhugeint_t &rhs) const;
	SABOT_SQL_API uhugeint_t operator<<(const uhugeint_t &rhs) const;
	SABOT_SQL_API uhugeint_t operator&(const uhugeint_t &rhs) const;
	SABOT_SQL_API uhugeint_t operator|(const uhugeint_t &rhs) const;
	SABOT_SQL_API uhugeint_t operator^(const uhugeint_t &rhs) const;
	SABOT_SQL_API uhugeint_t operator~() const;

	// in-place operators
	SABOT_SQL_API uhugeint_t &operator+=(const uhugeint_t &rhs);
	SABOT_SQL_API uhugeint_t &operator-=(const uhugeint_t &rhs);
	SABOT_SQL_API uhugeint_t &operator*=(const uhugeint_t &rhs);
	SABOT_SQL_API uhugeint_t &operator/=(const uhugeint_t &rhs);
	SABOT_SQL_API uhugeint_t &operator%=(const uhugeint_t &rhs);
	SABOT_SQL_API uhugeint_t &operator>>=(const uhugeint_t &rhs);
	SABOT_SQL_API uhugeint_t &operator<<=(const uhugeint_t &rhs);
	SABOT_SQL_API uhugeint_t &operator&=(const uhugeint_t &rhs);
	SABOT_SQL_API uhugeint_t &operator|=(const uhugeint_t &rhs);
	SABOT_SQL_API uhugeint_t &operator^=(const uhugeint_t &rhs);

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
	SABOT_SQL_API operator hugeint_t() const; // NOLINT: Allow implicit conversion from `uhugeint_t`
};

} // namespace sabot_sql

namespace std {
template <>
struct hash<sabot_sql::uhugeint_t> {
	size_t operator()(const sabot_sql::uhugeint_t &val) const {
		using std::hash;
		return hash<uint64_t> {}(val.upper) ^ hash<uint64_t> {}(val.lower);
	}
};
} // namespace std
