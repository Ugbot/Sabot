//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/common/exception_format_value.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/common/string.hpp"
#include "sabot_sql/common/hugeint.hpp"

#include <vector>

namespace sabot_sql {

class String;

// Helper class to support custom overloading
// Escaping " and quoting the value with "
class SQLIdentifier {
public:
	explicit SQLIdentifier(const string &raw_string) : raw_string(raw_string) {
	}

public:
	string raw_string;
};

// Helper class to support custom overloading
// Escaping ' and quoting the value with '
class SQLString {
public:
	explicit SQLString(const string &raw_string) : raw_string(raw_string) {
	}

public:
	string raw_string;
};

enum class PhysicalType : uint8_t;
struct LogicalType;

enum class ExceptionFormatValueType : uint8_t {
	FORMAT_VALUE_TYPE_DOUBLE,
	FORMAT_VALUE_TYPE_INTEGER,
	FORMAT_VALUE_TYPE_STRING
};

struct ExceptionFormatValue {
	SABOT_SQL_API ExceptionFormatValue(double dbl_val);     // NOLINT
	SABOT_SQL_API ExceptionFormatValue(int64_t int_val);    // NOLINT
	SABOT_SQL_API ExceptionFormatValue(idx_t uint_val);     // NOLINT
	SABOT_SQL_API ExceptionFormatValue(string str_val);     // NOLINT
	SABOT_SQL_API ExceptionFormatValue(String str_val);     // NOLINT
	SABOT_SQL_API ExceptionFormatValue(hugeint_t hg_val);   // NOLINT
	SABOT_SQL_API ExceptionFormatValue(uhugeint_t uhg_val); // NOLINT

	ExceptionFormatValueType type;

	double dbl_val = 0;
	hugeint_t int_val = 0;
	string str_val;

public:
	template <class T>
	static ExceptionFormatValue CreateFormatValue(T value) {
		return int64_t(value);
	}
	static string Format(const string &msg, std::vector<ExceptionFormatValue> &values);
};

template <>
SABOT_SQL_API ExceptionFormatValue ExceptionFormatValue::CreateFormatValue(PhysicalType value);
template <>
SABOT_SQL_API ExceptionFormatValue ExceptionFormatValue::CreateFormatValue(SQLString value);
template <>
SABOT_SQL_API ExceptionFormatValue ExceptionFormatValue::CreateFormatValue(SQLIdentifier value);
template <>
SABOT_SQL_API ExceptionFormatValue ExceptionFormatValue::CreateFormatValue(LogicalType value);
template <>
SABOT_SQL_API ExceptionFormatValue ExceptionFormatValue::CreateFormatValue(float value);
template <>
SABOT_SQL_API ExceptionFormatValue ExceptionFormatValue::CreateFormatValue(double value);
template <>
SABOT_SQL_API ExceptionFormatValue ExceptionFormatValue::CreateFormatValue(string value);
template <>
SABOT_SQL_API ExceptionFormatValue ExceptionFormatValue::CreateFormatValue(String value);
template <>
SABOT_SQL_API ExceptionFormatValue ExceptionFormatValue::CreateFormatValue(const char *value);
template <>
SABOT_SQL_API ExceptionFormatValue ExceptionFormatValue::CreateFormatValue(char *value);
template <>
SABOT_SQL_API ExceptionFormatValue ExceptionFormatValue::CreateFormatValue(idx_t value);
template <>
SABOT_SQL_API ExceptionFormatValue ExceptionFormatValue::CreateFormatValue(hugeint_t value);
template <>
SABOT_SQL_API ExceptionFormatValue ExceptionFormatValue::CreateFormatValue(uhugeint_t value);

} // namespace sabot_sql
