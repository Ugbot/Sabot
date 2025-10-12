//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/common/types/value.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/common/common.hpp"
#include "sabot_sql/common/exception.hpp"
#include "sabot_sql/common/types.hpp"
#include "sabot_sql/common/uhugeint.hpp"
#include "sabot_sql/common/winapi.hpp"
#include "sabot_sql/common/types/timestamp.hpp"
#include "sabot_sql/common/types/date.hpp"
#include "sabot_sql/common/types/datetime.hpp"
#include "sabot_sql/common/types/interval.hpp"
#include "sabot_sql/common/shared_ptr.hpp"
#include "sabot_sql/common/insertion_order_preserving_map.hpp"

namespace sabot_sql {

class String;
class CastFunctionSet;
struct GetCastFunctionInput;
struct ExtraValueInfo;

//! The Value object holds a single arbitrary value of any type that can be
//! stored in the database.
class Value {
	friend struct StringValue;
	friend struct StructValue;
	friend struct ListValue;
	friend struct UnionValue;
	friend struct ArrayValue;
	friend struct MapValue;

public:
	//! Create an empty NULL value of the specified type
	SABOT_SQL_API explicit Value(LogicalType type = LogicalType::SQLNULL);
	//! Create an INTEGER value
	SABOT_SQL_API Value(int32_t val); // NOLINT: Allow implicit conversion from `int32_t`
	//! Create a BOOLEAN value
	explicit SABOT_SQL_API Value(bool val);
	//! Create a BIGINT value
	SABOT_SQL_API Value(int64_t val); // NOLINT: Allow implicit conversion from `int64_t`
	//! Create a FLOAT value
	SABOT_SQL_API Value(float val); // NOLINT: Allow implicit conversion from `float`
	//! Create a DOUBLE value
	SABOT_SQL_API Value(double val); // NOLINT: Allow implicit conversion from `double`
	//! Create a VARCHAR value
	SABOT_SQL_API Value(const char *val); // NOLINT: Allow implicit conversion from `const char *`
	//! Create a NULL value
	SABOT_SQL_API Value(std::nullptr_t val); // NOLINT: Allow implicit conversion from `nullptr_t`
	//! Create a VARCHAR value
	SABOT_SQL_API Value(string_t val); // NOLINT: Allow implicit conversion from `string_t`
	//! Create a VARCHAR value
	SABOT_SQL_API Value(string val); // NOLINT: Allow implicit conversion from `string`
	//! Create a VARCHAR value
	SABOT_SQL_API Value(String val); // NOLINT: Allow implicit conversion from `string`
	//! Copy constructor
	SABOT_SQL_API Value(const Value &other);
	//! Move constructor
	SABOT_SQL_API Value(Value &&other) noexcept;
	//! Destructor
	SABOT_SQL_API ~Value();

	// copy assignment
	SABOT_SQL_API Value &operator=(const Value &other);
	// move assignment
	SABOT_SQL_API Value &operator=(Value &&other) noexcept;

	inline LogicalType &GetTypeMutable() {
		return type_;
	}
	inline const LogicalType &type() const { // NOLINT
		return type_;
	}
	inline bool IsNull() const {
		return is_null;
	}

	//! Create the lowest possible value of a given type (numeric only)
	SABOT_SQL_API static Value MinimumValue(const LogicalType &type);
	//! Create the highest possible value of a given type (numeric only)
	SABOT_SQL_API static Value MaximumValue(const LogicalType &type);
	//! Create the negative infinite value of a given type (numeric only)
	SABOT_SQL_API static Value NegativeInfinity(const LogicalType &type);
	//! Create the positive infinite value of a given type (numeric only)
	SABOT_SQL_API static Value Infinity(const LogicalType &type);
	//! Create a Numeric value of the specified type with the specified value
	SABOT_SQL_API static Value Numeric(const LogicalType &type, int64_t value);
	SABOT_SQL_API static Value Numeric(const LogicalType &type, hugeint_t value);
	SABOT_SQL_API static Value Numeric(const LogicalType &type, uhugeint_t value);

	//! Create a boolean Value from a specified value
	SABOT_SQL_API static Value BOOLEAN(bool value);
	//! Create a tinyint Value from a specified value
	SABOT_SQL_API static Value TINYINT(int8_t value);
	//! Create a smallint Value from a specified value
	SABOT_SQL_API static Value SMALLINT(int16_t value);
	//! Create an integer Value from a specified value
	SABOT_SQL_API static Value INTEGER(int32_t value);
	//! Create a bigint Value from a specified value
	SABOT_SQL_API static Value BIGINT(int64_t value);
	//! Create an unsigned tinyint Value from a specified value
	SABOT_SQL_API static Value UTINYINT(uint8_t value);
	//! Create an unsigned smallint Value from a specified value
	SABOT_SQL_API static Value USMALLINT(uint16_t value);
	//! Create an unsigned integer Value from a specified value
	SABOT_SQL_API static Value UINTEGER(uint32_t value);
	//! Create an unsigned bigint Value from a specified value
	SABOT_SQL_API static Value UBIGINT(uint64_t value);
	//! Create a hugeint Value from a specified value
	SABOT_SQL_API static Value HUGEINT(hugeint_t value);
	//! Create a uhugeint Value from a specified value
	SABOT_SQL_API static Value UHUGEINT(uhugeint_t value);
	//! Create a uuid Value from a specified value
	SABOT_SQL_API static Value UUID(const string &value);
	//! Create a uuid Value from a specified value
	SABOT_SQL_API static Value UUID(hugeint_t value);
	//! Create a hash Value from a specified value
	SABOT_SQL_API static Value HASH(hash_t value);
	//! Create a pointer Value from a specified value
	SABOT_SQL_API static Value POINTER(uintptr_t value);
	//! Create a date Value from a specified date
	SABOT_SQL_API static Value DATE(date_t date);
	//! Create a date Value from a specified date
	SABOT_SQL_API static Value DATE(int32_t year, int32_t month, int32_t day);
	//! Create a time Value from a specified time
	SABOT_SQL_API static Value TIME(dtime_t time);
	SABOT_SQL_API static Value TIME_NS(dtime_ns_t time);
	SABOT_SQL_API static Value TIMETZ(dtime_tz_t time);
	//! Create a time Value from a specified time
	SABOT_SQL_API static Value TIME(int32_t hour, int32_t min, int32_t sec, int32_t micros);
	//! Create a timestamp Value from a specified date/time combination.
	SABOT_SQL_API static Value TIMESTAMP(date_t date, dtime_t time);
	//! Create a timestamp Value from a specified value.
	SABOT_SQL_API static Value TIMESTAMP(timestamp_t timestamp);
	//! Create a timestamp_s Value from a specified value.
	SABOT_SQL_API static Value TIMESTAMPSEC(timestamp_sec_t timestamp);
	//! Create a timestamp_ms Value from a specified value.
	SABOT_SQL_API static Value TIMESTAMPMS(timestamp_ms_t timestamp);
	//! Create a timestamp_ns Value from a specified value.
	SABOT_SQL_API static Value TIMESTAMPNS(timestamp_ns_t timestamp);
	//! Create a timestamp_tz Value from a specified value.
	SABOT_SQL_API static Value TIMESTAMPTZ(timestamp_tz_t timestamp);
	//! Create a timestamp Value from a specified timestamp in separate values
	SABOT_SQL_API static Value TIMESTAMP(int32_t year, int32_t month, int32_t day, int32_t hour, int32_t min, int32_t sec,
	                                  int32_t micros);
	SABOT_SQL_API static Value INTERVAL(int32_t months, int32_t days, int64_t micros);
	SABOT_SQL_API static Value INTERVAL(interval_t interval);

	// Create a enum Value from a specified uint value
	SABOT_SQL_API static Value ENUM(uint64_t value, const LogicalType &original_type);

	// Decimal values
	SABOT_SQL_API static Value DECIMAL(int16_t value, uint8_t width, uint8_t scale);
	SABOT_SQL_API static Value DECIMAL(int32_t value, uint8_t width, uint8_t scale);
	SABOT_SQL_API static Value DECIMAL(int64_t value, uint8_t width, uint8_t scale);
	SABOT_SQL_API static Value DECIMAL(hugeint_t value, uint8_t width, uint8_t scale);
	//! Create a float Value from a specified value
	SABOT_SQL_API static Value FLOAT(float value);
	//! Create a double Value from a specified value
	SABOT_SQL_API static Value DOUBLE(double value);
	//! Create a struct value with given list of entries
	SABOT_SQL_API static Value STRUCT(child_list_t<Value> values);
	SABOT_SQL_API static Value STRUCT(const LogicalType &type, vector<Value> struct_values);
	//! Create a variant value with given list of internal variant data (keys/children/values/data)
	SABOT_SQL_API static Value VARIANT(vector<Value> children);
	//! Create a list value with the given entries
	SABOT_SQL_API static Value LIST(const LogicalType &child_type, vector<Value> values);
	//! Create a list value with the given entries
	//! The type of the first value determines the list type. The list cannot be empty.
	SABOT_SQL_API static Value LIST(vector<Value> values);
	// Create an array value with the given entries
	SABOT_SQL_API static Value ARRAY(const LogicalType &type, vector<Value> values);
	//! Create a map value with the given entries
	SABOT_SQL_API static Value MAP(const LogicalType &child_type, vector<Value> values);
	//! Create a map value with the given entries
	SABOT_SQL_API static Value MAP(const LogicalType &key_type, const LogicalType &value_type, vector<Value> keys,
	                            vector<Value> values);
	//! Create a map value from a set of key-value pairs
	SABOT_SQL_API static Value MAP(const InsertionOrderPreservingMap<string> &kv_pairs);

	//! Create a union value from a selected value and a tag from a set of alternatives.
	SABOT_SQL_API static Value UNION(child_list_t<LogicalType> members, uint8_t tag, Value value);

	//! Create a blob Value from a data pointer and a length: no bytes are interpreted
	SABOT_SQL_API static Value BLOB(const_data_ptr_t data, idx_t len);
	static Value BLOB_RAW(const string &data) { // NOLINT
		return Value::BLOB(const_data_ptr_cast(data.c_str()), data.size());
	}
	//! Creates a blob by casting a specified string to a blob (i.e. interpreting \x characters)
	SABOT_SQL_API static Value BLOB(const string &data);
	//! Creates a bitstring by casting a specified string to a bitstring
	SABOT_SQL_API static Value BIT(const_data_ptr_t data, idx_t len);
	SABOT_SQL_API static Value BIT(const string &data);
	SABOT_SQL_API static Value BIGNUM(const_data_ptr_t data, idx_t len);
	SABOT_SQL_API static Value BIGNUM(const string &data);

	SABOT_SQL_API static Value GEOMETRY(const_data_ptr_t data, idx_t len);

	//! Creates an aggregate state
	SABOT_SQL_API static Value AGGREGATE_STATE(const LogicalType &type, const_data_ptr_t data, idx_t len); // NOLINT

	template <class T>
	T GetValue() const;
	template <class T>
	static Value CreateValue(T value) {
		static_assert(AlwaysFalse<T>::VALUE, "No specialization exists for this type");
		return Value(nullptr);
	}
	// Returns the internal value. Unlike GetValue(), this method does not perform casting, and assumes T matches the
	// type of the value. Only use this if you know what you are doing.
	template <class T>
	T GetValueUnsafe() const;

	//! Return a copy of this value
	Value Copy() const {
		return Value(*this);
	}

	//! Hashes the Value
	SABOT_SQL_API hash_t Hash() const;
	//! Convert this value to a string
	SABOT_SQL_API string ToString() const;
	//! Convert this value to a SQL-parseable string
	SABOT_SQL_API string ToSQLString() const;

	SABOT_SQL_API uintptr_t GetPointer() const;

	//! Cast this value to another type, throws exception if its not possible
	SABOT_SQL_API Value CastAs(CastFunctionSet &set, GetCastFunctionInput &get_input, const LogicalType &target_type,
	                        bool strict = false) const;
	SABOT_SQL_API Value CastAs(ClientContext &context, const LogicalType &target_type, bool strict = false) const;
	SABOT_SQL_API Value DefaultCastAs(const LogicalType &target_type, bool strict = false) const;
	//! Tries to cast this value to another type, and stores the result in "new_value"
	SABOT_SQL_API bool TryCastAs(CastFunctionSet &set, GetCastFunctionInput &get_input, const LogicalType &target_type,
	                          Value &new_value, string *error_message, bool strict = false) const;
	SABOT_SQL_API bool TryCastAs(ClientContext &context, const LogicalType &target_type, Value &new_value,
	                          string *error_message, bool strict = false) const;
	SABOT_SQL_API bool DefaultTryCastAs(const LogicalType &target_type, Value &new_value, string *error_message,
	                                 bool strict = false) const;
	//! Tries to cast this value to another type, and stores the result in THIS value again
	SABOT_SQL_API bool TryCastAs(CastFunctionSet &set, GetCastFunctionInput &get_input, const LogicalType &target_type,
	                          bool strict = false);
	SABOT_SQL_API bool TryCastAs(ClientContext &context, const LogicalType &target_type, bool strict = false);
	SABOT_SQL_API bool DefaultTryCastAs(const LogicalType &target_type, bool strict = false);

	SABOT_SQL_API void Reinterpret(LogicalType new_type);

	//! Serializes a Value to a stand-alone binary blob
	SABOT_SQL_API void Serialize(Serializer &serializer) const;
	//! Deserializes a Value from a blob
	SABOT_SQL_API static Value Deserialize(Deserializer &deserializer);

	//===--------------------------------------------------------------------===//
	// Comparison Operators
	//===--------------------------------------------------------------------===//
	SABOT_SQL_API bool operator==(const Value &rhs) const;
	SABOT_SQL_API bool operator!=(const Value &rhs) const;
	SABOT_SQL_API bool operator<(const Value &rhs) const;
	SABOT_SQL_API bool operator>(const Value &rhs) const;
	SABOT_SQL_API bool operator<=(const Value &rhs) const;
	SABOT_SQL_API bool operator>=(const Value &rhs) const;

	SABOT_SQL_API bool operator==(const int64_t &rhs) const;
	SABOT_SQL_API bool operator!=(const int64_t &rhs) const;
	SABOT_SQL_API bool operator<(const int64_t &rhs) const;
	SABOT_SQL_API bool operator>(const int64_t &rhs) const;
	SABOT_SQL_API bool operator<=(const int64_t &rhs) const;
	SABOT_SQL_API bool operator>=(const int64_t &rhs) const;

	SABOT_SQL_API static bool FloatIsFinite(float value);
	SABOT_SQL_API static bool DoubleIsFinite(double value);
	template <class T>
	static bool IsNan(T value) {
		throw InternalException("Unimplemented template type for Value::IsNan");
	}
	template <class T>
	static bool IsFinite(T value) {
		return true;
	}
	SABOT_SQL_API static bool StringIsValid(const char *str, idx_t length);
	static bool StringIsValid(const string &str) {
		return StringIsValid(str.c_str(), str.size());
	}

	//! Returns true if the values are (approximately) equivalent. Note this is NOT the SQL equivalence. For this
	//! function, NULL values are equivalent and floating point values that are close are equivalent.
	SABOT_SQL_API static bool ValuesAreEqual(CastFunctionSet &set, GetCastFunctionInput &get_input,
	                                      const Value &result_value, const Value &value);
	SABOT_SQL_API static bool ValuesAreEqual(ClientContext &context, const Value &result_value, const Value &value);
	SABOT_SQL_API static bool DefaultValuesAreEqual(const Value &result_value, const Value &value);
	//! Returns true if the values are not distinct from each other, following SQL semantics for NOT DISTINCT FROM.
	SABOT_SQL_API static bool NotDistinctFrom(const Value &lvalue, const Value &rvalue);

	friend std::ostream &operator<<(std::ostream &out, const Value &val) {
		out << val.ToString();
		return out;
	}
	SABOT_SQL_API void Print() const;

private:
	void SerializeInternal(Serializer &serializer, bool serialize_type) const;
	static void SerializeChildren(Serializer &serializer, const vector<Value> &children,
	                              const LogicalType &parent_type);

private:
	//! The logical of the value
	LogicalType type_; // NOLINT

	//! Whether or not the value is NULL
	bool is_null;

	//! The value of the object, if it is of a constant size Type
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
		uintptr_t pointer;
		uint64_t hash;
		date_t date;
		dtime_t time;
		dtime_ns_t time_ns;
		dtime_tz_t timetz;
		timestamp_t timestamp;
		timestamp_sec_t timestamp_s;
		timestamp_ms_t timestamp_ms;
		timestamp_ns_t timestamp_ns;
		timestamp_tz_t timestamp_tz;
		interval_t interval;
	} value_; // NOLINT

	shared_ptr<ExtraValueInfo> value_info_; // NOLINT

private:
	template <class T>
	T GetValueInternal() const;
};

//===--------------------------------------------------------------------===//
// Type-specific getters
//===--------------------------------------------------------------------===//
// Note that these are equivalent to calling GetValueUnsafe<X>, meaning no cast will be performed
// instead, an assertion will be triggered if the value is not of the correct type
struct BooleanValue {
	SABOT_SQL_API static bool Get(const Value &value);
};

struct TinyIntValue {
	SABOT_SQL_API static int8_t Get(const Value &value);
};

struct SmallIntValue {
	SABOT_SQL_API static int16_t Get(const Value &value);
};

struct IntegerValue {
	SABOT_SQL_API static int32_t Get(const Value &value);
};

struct BigIntValue {
	SABOT_SQL_API static int64_t Get(const Value &value);
};

struct HugeIntValue {
	SABOT_SQL_API static hugeint_t Get(const Value &value);
};

struct UTinyIntValue {
	SABOT_SQL_API static uint8_t Get(const Value &value);
};

struct USmallIntValue {
	SABOT_SQL_API static uint16_t Get(const Value &value);
};

struct UIntegerValue {
	SABOT_SQL_API static uint32_t Get(const Value &value);
};

struct UBigIntValue {
	SABOT_SQL_API static uint64_t Get(const Value &value);
};

struct UhugeIntValue {
	SABOT_SQL_API static uhugeint_t Get(const Value &value);
};

struct FloatValue {
	SABOT_SQL_API static float Get(const Value &value);
};

struct DoubleValue {
	SABOT_SQL_API static double Get(const Value &value);
};

struct StringValue {
	SABOT_SQL_API static const string &Get(const Value &value);
};

struct DateValue {
	SABOT_SQL_API static date_t Get(const Value &value);
};

struct TimeValue {
	SABOT_SQL_API static dtime_t Get(const Value &value);
};

struct TimestampValue {
	SABOT_SQL_API static timestamp_t Get(const Value &value);
};

struct TimestampSValue {
	SABOT_SQL_API static timestamp_sec_t Get(const Value &value);
};

struct TimestampMSValue {
	SABOT_SQL_API static timestamp_ms_t Get(const Value &value);
};

struct TimestampNSValue {
	SABOT_SQL_API static timestamp_ns_t Get(const Value &value);
};

struct TimestampTZValue {
	SABOT_SQL_API static timestamp_tz_t Get(const Value &value);
};

struct IntervalValue {
	SABOT_SQL_API static interval_t Get(const Value &value);
};

struct StructValue {
	SABOT_SQL_API static const vector<Value> &GetChildren(const Value &value);
};

struct MapValue {
	SABOT_SQL_API static const vector<Value> &GetChildren(const Value &value);
};

struct ListValue {
	SABOT_SQL_API static const vector<Value> &GetChildren(const Value &value);
};

struct ArrayValue {
	SABOT_SQL_API static const vector<Value> &GetChildren(const Value &value);
};

struct UnionValue {
	SABOT_SQL_API static const Value &GetValue(const Value &value);
	SABOT_SQL_API static uint8_t GetTag(const Value &value);
	SABOT_SQL_API static const LogicalType &GetType(const Value &value);
};

//! Return the internal integral value for any type that is stored as an integral value internally
//! This can be used on values of type integer, uinteger, but also date, timestamp, decimal, etc
struct IntegralValue {
	static hugeint_t Get(const Value &value);
};

template <>
Value SABOT_SQL_API Value::CreateValue(bool value);
template <>
Value SABOT_SQL_API Value::CreateValue(uint8_t value);
template <>
Value SABOT_SQL_API Value::CreateValue(uint16_t value);
template <>
Value SABOT_SQL_API Value::CreateValue(uint32_t value);
template <>
Value SABOT_SQL_API Value::CreateValue(uint64_t value);
template <>
Value SABOT_SQL_API Value::CreateValue(int8_t value);
template <>
Value SABOT_SQL_API Value::CreateValue(int16_t value);
template <>
Value SABOT_SQL_API Value::CreateValue(int32_t value);
template <>
Value SABOT_SQL_API Value::CreateValue(int64_t value);
template <>
Value SABOT_SQL_API Value::CreateValue(hugeint_t value);
template <>
Value SABOT_SQL_API Value::CreateValue(uhugeint_t value);
template <>
Value SABOT_SQL_API Value::CreateValue(date_t value);
template <>
Value SABOT_SQL_API Value::CreateValue(dtime_t value);
template <>
Value SABOT_SQL_API Value::CreateValue(dtime_ns_t value);
template <>
Value SABOT_SQL_API Value::CreateValue(dtime_tz_t value);
template <>
Value SABOT_SQL_API Value::CreateValue(timestamp_t value);
template <>
Value SABOT_SQL_API Value::CreateValue(timestamp_sec_t value);
template <>
Value SABOT_SQL_API Value::CreateValue(timestamp_ms_t value);
template <>
Value SABOT_SQL_API Value::CreateValue(timestamp_ns_t value);
template <>
Value SABOT_SQL_API Value::CreateValue(timestamp_tz_t value);
template <>
Value SABOT_SQL_API Value::CreateValue(const char *value);
template <>
Value SABOT_SQL_API Value::CreateValue(string value);
template <>
Value SABOT_SQL_API Value::CreateValue(string_t value);
template <>
Value SABOT_SQL_API Value::CreateValue(float value);
template <>
Value SABOT_SQL_API Value::CreateValue(double value);
template <>
Value SABOT_SQL_API Value::CreateValue(interval_t value);
template <>
Value SABOT_SQL_API Value::CreateValue(Value value);

template <>
SABOT_SQL_API bool Value::GetValue() const;
template <>
SABOT_SQL_API int8_t Value::GetValue() const;
template <>
SABOT_SQL_API int16_t Value::GetValue() const;
template <>
SABOT_SQL_API int32_t Value::GetValue() const;
template <>
SABOT_SQL_API int64_t Value::GetValue() const;
template <>
SABOT_SQL_API uint8_t Value::GetValue() const;
template <>
SABOT_SQL_API uint16_t Value::GetValue() const;
template <>
SABOT_SQL_API uint32_t Value::GetValue() const;
template <>
SABOT_SQL_API uint64_t Value::GetValue() const;
template <>
SABOT_SQL_API hugeint_t Value::GetValue() const;
template <>
SABOT_SQL_API uhugeint_t Value::GetValue() const;
template <>
SABOT_SQL_API string Value::GetValue() const;
template <>
SABOT_SQL_API float Value::GetValue() const;
template <>
SABOT_SQL_API double Value::GetValue() const;
template <>
SABOT_SQL_API date_t Value::GetValue() const;
template <>
SABOT_SQL_API dtime_t Value::GetValue() const;
template <>
SABOT_SQL_API dtime_ns_t Value::GetValue() const;
template <>
SABOT_SQL_API dtime_tz_t Value::GetValue() const;
template <>
SABOT_SQL_API timestamp_t Value::GetValue() const;
template <>
SABOT_SQL_API timestamp_sec_t Value::GetValue() const;
template <>
SABOT_SQL_API timestamp_ms_t Value::GetValue() const;
template <>
SABOT_SQL_API timestamp_ns_t Value::GetValue() const;
template <>
SABOT_SQL_API timestamp_tz_t Value::GetValue() const;
template <>
SABOT_SQL_API interval_t Value::GetValue() const;
template <>
SABOT_SQL_API Value Value::GetValue() const;

template <>
SABOT_SQL_API bool Value::GetValueUnsafe() const;
template <>
SABOT_SQL_API int8_t Value::GetValueUnsafe() const;
template <>
SABOT_SQL_API int16_t Value::GetValueUnsafe() const;
template <>
SABOT_SQL_API int32_t Value::GetValueUnsafe() const;
template <>
SABOT_SQL_API int64_t Value::GetValueUnsafe() const;
template <>
SABOT_SQL_API hugeint_t Value::GetValueUnsafe() const;
template <>
SABOT_SQL_API uhugeint_t Value::GetValueUnsafe() const;
template <>
SABOT_SQL_API uint8_t Value::GetValueUnsafe() const;
template <>
SABOT_SQL_API uint16_t Value::GetValueUnsafe() const;
template <>
SABOT_SQL_API uint32_t Value::GetValueUnsafe() const;
template <>
SABOT_SQL_API uint64_t Value::GetValueUnsafe() const;
template <>
SABOT_SQL_API string Value::GetValueUnsafe() const;
template <>
SABOT_SQL_API string_t Value::GetValueUnsafe() const;
template <>
SABOT_SQL_API float Value::GetValueUnsafe() const;
template <>
SABOT_SQL_API double Value::GetValueUnsafe() const;
template <>
SABOT_SQL_API date_t Value::GetValueUnsafe() const;
template <>
SABOT_SQL_API dtime_t Value::GetValueUnsafe() const;
template <>
SABOT_SQL_API dtime_ns_t Value::GetValueUnsafe() const;
template <>
SABOT_SQL_API dtime_tz_t Value::GetValueUnsafe() const;
template <>
SABOT_SQL_API timestamp_t Value::GetValueUnsafe() const;
template <>
SABOT_SQL_API timestamp_sec_t Value::GetValueUnsafe() const;
template <>
SABOT_SQL_API timestamp_ms_t Value::GetValueUnsafe() const;
template <>
SABOT_SQL_API timestamp_ns_t Value::GetValueUnsafe() const;
template <>
SABOT_SQL_API timestamp_tz_t Value::GetValueUnsafe() const;
template <>
SABOT_SQL_API interval_t Value::GetValueUnsafe() const;

template <>
SABOT_SQL_API bool Value::IsNan(float input);
template <>
SABOT_SQL_API bool Value::IsNan(double input);

template <>
SABOT_SQL_API bool Value::IsFinite(float input);
template <>
SABOT_SQL_API bool Value::IsFinite(double input);
template <>
SABOT_SQL_API bool Value::IsFinite(date_t input);
template <>
SABOT_SQL_API bool Value::IsFinite(timestamp_t input);
template <>
SABOT_SQL_API bool Value::IsFinite(timestamp_sec_t input);
template <>
SABOT_SQL_API bool Value::IsFinite(timestamp_ms_t input);
template <>
SABOT_SQL_API bool Value::IsFinite(timestamp_ns_t input);
template <>
SABOT_SQL_API bool Value::IsFinite(timestamp_tz_t input);

} // namespace sabot_sql
