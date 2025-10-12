#include "sabot_sql/common/hugeint.hpp"
#include "sabot_sql/common/type_visitor.hpp"
#include "sabot_sql/common/types.hpp"
#include "sabot_sql/common/types/null_value.hpp"
#include "sabot_sql/common/types/string_type.hpp"
#include "sabot_sql/common/types/uuid.hpp"
#include "sabot_sql/common/types/value.hpp"
#include "sabot_sql/common/types/bignum.hpp"
#include "sabot_sql/main/capi/capi_internal.hpp"

using sabot_sql::LogicalTypeId;

static sabot_sql_value WrapValue(sabot_sql::Value *value) {
	return reinterpret_cast<sabot_sql_value>(value);
}

static sabot_sql::LogicalType &UnwrapType(sabot_sql_logical_type type) {
	return *(reinterpret_cast<sabot_sql::LogicalType *>(type));
}

static sabot_sql::Value &UnwrapValue(sabot_sql_value value) {
	return *(reinterpret_cast<sabot_sql::Value *>(value));
}
void sabot_sql_destroy_value(sabot_sql_value *value) {
	if (value && *value) {
		auto &unwrap_value = UnwrapValue(*value);
		delete &unwrap_value;
		*value = nullptr;
	}
}

sabot_sql_value sabot_sql_create_varchar_length(const char *text, idx_t length) {
	return WrapValue(new sabot_sql::Value(std::string(text, length)));
}

sabot_sql_value sabot_sql_create_varchar(const char *text) {
	return sabot_sql_create_varchar_length(text, strlen(text));
}

template <class T>
static sabot_sql_value CAPICreateValue(T input) {
	return WrapValue(new sabot_sql::Value(sabot_sql::Value::CreateValue<T>(input)));
}

template <class T, LogicalTypeId TYPE_ID>
static T CAPIGetValue(sabot_sql_value val) {
	auto &v = UnwrapValue(val);
	if (!v.DefaultTryCastAs(TYPE_ID)) {
		return sabot_sql::NullValue<T>();
	}
	return v.GetValue<T>();
}

sabot_sql_value sabot_sql_create_bool(bool input) {
	return CAPICreateValue(input);
}
bool sabot_sql_get_bool(sabot_sql_value val) {
	return CAPIGetValue<bool, LogicalTypeId::BOOLEAN>(val);
}
sabot_sql_value sabot_sql_create_int8(int8_t input) {
	return CAPICreateValue(input);
}
int8_t sabot_sql_get_int8(sabot_sql_value val) {
	return CAPIGetValue<int8_t, LogicalTypeId::TINYINT>(val);
}
sabot_sql_value sabot_sql_create_uint8(uint8_t input) {
	return CAPICreateValue(input);
}
uint8_t sabot_sql_get_uint8(sabot_sql_value val) {
	return CAPIGetValue<uint8_t, LogicalTypeId::UTINYINT>(val);
}
sabot_sql_value sabot_sql_create_int16(int16_t input) {
	return CAPICreateValue(input);
}
int16_t sabot_sql_get_int16(sabot_sql_value val) {
	return CAPIGetValue<int16_t, LogicalTypeId::SMALLINT>(val);
}
sabot_sql_value sabot_sql_create_uint16(uint16_t input) {
	return CAPICreateValue(input);
}
uint16_t sabot_sql_get_uint16(sabot_sql_value val) {
	return CAPIGetValue<uint16_t, LogicalTypeId::USMALLINT>(val);
}
sabot_sql_value sabot_sql_create_int32(int32_t input) {
	return CAPICreateValue(input);
}
int32_t sabot_sql_get_int32(sabot_sql_value val) {
	return CAPIGetValue<int32_t, LogicalTypeId::INTEGER>(val);
}
sabot_sql_value sabot_sql_create_uint32(uint32_t input) {
	return CAPICreateValue(input);
}
uint32_t sabot_sql_get_uint32(sabot_sql_value val) {
	return CAPIGetValue<uint32_t, LogicalTypeId::UINTEGER>(val);
}
sabot_sql_value sabot_sql_create_uint64(uint64_t input) {
	return CAPICreateValue(input);
}
uint64_t sabot_sql_get_uint64(sabot_sql_value val) {
	return CAPIGetValue<uint64_t, LogicalTypeId::UBIGINT>(val);
}
sabot_sql_value sabot_sql_create_int64(int64_t input) {
	return CAPICreateValue(input);
}
int64_t sabot_sql_get_int64(sabot_sql_value val) {
	return CAPIGetValue<int64_t, LogicalTypeId::BIGINT>(val);
}
sabot_sql_value sabot_sql_create_hugeint(sabot_sql_hugeint input) {
	return WrapValue(new sabot_sql::Value(sabot_sql::Value::HUGEINT(sabot_sql::hugeint_t(input.upper, input.lower))));
}
sabot_sql_hugeint sabot_sql_get_hugeint(sabot_sql_value val) {
	auto res = CAPIGetValue<sabot_sql::hugeint_t, LogicalTypeId::HUGEINT>(val);
	return {res.lower, res.upper};
}
sabot_sql_value sabot_sql_create_uhugeint(sabot_sql_uhugeint input) {
	return WrapValue(new sabot_sql::Value(sabot_sql::Value::UHUGEINT(sabot_sql::uhugeint_t(input.upper, input.lower))));
}
sabot_sql_uhugeint sabot_sql_get_uhugeint(sabot_sql_value val) {
	auto res = CAPIGetValue<sabot_sql::uhugeint_t, LogicalTypeId::UHUGEINT>(val);
	return {res.lower, res.upper};
}
sabot_sql_value sabot_sql_create_bignum(sabot_sql_bignum input) {
	return WrapValue(new sabot_sql::Value(
	    sabot_sql::Value::BIGNUM(sabot_sql::Bignum::FromByteArray(input.data, input.size, input.is_negative))));
}
sabot_sql_bignum sabot_sql_get_bignum(sabot_sql_value val) {
	auto v = UnwrapValue(val).DefaultCastAs(sabot_sql::LogicalType::BIGNUM);
	auto &str = sabot_sql::StringValue::Get(v);
	sabot_sql::vector<uint8_t> byte_array;
	bool is_negative;
	sabot_sql::Bignum::GetByteArray(byte_array, is_negative, sabot_sql::string_t(str));
	auto size = byte_array.size();
	auto data = reinterpret_cast<uint8_t *>(malloc(size));
	memcpy(data, byte_array.data(), size);
	return {data, size, is_negative};
}
sabot_sql_value sabot_sql_create_decimal(sabot_sql_decimal input) {
	sabot_sql::hugeint_t hugeint(input.value.upper, input.value.lower);
	int64_t int64;
	if (sabot_sql::Hugeint::TryCast<int64_t>(hugeint, int64)) {
		// The int64 DECIMAL value constructor will select the appropriate physical type based on width.
		return WrapValue(new sabot_sql::Value(sabot_sql::Value::DECIMAL(int64, input.width, input.scale)));
	} else {
		// The hugeint DECIMAL value constructor always uses a physical hugeint, and requires width >= MAX_WIDTH_INT64.
		return WrapValue(new sabot_sql::Value(sabot_sql::Value::DECIMAL(hugeint, input.width, input.scale)));
	}
}
sabot_sql_decimal sabot_sql_get_decimal(sabot_sql_value val) {
	auto &v = UnwrapValue(val);
	auto &type = v.type();
	if (type.id() != LogicalTypeId::DECIMAL) {
		return {0, 0, {0, 0}};
	}
	auto width = sabot_sql::DecimalType::GetWidth(type);
	auto scale = sabot_sql::DecimalType::GetScale(type);
	sabot_sql::hugeint_t hugeint = sabot_sql::IntegralValue::Get(v);
	return {width, scale, {hugeint.lower, hugeint.upper}};
}
sabot_sql_value sabot_sql_create_float(float input) {
	return CAPICreateValue(input);
}
float sabot_sql_get_float(sabot_sql_value val) {
	return CAPIGetValue<float, LogicalTypeId::FLOAT>(val);
}
sabot_sql_value sabot_sql_create_double(double input) {
	return CAPICreateValue(input);
}
double sabot_sql_get_double(sabot_sql_value val) {
	return CAPIGetValue<double, LogicalTypeId::DOUBLE>(val);
}
sabot_sql_value sabot_sql_create_date(sabot_sql_date input) {
	return CAPICreateValue(sabot_sql::date_t(input.days));
}
sabot_sql_date sabot_sql_get_date(sabot_sql_value val) {
	return {CAPIGetValue<sabot_sql::date_t, LogicalTypeId::DATE>(val).days};
}
sabot_sql_value sabot_sql_create_time(sabot_sql_time input) {
	return CAPICreateValue(sabot_sql::dtime_t(input.micros));
}
sabot_sql_time sabot_sql_get_time(sabot_sql_value val) {
	return {CAPIGetValue<sabot_sql::dtime_t, LogicalTypeId::TIME>(val).micros};
}
sabot_sql_value sabot_sql_create_time_tz_value(sabot_sql_time_tz input) {
	return CAPICreateValue(sabot_sql::dtime_tz_t(input.bits));
}
sabot_sql_time_tz sabot_sql_get_time_tz(sabot_sql_value val) {
	return {CAPIGetValue<sabot_sql::dtime_tz_t, LogicalTypeId::TIME_TZ>(val).bits};
}
sabot_sql_value sabot_sql_create_time_ns(sabot_sql_time_ns input) {
	return CAPICreateValue(sabot_sql::dtime_ns_t(input.nanos));
}
sabot_sql_time_ns sabot_sql_get_time_ns(sabot_sql_value val) {
	return {CAPIGetValue<sabot_sql::dtime_ns_t, LogicalTypeId::TIME_NS>(val).micros};
}

sabot_sql_value sabot_sql_create_timestamp(sabot_sql_timestamp input) {
	sabot_sql::timestamp_t ts(input.micros);
	return CAPICreateValue(ts);
}

sabot_sql_timestamp sabot_sql_get_timestamp(sabot_sql_value val) {
	if (!val) {
		return {0};
	}
	return {CAPIGetValue<sabot_sql::timestamp_t, LogicalTypeId::TIMESTAMP>(val).value};
}

sabot_sql_value sabot_sql_create_timestamp_tz(sabot_sql_timestamp input) {
	sabot_sql::timestamp_tz_t ts(input.micros);
	return CAPICreateValue(ts);
}

sabot_sql_timestamp sabot_sql_get_timestamp_tz(sabot_sql_value val) {
	if (!val) {
		return {0};
	}
	return {CAPIGetValue<sabot_sql::timestamp_tz_t, LogicalTypeId::TIMESTAMP_TZ>(val).value};
}

sabot_sql_value sabot_sql_create_timestamp_s(sabot_sql_timestamp_s input) {
	sabot_sql::timestamp_sec_t ts(input.seconds);
	return CAPICreateValue(ts);
}

sabot_sql_timestamp_s sabot_sql_get_timestamp_s(sabot_sql_value val) {
	if (!val) {
		return {0};
	}
	return {CAPIGetValue<sabot_sql::timestamp_sec_t, LogicalTypeId::TIMESTAMP_SEC>(val).value};
}

sabot_sql_value sabot_sql_create_timestamp_ms(sabot_sql_timestamp_ms input) {
	sabot_sql::timestamp_ms_t ts(input.millis);
	return CAPICreateValue(ts);
}

sabot_sql_timestamp_ms sabot_sql_get_timestamp_ms(sabot_sql_value val) {
	if (!val) {
		return {0};
	}
	return {CAPIGetValue<sabot_sql::timestamp_ms_t, LogicalTypeId::TIMESTAMP_MS>(val).value};
}

sabot_sql_value sabot_sql_create_timestamp_ns(sabot_sql_timestamp_ns input) {
	sabot_sql::timestamp_ns_t ts(input.nanos);
	return CAPICreateValue(ts);
}

sabot_sql_timestamp_ns sabot_sql_get_timestamp_ns(sabot_sql_value val) {
	if (!val) {
		return {0};
	}
	return {CAPIGetValue<sabot_sql::timestamp_ns_t, LogicalTypeId::TIMESTAMP_NS>(val).value};
}

sabot_sql_value sabot_sql_create_interval(sabot_sql_interval input) {
	return WrapValue(new sabot_sql::Value(sabot_sql::Value::INTERVAL(input.months, input.days, input.micros)));
}
sabot_sql_interval sabot_sql_get_interval(sabot_sql_value val) {
	auto interval = CAPIGetValue<sabot_sql::interval_t, LogicalTypeId::INTERVAL>(val);
	return {interval.months, interval.days, interval.micros};
}
sabot_sql_value sabot_sql_create_blob(const uint8_t *data, idx_t length) {
	return WrapValue(new sabot_sql::Value(sabot_sql::Value::BLOB((const uint8_t *)data, length)));
}
sabot_sql_blob sabot_sql_get_blob(sabot_sql_value val) {
	auto res = UnwrapValue(val).DefaultCastAs(sabot_sql::LogicalType::BLOB);
	auto &str = sabot_sql::StringValue::Get(res);

	auto result = reinterpret_cast<void *>(malloc(sizeof(char) * str.size()));
	memcpy(result, str.c_str(), str.size());
	return {result, str.size()};
}
sabot_sql_value sabot_sql_create_bit(sabot_sql_bit input) {
	return WrapValue(new sabot_sql::Value(sabot_sql::Value::BIT(input.data, input.size)));
}
sabot_sql_bit sabot_sql_get_bit(sabot_sql_value val) {
	auto v = UnwrapValue(val).DefaultCastAs(sabot_sql::LogicalType::BIT);
	auto &str = sabot_sql::StringValue::Get(v);
	auto size = str.size();
	auto data = reinterpret_cast<uint8_t *>(malloc(size));
	memcpy(data, str.c_str(), size);
	return {data, size};
}
sabot_sql_value sabot_sql_create_uuid(sabot_sql_uhugeint input) {
	// uhugeint_t has a constexpr ctor with upper first
	return WrapValue(new sabot_sql::Value(sabot_sql::Value::UUID(sabot_sql::UUID::FromUHugeint({input.upper, input.lower}))));
}
sabot_sql_uhugeint sabot_sql_get_uuid(sabot_sql_value val) {
	auto hugeint = CAPIGetValue<sabot_sql::hugeint_t, LogicalTypeId::UUID>(val);
	auto uhugeint = sabot_sql::UUID::ToUHugeint(hugeint);
	// sabot_sql_uhugeint has no constexpr ctor; struct is lower first
	return {uhugeint.lower, uhugeint.upper};
}

sabot_sql_logical_type sabot_sql_get_value_type(sabot_sql_value val) {
	auto &type = UnwrapValue(val).type();
	return (sabot_sql_logical_type)(&type);
}

char *sabot_sql_get_varchar(sabot_sql_value value) {
	auto val = reinterpret_cast<sabot_sql::Value *>(value);
	auto str_val = val->DefaultCastAs(sabot_sql::LogicalType::VARCHAR);
	auto &str = sabot_sql::StringValue::Get(str_val);

	auto result = reinterpret_cast<char *>(malloc(sizeof(char) * (str.size() + 1)));
	memcpy(result, str.c_str(), str.size());
	result[str.size()] = '\0';
	return result;
}
sabot_sql_value sabot_sql_create_struct_value(sabot_sql_logical_type type, sabot_sql_value *values) {
	if (!type || !values) {
		return nullptr;
	}
	const auto &logical_type = UnwrapType(type);
	if (logical_type.id() != sabot_sql::LogicalTypeId::STRUCT) {
		return nullptr;
	}
	if (sabot_sql::TypeVisitor::Contains(logical_type, sabot_sql::LogicalTypeId::INVALID) ||
	    sabot_sql::TypeVisitor::Contains(logical_type, sabot_sql::LogicalTypeId::ANY)) {
		return nullptr;
	}

	auto count = sabot_sql::StructType::GetChildCount(logical_type);
	sabot_sql::vector<sabot_sql::Value> unwrapped_values;
	for (idx_t i = 0; i < count; i++) {
		auto value = values[i];
		if (!value) {
			return nullptr;
		}
		unwrapped_values.emplace_back(UnwrapValue(value));
	}
	sabot_sql::Value *struct_value = new sabot_sql::Value;
	try {
		*struct_value = sabot_sql::Value::STRUCT(logical_type, std::move(unwrapped_values));
	} catch (...) {
		delete struct_value;
		return nullptr;
	}
	return WrapValue(struct_value);
}

sabot_sql_value sabot_sql_create_list_value(sabot_sql_logical_type type, sabot_sql_value *values, idx_t value_count) {
	if (!type || !values) {
		return nullptr;
	}
	auto &logical_type = UnwrapType(type);
	sabot_sql::vector<sabot_sql::Value> unwrapped_values;
	if (sabot_sql::TypeVisitor::Contains(logical_type, sabot_sql::LogicalTypeId::INVALID) ||
	    sabot_sql::TypeVisitor::Contains(logical_type, sabot_sql::LogicalTypeId::ANY)) {
		return nullptr;
	}

	for (idx_t i = 0; i < value_count; i++) {
		auto value = values[i];
		if (!value) {
			return nullptr;
		}
		unwrapped_values.push_back(UnwrapValue(value));
	}
	auto list_value = new sabot_sql::Value;
	try {
		*list_value = sabot_sql::Value::LIST(logical_type, std::move(unwrapped_values));
	} catch (...) {
		delete list_value;
		return nullptr;
	}
	return WrapValue(list_value);
}

sabot_sql_value sabot_sql_create_array_value(sabot_sql_logical_type type, sabot_sql_value *values, idx_t value_count) {
	if (!type || !values) {
		return nullptr;
	}
	if (value_count >= sabot_sql::ArrayType::MAX_ARRAY_SIZE) {
		return nullptr;
	}
	auto &logical_type = UnwrapType(type);
	if (sabot_sql::TypeVisitor::Contains(logical_type, sabot_sql::LogicalTypeId::INVALID) ||
	    sabot_sql::TypeVisitor::Contains(logical_type, sabot_sql::LogicalTypeId::ANY)) {
		return nullptr;
	}
	sabot_sql::vector<sabot_sql::Value> unwrapped_values;

	for (idx_t i = 0; i < value_count; i++) {
		auto value = values[i];
		if (!value) {
			return nullptr;
		}
		unwrapped_values.push_back(UnwrapValue(value));
	}
	sabot_sql::Value *array_value = new sabot_sql::Value;
	try {
		*array_value = sabot_sql::Value::ARRAY(logical_type, std::move(unwrapped_values));
	} catch (...) {
		delete array_value;
		return nullptr;
	}
	return WrapValue(array_value);
}

sabot_sql_value sabot_sql_create_map_value(sabot_sql_logical_type map_type, sabot_sql_value *keys, sabot_sql_value *values,
                                     idx_t entry_count) {
	if (!map_type || !keys || !values) {
		return nullptr;
	}
	const auto &map_logical_type = UnwrapType(map_type);
	if (map_logical_type.id() != sabot_sql::LogicalTypeId::MAP) {
		return nullptr;
	}
	if (sabot_sql::TypeVisitor::Contains(map_logical_type, sabot_sql::LogicalTypeId::INVALID) ||
	    sabot_sql::TypeVisitor::Contains(map_logical_type, sabot_sql::LogicalTypeId::ANY)) {
		return nullptr;
	}

	const auto &key_logical_type = sabot_sql::MapType::KeyType(map_logical_type);
	const auto &value_logical_type = sabot_sql::MapType::ValueType(map_logical_type);
	sabot_sql::vector<sabot_sql::Value> unwrapped_keys;
	sabot_sql::vector<sabot_sql::Value> unwrapped_values;
	for (idx_t i = 0; i < entry_count; i++) {
		const auto key = keys[i];
		const auto value = values[i];
		if (!key || !value) {
			return nullptr;
		}
		unwrapped_keys.emplace_back(UnwrapValue(key));
		unwrapped_values.emplace_back(UnwrapValue(value));
	}
	sabot_sql::Value *map_value = new sabot_sql::Value;
	try {
		*map_value = sabot_sql::Value::MAP(key_logical_type, value_logical_type, std::move(unwrapped_keys),
		                                std::move(unwrapped_values));
	} catch (...) {
		delete map_value;
		return nullptr;
	}
	return WrapValue(map_value);
}

sabot_sql_value sabot_sql_create_union_value(sabot_sql_logical_type union_type, idx_t tag_index, sabot_sql_value value) {
	if (!union_type || !value) {
		return nullptr;
	}
	const auto &union_logical_type = UnwrapType(union_type);
	if (union_logical_type.id() != sabot_sql::LogicalTypeId::UNION) {
		return nullptr;
	}
	idx_t member_count = sabot_sql::UnionType::GetMemberCount(union_logical_type);
	if (tag_index >= member_count) {
		return nullptr;
	}
	const auto &member_type = sabot_sql::UnionType::GetMemberType(union_logical_type, tag_index);
	const auto &unwrapped_value = UnwrapValue(value);
	if (unwrapped_value.type() != member_type) {
		return nullptr;
	}
	const auto member_types = sabot_sql::UnionType::CopyMemberTypes(union_logical_type);
	sabot_sql::Value *union_value = new sabot_sql::Value;
	try {
		*union_value = sabot_sql::Value::UNION(member_types, sabot_sql::NumericCast<uint8_t>(tag_index), unwrapped_value);
	} catch (...) {
		delete union_value;
		return nullptr;
	}
	return WrapValue(union_value);
}

idx_t sabot_sql_get_map_size(sabot_sql_value value) {
	if (!value) {
		return 0;
	}

	auto val = UnwrapValue(value);
	if (val.type().id() != LogicalTypeId::MAP || val.IsNull()) {
		return 0;
	}

	auto &children = sabot_sql::MapValue::GetChildren(val);
	return children.size();
}

sabot_sql_value sabot_sql_get_map_key(sabot_sql_value value, idx_t index) {
	if (!value) {
		return nullptr;
	}

	auto val = UnwrapValue(value);
	if (val.type().id() != LogicalTypeId::MAP || val.IsNull()) {
		return nullptr;
	}

	auto &children = sabot_sql::MapValue::GetChildren(val);
	if (index >= children.size()) {
		return nullptr;
	}

	auto &child = children[index];
	auto &child_struct = sabot_sql::StructValue::GetChildren(child);
	return WrapValue(new sabot_sql::Value(child_struct[0]));
}

sabot_sql_value sabot_sql_get_map_value(sabot_sql_value value, idx_t index) {
	if (!value) {
		return nullptr;
	}

	auto val = UnwrapValue(value);
	if (val.type().id() != LogicalTypeId::MAP || val.IsNull()) {
		return nullptr;
	}

	auto &children = sabot_sql::MapValue::GetChildren(val);
	if (index >= children.size()) {
		return nullptr;
	}

	auto &child = children[index];
	auto &child_struct = sabot_sql::StructValue::GetChildren(child);
	return WrapValue(new sabot_sql::Value(child_struct[1]));
}

bool sabot_sql_is_null_value(sabot_sql_value value) {
	if (!value) {
		return false;
	}
	return UnwrapValue(value).IsNull();
}

sabot_sql_value sabot_sql_create_null_value() {
	return WrapValue(new sabot_sql::Value());
}

idx_t sabot_sql_get_list_size(sabot_sql_value value) {
	if (!value) {
		return 0;
	}

	auto val = UnwrapValue(value);
	if (val.type().id() != LogicalTypeId::LIST || val.IsNull()) {
		return 0;
	}

	auto &children = sabot_sql::ListValue::GetChildren(val);
	return children.size();
}

sabot_sql_value sabot_sql_get_list_child(sabot_sql_value value, idx_t index) {
	if (!value) {
		return nullptr;
	}

	auto val = UnwrapValue(value);
	if (val.type().id() != LogicalTypeId::LIST || val.IsNull()) {
		return nullptr;
	}

	auto &children = sabot_sql::ListValue::GetChildren(val);
	if (index >= children.size()) {
		return nullptr;
	}

	return WrapValue(new sabot_sql::Value(children[index]));
}

sabot_sql_value sabot_sql_create_enum_value(sabot_sql_logical_type type, uint64_t value) {
	if (!type) {
		return nullptr;
	}

	auto &logical_type = UnwrapType(type);
	if (logical_type.id() != LogicalTypeId::ENUM) {
		return nullptr;
	}

	if (value >= sabot_sql::EnumType::GetSize(logical_type)) {
		return nullptr;
	}

	return WrapValue(new sabot_sql::Value(sabot_sql::Value::ENUM(value, logical_type)));
}

uint64_t sabot_sql_get_enum_value(sabot_sql_value value) {
	if (!value) {
		return 0;
	}

	auto val = UnwrapValue(value);
	if (val.type().id() != LogicalTypeId::ENUM || val.IsNull()) {
		return 0;
	}

	return val.GetValue<uint64_t>();
}

sabot_sql_value sabot_sql_get_struct_child(sabot_sql_value value, idx_t index) {
	if (!value) {
		return nullptr;
	}

	auto val = UnwrapValue(value);
	if (val.type().id() != LogicalTypeId::STRUCT || val.IsNull()) {
		return nullptr;
	}

	auto &children = sabot_sql::StructValue::GetChildren(val);
	if (index >= children.size()) {
		return nullptr;
	}

	return WrapValue(new sabot_sql::Value(children[index]));
}

char *sabot_sql_value_to_string(sabot_sql_value val) {
	if (!val) {
		return nullptr;
	}

	auto v = UnwrapValue(val);
	auto str = v.ToSQLString();

	auto result = reinterpret_cast<char *>(malloc(sizeof(char) * (str.size() + 1)));
	memcpy(result, str.c_str(), str.size());
	result[str.size()] = '\0';
	return result;
}
