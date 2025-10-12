#include "sabot_sql/main/capi/capi_internal.hpp"
#include "sabot_sql/common/types/date.hpp"
#include "sabot_sql/common/types/time.hpp"
#include "sabot_sql/common/types/timestamp.hpp"
#include "sabot_sql/common/types.hpp"
#include "sabot_sql/common/uhugeint.hpp"

#include "sabot_sql/main/capi/cast/generic.hpp"

#include <cstring>

using sabot_sql::date_t;
using sabot_sql::dtime_t;
using sabot_sql::FetchDefaultValue;
using sabot_sql::GetInternalCValue;
using sabot_sql::hugeint_t;
using sabot_sql::interval_t;
using sabot_sql::StringCast;
using sabot_sql::timestamp_t;
using sabot_sql::ToCStringCastWrapper;
using sabot_sql::uhugeint_t;
using sabot_sql::UnsafeFetch;

bool sabot_sql_value_boolean(sabot_sql_result *result, idx_t col, idx_t row) {
	return GetInternalCValue<bool>(result, col, row);
}

int8_t sabot_sql_value_int8(sabot_sql_result *result, idx_t col, idx_t row) {
	return GetInternalCValue<int8_t>(result, col, row);
}

int16_t sabot_sql_value_int16(sabot_sql_result *result, idx_t col, idx_t row) {
	return GetInternalCValue<int16_t>(result, col, row);
}

int32_t sabot_sql_value_int32(sabot_sql_result *result, idx_t col, idx_t row) {
	return GetInternalCValue<int32_t>(result, col, row);
}

int64_t sabot_sql_value_int64(sabot_sql_result *result, idx_t col, idx_t row) {
	return GetInternalCValue<int64_t>(result, col, row);
}

static bool ResultIsDecimal(sabot_sql_result *result, idx_t col) {
	if (!result) {
		return false;
	}
	if (!result->internal_data) {
		return false;
	}
	auto result_data = (sabot_sql::SabotSQLResultData *)result->internal_data;
	auto &query_result = result_data->result;
	auto &source_type = query_result->types[col];
	return source_type.id() == sabot_sql::LogicalTypeId::DECIMAL;
}

sabot_sql_decimal sabot_sql_value_decimal(sabot_sql_result *result, idx_t col, idx_t row) {
	if (!CanFetchValue(result, col, row) || !ResultIsDecimal(result, col)) {
		return FetchDefaultValue::Operation<sabot_sql_decimal>();
	}

	return GetInternalCValue<sabot_sql_decimal>(result, col, row);
}

sabot_sql_hugeint sabot_sql_value_hugeint(sabot_sql_result *result, idx_t col, idx_t row) {
	sabot_sql_hugeint result_value;
	auto internal_value = GetInternalCValue<hugeint_t>(result, col, row);
	result_value.lower = internal_value.lower;
	result_value.upper = internal_value.upper;
	return result_value;
}

sabot_sql_uhugeint sabot_sql_value_uhugeint(sabot_sql_result *result, idx_t col, idx_t row) {
	sabot_sql_uhugeint result_value;
	auto internal_value = GetInternalCValue<uhugeint_t>(result, col, row);
	result_value.lower = internal_value.lower;
	result_value.upper = internal_value.upper;
	return result_value;
}

uint8_t sabot_sql_value_uint8(sabot_sql_result *result, idx_t col, idx_t row) {
	return GetInternalCValue<uint8_t>(result, col, row);
}

uint16_t sabot_sql_value_uint16(sabot_sql_result *result, idx_t col, idx_t row) {
	return GetInternalCValue<uint16_t>(result, col, row);
}

uint32_t sabot_sql_value_uint32(sabot_sql_result *result, idx_t col, idx_t row) {
	return GetInternalCValue<uint32_t>(result, col, row);
}

uint64_t sabot_sql_value_uint64(sabot_sql_result *result, idx_t col, idx_t row) {
	return GetInternalCValue<uint64_t>(result, col, row);
}

float sabot_sql_value_float(sabot_sql_result *result, idx_t col, idx_t row) {
	return GetInternalCValue<float>(result, col, row);
}

double sabot_sql_value_double(sabot_sql_result *result, idx_t col, idx_t row) {
	return GetInternalCValue<double>(result, col, row);
}

sabot_sql_date sabot_sql_value_date(sabot_sql_result *result, idx_t col, idx_t row) {
	sabot_sql_date result_value;
	result_value.days = GetInternalCValue<date_t>(result, col, row).days;
	return result_value;
}

sabot_sql_time sabot_sql_value_time(sabot_sql_result *result, idx_t col, idx_t row) {
	sabot_sql_time result_value;
	result_value.micros = GetInternalCValue<dtime_t>(result, col, row).micros;
	return result_value;
}

sabot_sql_timestamp sabot_sql_value_timestamp(sabot_sql_result *result, idx_t col, idx_t row) {
	sabot_sql_timestamp result_value;
	result_value.micros = GetInternalCValue<timestamp_t>(result, col, row).value;
	return result_value;
}

sabot_sql_interval sabot_sql_value_interval(sabot_sql_result *result, idx_t col, idx_t row) {
	sabot_sql_interval result_value;
	auto ival = GetInternalCValue<interval_t>(result, col, row);
	result_value.months = ival.months;
	result_value.days = ival.days;
	result_value.micros = ival.micros;
	return result_value;
}

char *sabot_sql_value_varchar(sabot_sql_result *result, idx_t col, idx_t row) {
	return sabot_sql_value_string(result, col, row).data;
}

sabot_sql_string sabot_sql_value_string(sabot_sql_result *result, idx_t col, idx_t row) {
	return GetInternalCValue<sabot_sql_string, ToCStringCastWrapper<StringCast>>(result, col, row);
}

char *sabot_sql_value_varchar_internal(sabot_sql_result *result, idx_t col, idx_t row) {
	return sabot_sql_value_string_internal(result, col, row).data;
}

sabot_sql_string sabot_sql_value_string_internal(sabot_sql_result *result, idx_t col, idx_t row) {
	if (!CanFetchValue(result, col, row)) {
		return FetchDefaultValue::Operation<sabot_sql_string>();
	}
	if (sabot_sql_column_type(result, col) != SABOT_SQL_TYPE_VARCHAR) {
		return FetchDefaultValue::Operation<sabot_sql_string>();
	}
	// FIXME: this obviously does not work when there are null bytes in the string
	// we need to remove the deprecated C result materialization to get that to work correctly
	// since the deprecated C result materialization stores strings as null-terminated
	sabot_sql_string res;
	res.data = UnsafeFetch<char *>(result, col, row);
	res.size = strlen(res.data);
	return res;
}

sabot_sql_blob sabot_sql_value_blob(sabot_sql_result *result, idx_t col, idx_t row) {
	if (CanFetchValue(result, col, row) && result->deprecated_columns[col].deprecated_type == SABOT_SQL_TYPE_BLOB) {
		auto internal_result = UnsafeFetch<sabot_sql_blob>(result, col, row);

		sabot_sql_blob result_blob;
		result_blob.data = malloc(internal_result.size);
		result_blob.size = internal_result.size;
		memcpy(result_blob.data, internal_result.data, internal_result.size);
		return result_blob;
	}
	return FetchDefaultValue::Operation<sabot_sql_blob>();
}

bool sabot_sql_value_is_null(sabot_sql_result *result, idx_t col, idx_t row) {
	if (!CanUseDeprecatedFetch(result, col, row)) {
		return false;
	}
	return result->deprecated_columns[col].deprecated_nullmask[row];
}
