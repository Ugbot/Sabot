#include "sabot_sql/main/capi/cast/utils.hpp"

namespace sabot_sql {

template <>
sabot_sql_decimal FetchDefaultValue::Operation() {
	sabot_sql_decimal result;
	result.scale = 0;
	result.width = 0;
	result.value = {0, 0};
	return result;
}

template <>
date_t FetchDefaultValue::Operation() {
	date_t result;
	result.days = 0;
	return result;
}

template <>
dtime_t FetchDefaultValue::Operation() {
	dtime_t result;
	result.micros = 0;
	return result;
}

template <>
timestamp_t FetchDefaultValue::Operation() {
	timestamp_t result;
	result.value = 0;
	return result;
}

template <>
interval_t FetchDefaultValue::Operation() {
	interval_t result;
	result.months = 0;
	result.days = 0;
	result.micros = 0;
	return result;
}

template <>
char *FetchDefaultValue::Operation() {
	return nullptr;
}

template <>
sabot_sql_string FetchDefaultValue::Operation() {
	sabot_sql_string result;
	result.data = nullptr;
	result.size = 0;
	return result;
}

template <>
sabot_sql_blob FetchDefaultValue::Operation() {
	sabot_sql_blob result;
	result.data = nullptr;
	result.size = 0;
	return result;
}

//===--------------------------------------------------------------------===//
// Blob Casts
//===--------------------------------------------------------------------===//

template <>
bool FromCBlobCastWrapper::Operation(sabot_sql_blob input, sabot_sql_string &result) {
	string_t input_str(const_char_ptr_cast(input.data), UnsafeNumericCast<uint32_t>(input.size));
	return ToCStringCastWrapper<sabot_sql::CastFromBlob>::template Operation<string_t, sabot_sql_string>(input_str, result);
}

} // namespace sabot_sql

bool CanUseDeprecatedFetch(sabot_sql_result *result, idx_t col, idx_t row) {
	if (!result) {
		return false;
	}
	if (!sabot_sql::DeprecatedMaterializeResult(result)) {
		return false;
	}
	if (col >= result->deprecated_column_count || row >= result->deprecated_row_count) {
		return false;
	}
	return true;
}

bool CanFetchValue(sabot_sql_result *result, idx_t col, idx_t row) {
	if (!CanUseDeprecatedFetch(result, col, row)) {
		return false;
	}
	if (result->deprecated_columns[col].deprecated_nullmask[row]) {
		return false;
	}
	return true;
}
