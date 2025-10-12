//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/main/capi/capi_cast_from_decimal.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/main/capi/cast/utils.hpp"

namespace sabot_sql {

//! DECIMAL -> ?
template <class RESULT_TYPE>
bool CastDecimalCInternal(sabot_sql_result *source, RESULT_TYPE &result, idx_t col, idx_t row) {
	auto result_data = (sabot_sql::SabotSQLResultData *)source->internal_data;
	auto &query_result = result_data->result;
	auto &source_type = query_result->types[col];
	auto width = sabot_sql::DecimalType::GetWidth(source_type);
	auto scale = sabot_sql::DecimalType::GetScale(source_type);
	void *source_address = UnsafeFetchPtr<hugeint_t>(source, col, row);

	CastParameters parameters;
	switch (source_type.InternalType()) {
	case sabot_sql::PhysicalType::INT16:
		return sabot_sql::TryCastFromDecimal::Operation<int16_t, RESULT_TYPE>(UnsafeFetchFromPtr<int16_t>(source_address),
		                                                                   result, parameters, width, scale);
	case sabot_sql::PhysicalType::INT32:
		return sabot_sql::TryCastFromDecimal::Operation<int32_t, RESULT_TYPE>(UnsafeFetchFromPtr<int32_t>(source_address),
		                                                                   result, parameters, width, scale);
	case sabot_sql::PhysicalType::INT64:
		return sabot_sql::TryCastFromDecimal::Operation<int64_t, RESULT_TYPE>(UnsafeFetchFromPtr<int64_t>(source_address),
		                                                                   result, parameters, width, scale);
	case sabot_sql::PhysicalType::INT128:
		return sabot_sql::TryCastFromDecimal::Operation<hugeint_t, RESULT_TYPE>(
		    UnsafeFetchFromPtr<hugeint_t>(source_address), result, parameters, width, scale);
	default:
		throw sabot_sql::InternalException("Unimplemented internal type for decimal");
	}
}

//! DECIMAL -> VARCHAR
template <>
bool CastDecimalCInternal(sabot_sql_result *source, sabot_sql_string &result, idx_t col, idx_t row);

//! DECIMAL -> DECIMAL (internal fetch)
template <>
bool CastDecimalCInternal(sabot_sql_result *source, sabot_sql_decimal &result, idx_t col, idx_t row);

//! DECIMAL -> ...
template <class RESULT_TYPE>
RESULT_TYPE TryCastDecimalCInternal(sabot_sql_result *source, idx_t col, idx_t row) {
	RESULT_TYPE result_value;
	try {
		if (!CastDecimalCInternal<RESULT_TYPE>(source, result_value, col, row)) {
			return FetchDefaultValue::Operation<RESULT_TYPE>();
		}
	} catch (...) {
		return FetchDefaultValue::Operation<RESULT_TYPE>();
	}
	return result_value;
}

} // namespace sabot_sql
