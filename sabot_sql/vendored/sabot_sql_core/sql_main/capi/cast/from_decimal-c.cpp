#include "sabot_sql/main/capi/cast/from_decimal.hpp"
#include "sabot_sql/common/types/decimal.hpp"

namespace sabot_sql {

//! DECIMAL -> VARCHAR
template <>
bool CastDecimalCInternal(sabot_sql_result *source, sabot_sql_string &result, idx_t col, idx_t row) {
	auto result_data = (sabot_sql::SabotSQLResultData *)source->internal_data;
	auto &query_result = result_data->result;
	auto &source_type = query_result->types[col];
	auto width = sabot_sql::DecimalType::GetWidth(source_type);
	auto scale = sabot_sql::DecimalType::GetScale(source_type);
	sabot_sql::Vector result_vec(sabot_sql::LogicalType::VARCHAR, false, false);
	sabot_sql::string_t result_string;
	void *source_address = UnsafeFetchPtr<hugeint_t>(source, col, row);
	switch (source_type.InternalType()) {
	case sabot_sql::PhysicalType::INT16:
		result_string = sabot_sql::StringCastFromDecimal::Operation<int16_t>(UnsafeFetchFromPtr<int16_t>(source_address),
		                                                                  width, scale, result_vec);
		break;
	case sabot_sql::PhysicalType::INT32:
		result_string = sabot_sql::StringCastFromDecimal::Operation<int32_t>(UnsafeFetchFromPtr<int32_t>(source_address),
		                                                                  width, scale, result_vec);
		break;
	case sabot_sql::PhysicalType::INT64:
		result_string = sabot_sql::StringCastFromDecimal::Operation<int64_t>(UnsafeFetchFromPtr<int64_t>(source_address),
		                                                                  width, scale, result_vec);
		break;
	case sabot_sql::PhysicalType::INT128:
		result_string = sabot_sql::StringCastFromDecimal::Operation<hugeint_t>(
		    UnsafeFetchFromPtr<hugeint_t>(source_address), width, scale, result_vec);
		break;
	default:
		throw sabot_sql::InternalException("Unimplemented internal type for decimal");
	}
	result.data = reinterpret_cast<char *>(sabot_sql_malloc(sizeof(char) * (result_string.GetSize() + 1)));
	memcpy(result.data, result_string.GetData(), result_string.GetSize());
	result.data[result_string.GetSize()] = '\0';
	result.size = result_string.GetSize();
	return true;
}

template <class INTERNAL_TYPE>
sabot_sql_hugeint FetchInternals(void *source_address) {
	throw sabot_sql::NotImplementedException("FetchInternals not implemented for internal type");
}

template <>
sabot_sql_hugeint FetchInternals<int16_t>(void *source_address) {
	sabot_sql_hugeint result;
	int16_t intermediate_result;

	if (!TryCast::Operation<int16_t, int16_t>(UnsafeFetchFromPtr<int16_t>(source_address), intermediate_result)) {
		intermediate_result = FetchDefaultValue::Operation<int16_t>();
	}
	hugeint_t hugeint_result = Hugeint::Cast<int16_t>(intermediate_result);
	result.lower = hugeint_result.lower;
	result.upper = hugeint_result.upper;
	return result;
}
template <>
sabot_sql_hugeint FetchInternals<int32_t>(void *source_address) {
	sabot_sql_hugeint result;
	int32_t intermediate_result;

	if (!TryCast::Operation<int32_t, int32_t>(UnsafeFetchFromPtr<int32_t>(source_address), intermediate_result)) {
		intermediate_result = FetchDefaultValue::Operation<int32_t>();
	}
	hugeint_t hugeint_result = Hugeint::Cast<int32_t>(intermediate_result);
	result.lower = hugeint_result.lower;
	result.upper = hugeint_result.upper;
	return result;
}
template <>
sabot_sql_hugeint FetchInternals<int64_t>(void *source_address) {
	sabot_sql_hugeint result;
	int64_t intermediate_result;

	if (!TryCast::Operation<int64_t, int64_t>(UnsafeFetchFromPtr<int64_t>(source_address), intermediate_result)) {
		intermediate_result = FetchDefaultValue::Operation<int64_t>();
	}
	hugeint_t hugeint_result = Hugeint::Cast<int64_t>(intermediate_result);
	result.lower = hugeint_result.lower;
	result.upper = hugeint_result.upper;
	return result;
}
template <>
sabot_sql_hugeint FetchInternals<hugeint_t>(void *source_address) {
	sabot_sql_hugeint result;
	hugeint_t intermediate_result;

	if (!TryCast::Operation<hugeint_t, hugeint_t>(UnsafeFetchFromPtr<hugeint_t>(source_address), intermediate_result)) {
		intermediate_result = FetchDefaultValue::Operation<hugeint_t>();
	}
	result.lower = intermediate_result.lower;
	result.upper = intermediate_result.upper;
	return result;
}

//! DECIMAL -> DECIMAL (internal fetch)
template <>
bool CastDecimalCInternal(sabot_sql_result *source, sabot_sql_decimal &result, idx_t col, idx_t row) {
	auto result_data = (sabot_sql::SabotSQLResultData *)source->internal_data;
	result_data->result->types[col].GetDecimalProperties(result.width, result.scale);
	auto source_address = UnsafeFetchPtr<hugeint_t>(source, col, row);

	if (result.width > sabot_sql::Decimal::MAX_WIDTH_INT64) {
		result.value = FetchInternals<hugeint_t>(source_address);
	} else if (result.width > sabot_sql::Decimal::MAX_WIDTH_INT32) {
		result.value = FetchInternals<int64_t>(source_address);
	} else if (result.width > sabot_sql::Decimal::MAX_WIDTH_INT16) {
		result.value = FetchInternals<int32_t>(source_address);
	} else {
		result.value = FetchInternals<int16_t>(source_address);
	}
	return true;
}

} // namespace sabot_sql
