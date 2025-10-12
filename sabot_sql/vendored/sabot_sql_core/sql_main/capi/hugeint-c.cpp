#include "sabot_sql/main/capi/capi_internal.hpp"
#include "sabot_sql/common/types/hugeint.hpp"
#include "sabot_sql/common/types/uhugeint.hpp"
#include "sabot_sql/common/types/decimal.hpp"
#include "sabot_sql/common/operator/decimal_cast_operators.hpp"
#include "sabot_sql/main/capi/cast/utils.hpp"
#include "sabot_sql/main/capi/cast/to_decimal.hpp"

using sabot_sql::Hugeint;
using sabot_sql::hugeint_t;
using sabot_sql::Uhugeint;
using sabot_sql::uhugeint_t;
using sabot_sql::Value;

double sabot_sql_hugeint_to_double(sabot_sql_hugeint val) {
	hugeint_t internal;
	internal.lower = val.lower;
	internal.upper = val.upper;
	return Hugeint::Cast<double>(internal);
}

double sabot_sql_uhugeint_to_double(sabot_sql_uhugeint val) {
	uhugeint_t internal;
	internal.lower = val.lower;
	internal.upper = val.upper;
	return Uhugeint::Cast<double>(internal);
}

static sabot_sql_decimal to_decimal_cast(double val, uint8_t width, uint8_t scale) {
	if (width > sabot_sql::Decimal::MAX_WIDTH_INT64) {
		return sabot_sql::TryCastToDecimalCInternal<double, sabot_sql::ToCDecimalCastWrapper<hugeint_t>>(val, width, scale);
	}
	if (width > sabot_sql::Decimal::MAX_WIDTH_INT32) {
		return sabot_sql::TryCastToDecimalCInternal<double, sabot_sql::ToCDecimalCastWrapper<int64_t>>(val, width, scale);
	}
	if (width > sabot_sql::Decimal::MAX_WIDTH_INT16) {
		return sabot_sql::TryCastToDecimalCInternal<double, sabot_sql::ToCDecimalCastWrapper<int32_t>>(val, width, scale);
	}
	return sabot_sql::TryCastToDecimalCInternal<double, sabot_sql::ToCDecimalCastWrapper<int16_t>>(val, width, scale);
}

sabot_sql_decimal sabot_sql_double_to_decimal(double val, uint8_t width, uint8_t scale) {
	if (scale > width || width > sabot_sql::Decimal::MAX_WIDTH_INT128) {
		return sabot_sql::FetchDefaultValue::Operation<sabot_sql_decimal>();
	}
	return to_decimal_cast(val, width, scale);
}

sabot_sql_hugeint sabot_sql_double_to_hugeint(double val) {
	hugeint_t internal_result;
	if (!Value::DoubleIsFinite(val) || !Hugeint::TryConvert<double>(val, internal_result)) {
		internal_result.lower = 0;
		internal_result.upper = 0;
	}

	sabot_sql_hugeint result;
	result.lower = internal_result.lower;
	result.upper = internal_result.upper;
	return result;
}

sabot_sql_uhugeint sabot_sql_double_to_uhugeint(double val) {
	uhugeint_t internal_result;
	if (!Value::DoubleIsFinite(val) || !Uhugeint::TryConvert<double>(val, internal_result)) {
		internal_result.lower = 0;
		internal_result.upper = 0;
	}

	sabot_sql_uhugeint result;
	result.lower = internal_result.lower;
	result.upper = internal_result.upper;
	return result;
}

double sabot_sql_decimal_to_double(sabot_sql_decimal val) {
	double result;
	hugeint_t value;
	value.lower = val.value.lower;
	value.upper = val.value.upper;
	sabot_sql::CastParameters parameters;
	sabot_sql::TryCastFromDecimal::Operation<hugeint_t, double>(value, result, parameters, val.width, val.scale);
	return result;
}
