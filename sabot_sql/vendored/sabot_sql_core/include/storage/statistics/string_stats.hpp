//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/storage/statistics/string_stats.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/common/common.hpp"
#include "sabot_sql/common/enums/expression_type.hpp"
#include "sabot_sql/common/enums/filter_propagate_result.hpp"
#include "sabot_sql/common/exception.hpp"
#include "sabot_sql/common/operator/comparison_operators.hpp"
#include "sabot_sql/common/types/hugeint.hpp"
#include "sabot_sql/common/array_ptr.hpp"

namespace sabot_sql {
class BaseStatistics;
struct SelectionVector;
class Vector;

struct StringStatsData {
	constexpr static uint32_t MAX_STRING_MINMAX_SIZE = 8;

	//! The minimum value of the segment, potentially truncated
	data_t min[MAX_STRING_MINMAX_SIZE];
	//! The maximum value of the segment, potentially truncated
	data_t max[MAX_STRING_MINMAX_SIZE];
	//! Whether or not the column can contain unicode characters
	bool has_unicode;
	//! Whether or not the maximum string length is known
	bool has_max_string_length;
	//! The maximum string length in bytes
	uint32_t max_string_length;
};

struct StringStats {
	//! Unknown statistics - i.e. "has_unicode" is true, "max_string_length" is unknown, "min" is \0, max is \xFF
	SABOT_SQL_API static BaseStatistics CreateUnknown(LogicalType type);
	//! Empty statistics - i.e. "has_unicode" is false, "max_string_length" is 0, "min" is \xFF, max is \x00
	SABOT_SQL_API static BaseStatistics CreateEmpty(LogicalType type);
	//! Whether or not the statistics have a maximum string length defined
	SABOT_SQL_API static bool HasMaxStringLength(const BaseStatistics &stats);
	//! Returns the maximum string length, or throws an exception if !HasMaxStringLength()
	SABOT_SQL_API static uint32_t MaxStringLength(const BaseStatistics &stats);
	//! Whether or not the strings can contain unicode
	SABOT_SQL_API static bool CanContainUnicode(const BaseStatistics &stats);
	//! Returns the min value (up to a length of StringStatsData::MAX_STRING_MINMAX_SIZE)
	SABOT_SQL_API static string Min(const BaseStatistics &stats);
	//! Returns the max value (up to a length of StringStatsData::MAX_STRING_MINMAX_SIZE)
	SABOT_SQL_API static string Max(const BaseStatistics &stats);

	//! Resets the max string length so HasMaxStringLength() is false
	SABOT_SQL_API static void ResetMaxStringLength(BaseStatistics &stats);
	//! Sets the max string length
	SABOT_SQL_API static void SetMaxStringLength(BaseStatistics &stats, uint32_t length);
	//! FIXME: make this part of Set on statistics
	SABOT_SQL_API static void SetContainsUnicode(BaseStatistics &stats);

	SABOT_SQL_API static void Serialize(const BaseStatistics &stats, Serializer &serializer);
	SABOT_SQL_API static void Deserialize(Deserializer &deserializer, BaseStatistics &base);

	SABOT_SQL_API static string ToString(const BaseStatistics &stats);

	SABOT_SQL_API static FilterPropagateResult CheckZonemap(const BaseStatistics &stats, ExpressionType comparison_type,
	                                                     array_ptr<const Value> constants);
	SABOT_SQL_API static FilterPropagateResult CheckZonemap(const_data_ptr_t min_data, idx_t min_len,
	                                                     const_data_ptr_t max_data, idx_t max_len,
	                                                     ExpressionType comparison_type, const string &value);

	SABOT_SQL_API static void Update(BaseStatistics &stats, const string_t &value);
	SABOT_SQL_API static void SetMin(BaseStatistics &stats, const string_t &value);
	SABOT_SQL_API static void SetMax(BaseStatistics &stats, const string_t &value);
	SABOT_SQL_API static void Merge(BaseStatistics &stats, const BaseStatistics &other);
	SABOT_SQL_API static void Verify(const BaseStatistics &stats, Vector &vector, const SelectionVector &sel, idx_t count);

private:
	static StringStatsData &GetDataUnsafe(BaseStatistics &stats);
	static const StringStatsData &GetDataUnsafe(const BaseStatistics &stats);
};

} // namespace sabot_sql
