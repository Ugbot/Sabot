//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/storage/statistics/base_statistics.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/common/common.hpp"
#include "sabot_sql/common/enums/expression_type.hpp"
#include "sabot_sql/common/operator/comparison_operators.hpp"
#include "sabot_sql/common/types.hpp"
#include "sabot_sql/common/types/value.hpp"
#include "sabot_sql/storage/statistics/numeric_stats.hpp"
#include "sabot_sql/storage/statistics/string_stats.hpp"

namespace sabot_sql {
struct SelectionVector;

class Serializer;
class Deserializer;

class Vector;
struct UnifiedVectorFormat;

enum class StatsInfo : uint8_t {
	CAN_HAVE_NULL_VALUES = 0,
	CANNOT_HAVE_NULL_VALUES = 1,
	CAN_HAVE_VALID_VALUES = 2,
	CANNOT_HAVE_VALID_VALUES = 3,
	CAN_HAVE_NULL_AND_VALID_VALUES = 4
};

enum class StatisticsType : uint8_t { NUMERIC_STATS, STRING_STATS, LIST_STATS, STRUCT_STATS, BASE_STATS, ARRAY_STATS };

class BaseStatistics {
	friend struct NumericStats;
	friend struct StringStats;
	friend struct StructStats;
	friend struct ListStats;
	friend struct ArrayStats;

public:
	SABOT_SQL_API ~BaseStatistics();
	// disable copy constructors
	BaseStatistics(const BaseStatistics &other) = delete;
	BaseStatistics &operator=(const BaseStatistics &) = delete;
	//! enable move constructors
	SABOT_SQL_API BaseStatistics(BaseStatistics &&other) noexcept;
	SABOT_SQL_API BaseStatistics &operator=(BaseStatistics &&) noexcept;

public:
	//! Creates a set of statistics for data that is unknown, i.e. "has_null" is true, "has_no_null" is true, etc
	//! This can be used in case nothing is known about the data - or can be used as a baseline when only a few things
	//! are known
	static BaseStatistics CreateUnknown(LogicalType type);
	//! Creates statistics for an empty database, i.e. "has_null" is false, "has_no_null" is false, etc
	//! This is used when incrementally constructing statistics by constantly adding new values
	static BaseStatistics CreateEmpty(LogicalType type);

	SABOT_SQL_API StatisticsType GetStatsType() const;
	SABOT_SQL_API static StatisticsType GetStatsType(const LogicalType &type);

	SABOT_SQL_API bool CanHaveNull() const;
	SABOT_SQL_API bool CanHaveNoNull() const;

	void SetDistinctCount(idx_t distinct_count);

	bool IsConstant() const;

	const LogicalType &GetType() const {
		return type;
	}

	void Set(StatsInfo info);
	void CombineValidity(BaseStatistics &left, BaseStatistics &right);
	void CopyValidity(BaseStatistics &stats);
	//! Set that the CURRENT level can have null values
	//! Note that this is not correct for nested types unless this information is propagated in a different manner
	//! Use Set(StatsInfo::CAN_HAVE_NULL_VALUES) in the general case
	inline void SetHasNullFast() {
		has_null = true;
	}
	//! Set that the CURRENT level can have valid values
	//! Note that this is not correct for nested types unless this information is propagated in a different manner
	//! Use Set(StatsInfo::CAN_HAVE_VALID_VALUES) in the general case
	inline void SetHasNoNullFast() {
		has_no_null = true;
	}
	void SetHasNull();
	void SetHasNoNull();

	void Merge(const BaseStatistics &other);

	void Copy(const BaseStatistics &other);

	BaseStatistics Copy() const;
	unique_ptr<BaseStatistics> ToUnique() const;
	void CopyBase(const BaseStatistics &orig);

	void Serialize(Serializer &serializer) const;
	static BaseStatistics Deserialize(Deserializer &deserializer);

	//! Verify that a vector does not violate the statistics
	void Verify(Vector &vector, const SelectionVector &sel, idx_t count, bool ignore_has_null = false) const;
	void Verify(Vector &vector, idx_t count) const;

	string ToString() const;

	idx_t GetDistinctCount();
	static BaseStatistics FromConstant(const Value &input);

	template <class T>
	void UpdateNumericStats(T new_value) {
		D_ASSERT(GetStatsType() == StatisticsType::NUMERIC_STATS);
		NumericStats::Update(stats_union.numeric_data, new_value);
	}

private:
	BaseStatistics();
	explicit BaseStatistics(LogicalType type);

	static void Construct(BaseStatistics &stats, LogicalType type);

	void InitializeUnknown();
	void InitializeEmpty();

	static BaseStatistics CreateUnknownType(LogicalType type);
	static BaseStatistics CreateEmptyType(LogicalType type);
	static BaseStatistics FromConstantType(const Value &input);

private:
	//! The type of the logical segment
	LogicalType type;
	//! Whether or not the segment can contain NULL values
	bool has_null;
	//! Whether or not the segment can contain values that are not null
	bool has_no_null;
	//! estimate that one may have even if distinct_stats==nullptr
	idx_t distinct_count;
	//! Numeric and String stats
	union {
		//! Numeric stats data, for numeric stats
		NumericStatsData numeric_data;
		//! String stats data, for string stats
		StringStatsData string_data;
	} stats_union;
	//! Child stats (for LIST and STRUCT)
	unsafe_unique_array<BaseStatistics> child_stats;
};

template <>
inline void BaseStatistics::UpdateNumericStats<interval_t>(interval_t new_value) {
}
template <>
inline void BaseStatistics::UpdateNumericStats<list_entry_t>(list_entry_t new_value) {
}

} // namespace sabot_sql
