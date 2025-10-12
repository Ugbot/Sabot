//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/storage/statistics/struct_stats.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/common/common.hpp"
#include "sabot_sql/common/exception.hpp"

namespace sabot_sql {
class BaseStatistics;
struct SelectionVector;
class Vector;

struct StructStats {
	SABOT_SQL_API static void Construct(BaseStatistics &stats);
	SABOT_SQL_API static BaseStatistics CreateUnknown(LogicalType type);
	SABOT_SQL_API static BaseStatistics CreateEmpty(LogicalType type);

	SABOT_SQL_API static const BaseStatistics *GetChildStats(const BaseStatistics &stats);
	SABOT_SQL_API static const BaseStatistics &GetChildStats(const BaseStatistics &stats, idx_t i);
	SABOT_SQL_API static BaseStatistics &GetChildStats(BaseStatistics &stats, idx_t i);
	SABOT_SQL_API static void SetChildStats(BaseStatistics &stats, idx_t i, const BaseStatistics &new_stats);
	SABOT_SQL_API static void SetChildStats(BaseStatistics &stats, idx_t i, unique_ptr<BaseStatistics> new_stats);

	SABOT_SQL_API static void Serialize(const BaseStatistics &stats, Serializer &serializer);
	SABOT_SQL_API static void Deserialize(Deserializer &deserializer, BaseStatistics &base);

	SABOT_SQL_API static string ToString(const BaseStatistics &stats);

	SABOT_SQL_API static void Merge(BaseStatistics &stats, const BaseStatistics &other);
	SABOT_SQL_API static void Copy(BaseStatistics &stats, const BaseStatistics &other);
	SABOT_SQL_API static void Verify(const BaseStatistics &stats, Vector &vector, const SelectionVector &sel, idx_t count);
};

} // namespace sabot_sql
