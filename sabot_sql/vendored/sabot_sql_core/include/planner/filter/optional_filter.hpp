
//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/planner/filter/optional_filter.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/planner/table_filter.hpp"

namespace sabot_sql {

class OptionalFilter : public TableFilter {
public:
	static constexpr const TableFilterType TYPE = TableFilterType::OPTIONAL_FILTER;

public:
	explicit OptionalFilter(unique_ptr<TableFilter> filter = nullptr);

	//! Optional child filters.
	unique_ptr<TableFilter> child_filter;

public:
	string ToString(const string &column_name) const override;
	unique_ptr<TableFilter> Copy() const override;
	unique_ptr<Expression> ToExpression(const Expression &column) const override;
	FilterPropagateResult CheckStatistics(BaseStatistics &stats) const override;
	void Serialize(Serializer &serializer) const override;
	static unique_ptr<TableFilter> Deserialize(Deserializer &deserializer);
};

} // namespace sabot_sql
