//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/planner/filter/in_filter.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/planner/table_filter.hpp"
#include "sabot_sql/common/types/value.hpp"

namespace sabot_sql {

class InFilter : public TableFilter {
public:
	static constexpr const TableFilterType TYPE = TableFilterType::IN_FILTER;

public:
	explicit InFilter(vector<Value> values);

	vector<Value> values;

public:
	FilterPropagateResult CheckStatistics(BaseStatistics &stats) const override;
	string ToString(const string &column_name) const override;
	bool Equals(const TableFilter &other) const override;
	unique_ptr<TableFilter> Copy() const override;
	unique_ptr<Expression> ToExpression(const Expression &column) const override;
	void Serialize(Serializer &serializer) const override;
	static unique_ptr<TableFilter> Deserialize(Deserializer &deserializer);
};

} // namespace sabot_sql
