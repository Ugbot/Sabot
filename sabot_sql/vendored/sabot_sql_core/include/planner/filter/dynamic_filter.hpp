
//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/planner/filter/dynamic_filter.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/planner/table_filter.hpp"
#include "sabot_sql/common/types/value.hpp"
#include "sabot_sql/common/enums/expression_type.hpp"
#include "sabot_sql/planner/filter/constant_filter.hpp"

namespace sabot_sql {

struct DynamicFilterData {
	mutex lock;
	unique_ptr<ConstantFilter> filter;
	bool initialized = false;

	void SetValue(Value val);
	void Reset();
};

class DynamicFilter : public TableFilter {
public:
	static constexpr const TableFilterType TYPE = TableFilterType::DYNAMIC_FILTER;

public:
	DynamicFilter();
	explicit DynamicFilter(shared_ptr<DynamicFilterData> filter_data);

	//! The shared, dynamic filter data
	shared_ptr<DynamicFilterData> filter_data;

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
