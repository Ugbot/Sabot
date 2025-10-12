#include "sabot_sql/planner/table_filter.hpp"
#include "sabot_sql/planner/filter/optional_filter.hpp"
#include "sabot_sql/planner/expression.hpp"

namespace sabot_sql {

OptionalFilter::OptionalFilter(unique_ptr<TableFilter> filter)
    : TableFilter(TableFilterType::OPTIONAL_FILTER), child_filter(std::move(filter)) {
}

FilterPropagateResult OptionalFilter::CheckStatistics(BaseStatistics &stats) const {
	return child_filter->CheckStatistics(stats);
}

string OptionalFilter::ToString(const string &column_name) const {
	return string("optional: ") + child_filter->ToString(column_name);
}

unique_ptr<Expression> OptionalFilter::ToExpression(const Expression &column) const {
	return child_filter->ToExpression(column);
}

unique_ptr<TableFilter> OptionalFilter::Copy() const {
	auto copy = make_uniq<OptionalFilter>();
	copy->child_filter = child_filter->Copy();
	return sabot_sql::unique_ptr_cast<OptionalFilter, TableFilter>(std::move(copy));
}

} // namespace sabot_sql
