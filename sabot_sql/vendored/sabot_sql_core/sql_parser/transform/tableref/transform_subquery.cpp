#include "sabot_sql/parser/tableref/subqueryref.hpp"
#include "sabot_sql/parser/transformer.hpp"

namespace sabot_sql {

unique_ptr<TableRef> Transformer::TransformRangeSubselect(sabot_sql_libpgquery::PGRangeSubselect &root) {
	Transformer subquery_transformer(*this);
	auto subquery = subquery_transformer.TransformSelectStmt(*root.subquery);
	if (!subquery) {
		return nullptr;
	}
	auto result = make_uniq<SubqueryRef>(std::move(subquery));
	result->alias = TransformAlias(root.alias, result->column_name_alias);
	if (root.sample) {
		result->sample = TransformSampleOptions(root.sample);
	}
	return std::move(result);
}

} // namespace sabot_sql
