#include "sabot_sql/main/relation/materialized_relation.hpp"
#include "sabot_sql/parser/query_node/select_node.hpp"
#include "sabot_sql/main/client_context.hpp"
#include "sabot_sql/planner/bound_statement.hpp"
#include "sabot_sql/planner/operator/logical_column_data_get.hpp"
#include "sabot_sql/parser/tableref/column_data_ref.hpp"
#include "sabot_sql/parser/expression/star_expression.hpp"
#include "sabot_sql/common/exception.hpp"

namespace sabot_sql {

MaterializedRelation::MaterializedRelation(const shared_ptr<ClientContext> &context,
                                           unique_ptr<ColumnDataCollection> &&collection_p, vector<string> names,
                                           string alias_p)
    : Relation(context, RelationType::MATERIALIZED_RELATION), alias(std::move(alias_p)),
      collection(std::move(collection_p)) {
	// create constant expressions for the values
	auto types = collection->Types();
	D_ASSERT(types.size() == names.size());

	QueryResult::DeduplicateColumns(names);
	for (idx_t i = 0; i < types.size(); i++) {
		auto &type = types[i];
		auto &name = names[i];
		auto column_definition = ColumnDefinition(name, type);
		columns.push_back(std::move(column_definition));
	}
}

unique_ptr<QueryNode> MaterializedRelation::GetQueryNode() {
	auto result = make_uniq<SelectNode>();
	result->select_list.push_back(make_uniq<StarExpression>());
	result->from_table = GetTableRef();
	return std::move(result);
}

unique_ptr<TableRef> MaterializedRelation::GetTableRef() {
	auto table_ref = make_uniq<ColumnDataRef>(collection);
	for (auto &col : columns) {
		table_ref->expected_names.push_back(col.Name());
	}
	table_ref->alias = GetAlias();
	return std::move(table_ref);
}

string MaterializedRelation::GetAlias() {
	return alias;
}

const vector<ColumnDefinition> &MaterializedRelation::Columns() {
	return columns;
}

string MaterializedRelation::ToString(idx_t depth) {
	return collection->ToString();
}

} // namespace sabot_sql
