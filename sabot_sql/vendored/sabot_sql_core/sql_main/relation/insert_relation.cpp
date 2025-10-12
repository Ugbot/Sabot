#include "sabot_sql/main/relation/insert_relation.hpp"
#include "sabot_sql/parser/statement/insert_statement.hpp"
#include "sabot_sql/parser/statement/select_statement.hpp"
#include "sabot_sql/parser/parsed_data/create_table_info.hpp"
#include "sabot_sql/planner/binder.hpp"
#include "sabot_sql/main/client_context.hpp"

namespace sabot_sql {

InsertRelation::InsertRelation(shared_ptr<Relation> child_p, string schema_name, string table_name)
    : Relation(child_p->context, RelationType::INSERT_RELATION), child(std::move(child_p)),
      schema_name(std::move(schema_name)), table_name(std::move(table_name)) {
	TryBindRelation(columns);
}

BoundStatement InsertRelation::Bind(Binder &binder) {
	InsertStatement stmt;
	auto select = make_uniq<SelectStatement>();
	select->node = child->GetQueryNode();

	stmt.schema = schema_name;
	stmt.table = table_name;
	stmt.select_statement = std::move(select);
	return binder.Bind(stmt.Cast<SQLStatement>());
}

const vector<ColumnDefinition> &InsertRelation::Columns() {
	return columns;
}

string InsertRelation::ToString(idx_t depth) {
	string str = RenderWhitespace(depth) + "Insert\n";
	return str + child->ToString(depth + 1);
}

} // namespace sabot_sql
