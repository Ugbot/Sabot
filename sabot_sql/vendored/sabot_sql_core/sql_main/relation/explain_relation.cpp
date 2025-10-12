#include "sabot_sql/main/relation/explain_relation.hpp"
#include "sabot_sql/parser/statement/explain_statement.hpp"
#include "sabot_sql/parser/statement/select_statement.hpp"
#include "sabot_sql/parser/parsed_data/create_view_info.hpp"
#include "sabot_sql/planner/binder.hpp"
#include "sabot_sql/main/client_context.hpp"

namespace sabot_sql {

ExplainRelation::ExplainRelation(shared_ptr<Relation> child_p, ExplainType type, ExplainFormat format)
    : Relation(child_p->context, RelationType::EXPLAIN_RELATION), child(std::move(child_p)), type(type),
      format(format) {
	TryBindRelation(columns);
}

BoundStatement ExplainRelation::Bind(Binder &binder) {
	auto select = make_uniq<SelectStatement>();
	select->node = child->GetQueryNode();
	ExplainStatement explain(std::move(select), type, format);
	return binder.Bind(explain.Cast<SQLStatement>());
}

const vector<ColumnDefinition> &ExplainRelation::Columns() {
	return columns;
}

string ExplainRelation::ToString(idx_t depth) {
	string str = RenderWhitespace(depth) + "Explain\n";
	return str + child->ToString(depth + 1);
}

} // namespace sabot_sql
