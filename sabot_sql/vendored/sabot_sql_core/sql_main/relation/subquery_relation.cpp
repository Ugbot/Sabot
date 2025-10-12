#include "sabot_sql/main/relation/subquery_relation.hpp"
#include "sabot_sql/main/client_context.hpp"
#include "sabot_sql/parser/query_node.hpp"

namespace sabot_sql {

SubqueryRelation::SubqueryRelation(shared_ptr<Relation> child_p, const string &alias_p)
    : Relation(child_p->context, RelationType::SUBQUERY_RELATION, alias_p), child(std::move(child_p)) {
	D_ASSERT(child.get() != this);
	vector<ColumnDefinition> dummy_columns;
	Relation::TryBindRelation(dummy_columns);
}

unique_ptr<QueryNode> SubqueryRelation::GetQueryNode() {
	return child->GetQueryNode();
}

const vector<ColumnDefinition> &SubqueryRelation::Columns() {
	return child->Columns();
}

string SubqueryRelation::ToString(idx_t depth) {
	return child->ToString(depth);
}

} // namespace sabot_sql
