#include "sabot_sql/catalog/catalog.hpp"
#include "sabot_sql/parser/expression/constant_expression.hpp"
#include "sabot_sql/parser/statement/insert_statement.hpp"
#include "sabot_sql/parser/query_node/select_node.hpp"
#include "sabot_sql/planner/binder.hpp"
#include "sabot_sql/parser/statement/relation_statement.hpp"

namespace sabot_sql {

BoundStatement Binder::Bind(RelationStatement &stmt) {
	return stmt.relation->Bind(*this);
}

} // namespace sabot_sql
