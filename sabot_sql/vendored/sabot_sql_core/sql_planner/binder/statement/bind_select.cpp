#include "sabot_sql/parser/statement/select_statement.hpp"
#include "sabot_sql/planner/binder.hpp"
#include "sabot_sql/planner/bound_query_node.hpp"

namespace sabot_sql {

BoundStatement Binder::Bind(SelectStatement &stmt) {
	auto &properties = GetStatementProperties();
	properties.allow_stream_result = true;
	properties.return_type = StatementReturnType::QUERY_RESULT;
	return Bind(*stmt.node);
}

} // namespace sabot_sql
