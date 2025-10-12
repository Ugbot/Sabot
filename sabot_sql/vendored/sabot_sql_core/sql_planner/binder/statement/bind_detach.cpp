#include "sabot_sql/parser/statement/detach_statement.hpp"
#include "sabot_sql/planner/binder.hpp"
#include "sabot_sql/planner/operator/logical_simple.hpp"
#include "sabot_sql/main/config.hpp"

namespace sabot_sql {

BoundStatement Binder::Bind(DetachStatement &stmt) {
	BoundStatement result;

	result.plan = make_uniq<LogicalSimple>(LogicalOperatorType::LOGICAL_DETACH, std::move(stmt.info));
	result.names = {"Success"};
	result.types = {LogicalType::BOOLEAN};

	auto &properties = GetStatementProperties();
	properties.allow_stream_result = false;
	properties.return_type = StatementReturnType::NOTHING;
	return result;
}

} // namespace sabot_sql
