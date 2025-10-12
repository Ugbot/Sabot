#include "sabot_sql/parser/statement/call_statement.hpp"
#include "sabot_sql/parser/tableref/table_function_ref.hpp"
#include "sabot_sql/planner/binder.hpp"
#include "sabot_sql/planner/operator/logical_get.hpp"
#include "sabot_sql/planner/tableref/bound_table_function.hpp"
#include "sabot_sql/parser/query_node/select_node.hpp"
#include "sabot_sql/parser/expression/star_expression.hpp"

namespace sabot_sql {

BoundStatement Binder::Bind(CallStatement &stmt) {
	SelectStatement select_statement;
	auto select_node = make_uniq<SelectNode>();
	auto table_function = make_uniq<TableFunctionRef>();
	table_function->function = std::move(stmt.function);
	select_node->select_list.push_back(make_uniq<StarExpression>());
	select_node->from_table = std::move(table_function);
	select_statement.node = std::move(select_node);

	auto result = Bind(select_statement);
	auto &properties = GetStatementProperties();
	properties.allow_stream_result = false;
	return result;
}

} // namespace sabot_sql
