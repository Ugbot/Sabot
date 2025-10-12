#include "sabot_sql/parser/transformer.hpp"
#include "sabot_sql/parser/expression/function_expression.hpp"
#include "sabot_sql/parser/statement/call_statement.hpp"
#include "sabot_sql/parser/expression/constant_expression.hpp"

namespace sabot_sql {

unique_ptr<SQLStatement> Transformer::TransformCheckpoint(sabot_sql_libpgquery::PGCheckPointStmt &stmt) {
	vector<unique_ptr<ParsedExpression>> children;
	// transform into "CALL checkpoint()" or "CALL force_checkpoint()"
	auto checkpoint_name = stmt.force ? "force_checkpoint" : "checkpoint";
	auto result = make_uniq<CallStatement>();
	auto function = make_uniq<FunctionExpression>(checkpoint_name, std::move(children));
	function->catalog = SYSTEM_CATALOG;
	function->schema = DEFAULT_SCHEMA;
	if (stmt.name) {
		function->children.push_back(make_uniq<ConstantExpression>(Value(stmt.name)));
	}
	result->function = std::move(function);
	return std::move(result);
}

} // namespace sabot_sql
