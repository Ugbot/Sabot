#include "sabot_sql/parser/sql_statement.hpp"
#include "sabot_sql/parser/transformer.hpp"
#include "sabot_sql/parser/expression/star_expression.hpp"
#include "sabot_sql/parser/statement/select_statement.hpp"
#include "sabot_sql/parser/query_node/select_node.hpp"
#include "sabot_sql/parser/tableref/showref.hpp"

namespace sabot_sql {

unique_ptr<QueryNode> Transformer::TransformShowSelect(sabot_sql_libpgquery::PGVariableShowSelectStmt &stmt) {
	// we capture the select statement of SHOW
	auto select_node = make_uniq<SelectNode>();
	select_node->select_list.push_back(make_uniq<StarExpression>());

	auto show_ref = make_uniq<ShowRef>();
	show_ref->show_type = stmt.is_summary ? ShowType::SUMMARY : ShowType::DESCRIBE;
	show_ref->query = TransformSelectNode(*stmt.stmt);
	select_node->from_table = std::move(show_ref);
	return std::move(select_node);
}

unique_ptr<SelectStatement> Transformer::TransformShowSelectStmt(sabot_sql_libpgquery::PGVariableShowSelectStmt &stmt) {
	auto result = make_uniq<SelectStatement>();
	result->node = TransformShowSelect(stmt);
	return result;
}

} // namespace sabot_sql
