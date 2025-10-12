#include "sabot_sql/parser/statement/select_statement.hpp"
#include "sabot_sql/parser/transformer.hpp"
#include "sabot_sql/common/string_util.hpp"

namespace sabot_sql {

unique_ptr<QueryNode> Transformer::TransformSelectNode(sabot_sql_libpgquery::PGNode &node, bool is_select) {
	switch (node.type) {
	case sabot_sql_libpgquery::T_PGVariableShowSelectStmt:
		return TransformShowSelect(PGCast<sabot_sql_libpgquery::PGVariableShowSelectStmt>(node));
	case sabot_sql_libpgquery::T_PGVariableShowStmt:
		return TransformShow(PGCast<sabot_sql_libpgquery::PGVariableShowStmt>(node));
	default:
		return TransformSelectNodeInternal(PGCast<sabot_sql_libpgquery::PGSelectStmt>(node), is_select);
	}
}

unique_ptr<QueryNode> Transformer::TransformSelectNodeInternal(sabot_sql_libpgquery::PGSelectStmt &select,
                                                               bool is_select) {
	// Both Insert/Create Table As uses this.
	if (is_select) {
		if (select.intoClause) {
			throw ParserException("SELECT INTO not supported!");
		}
		if (select.lockingClause) {
			throw ParserException("SELECT locking clause is not supported!");
		}
	}
	unique_ptr<QueryNode> stmt = nullptr;
	if (select.pivot) {
		stmt = TransformPivotStatement(select);
	} else {
		stmt = TransformSelectInternal(select);
	}
	return TransformMaterializedCTE(std::move(stmt));
}

unique_ptr<SelectStatement> Transformer::TransformSelectStmt(sabot_sql_libpgquery::PGSelectStmt &select, bool is_select) {
	auto result = make_uniq<SelectStatement>();
	result->node = TransformSelectNodeInternal(select, is_select);
	return result;
}

unique_ptr<SelectStatement> Transformer::TransformSelectStmt(sabot_sql_libpgquery::PGNode &node, bool is_select) {
	auto select_node = TransformSelectNode(node, is_select);
	auto select_statement = make_uniq<SelectStatement>();
	select_statement->node = std::move(select_node);
	return select_statement;
}

} // namespace sabot_sql
