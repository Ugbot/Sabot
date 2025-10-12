#include "sabot_sql/parser/statement/delete_statement.hpp"
#include "sabot_sql/parser/transformer.hpp"

namespace sabot_sql {

unique_ptr<DeleteStatement> Transformer::TransformDelete(sabot_sql_libpgquery::PGDeleteStmt &stmt) {
	auto result = make_uniq<DeleteStatement>();
	if (stmt.withClause) {
		TransformCTE(*PGPointerCast<sabot_sql_libpgquery::PGWithClause>(stmt.withClause), result->cte_map);
	}

	result->condition = TransformExpression(stmt.whereClause);
	result->table = TransformRangeVar(*stmt.relation);
	if (result->table->type != TableReferenceType::BASE_TABLE) {
		throw InvalidInputException("Can only delete from base tables!");
	}
	if (stmt.usingClause) {
		for (auto n = stmt.usingClause->head; n != nullptr; n = n->next) {
			auto target = PGPointerCast<sabot_sql_libpgquery::PGNode>(n->data.ptr_value);
			auto using_entry = TransformTableRefNode(*target);
			result->using_clauses.push_back(std::move(using_entry));
		}
	}

	if (stmt.returningList) {
		TransformExpressionList(*stmt.returningList, result->returning_list);
	}

	return result;
}

} // namespace sabot_sql
