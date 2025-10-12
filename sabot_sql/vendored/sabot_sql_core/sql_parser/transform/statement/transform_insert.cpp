#include "sabot_sql/parser/statement/insert_statement.hpp"
#include "sabot_sql/parser/tableref/expressionlistref.hpp"
#include "sabot_sql/parser/transformer.hpp"

namespace sabot_sql {

unique_ptr<TableRef> Transformer::TransformValuesList(sabot_sql_libpgquery::PGList *list) {
	auto result = make_uniq<ExpressionListRef>();
	for (auto value_list = list->head; value_list != nullptr; value_list = value_list->next) {
		auto target = PGPointerCast<sabot_sql_libpgquery::PGList>(value_list->data.ptr_value);

		vector<unique_ptr<ParsedExpression>> insert_values;
		TransformExpressionList(*target, insert_values);
		if (!result->values.empty()) {
			if (result->values[0].size() != insert_values.size()) {
				throw ParserException("VALUES lists must all be the same length");
			}
		}
		result->values.push_back(std::move(insert_values));
	}
	result->alias = "valueslist";
	return std::move(result);
}

vector<string> Transformer::TransformInsertColumns(sabot_sql_libpgquery::PGList &cols) {
	vector<string> result;
	for (auto c = cols.head; c != nullptr; c = lnext(c)) {
		auto target = PGPointerCast<sabot_sql_libpgquery::PGResTarget>(c->data.ptr_value);
		result.emplace_back(target->name);
	}
	return result;
}

InsertColumnOrder Transformer::TransformColumnOrder(sabot_sql_libpgquery::PGInsertColumnOrder insert_column_order) {
	switch (insert_column_order) {
	case sabot_sql_libpgquery::PG_INSERT_BY_POSITION:
		return InsertColumnOrder::INSERT_BY_POSITION;
	case sabot_sql_libpgquery::PG_INSERT_BY_NAME:
		return InsertColumnOrder::INSERT_BY_NAME;
	default:
		throw InternalException("Unrecognized insert column order in TransformInsert");
	}
}

unique_ptr<InsertStatement> Transformer::TransformInsert(sabot_sql_libpgquery::PGInsertStmt &stmt) {
	auto result = make_uniq<InsertStatement>();
	if (stmt.withClause) {
		TransformCTE(*PGPointerCast<sabot_sql_libpgquery::PGWithClause>(stmt.withClause), result->cte_map);
	}

	// first check if there are any columns specified
	if (stmt.cols) {
		result->columns = TransformInsertColumns(*stmt.cols);
	}

	// Grab and transform the returning columns from the parser.
	if (stmt.returningList) {
		TransformExpressionList(*stmt.returningList, result->returning_list);
	}
	if (stmt.selectStmt) {
		result->select_statement = TransformSelectStmt(*stmt.selectStmt, false);
	} else {
		result->default_values = true;
	}

	auto qname = TransformQualifiedName(*stmt.relation);
	result->table = qname.name;
	result->schema = qname.schema;

	if (stmt.onConflictClause) {
		if (stmt.onConflictAlias != sabot_sql_libpgquery::PG_ONCONFLICT_ALIAS_NONE) {
			// OR REPLACE | OR IGNORE are shorthands for the ON CONFLICT clause
			throw ParserException("You can not provide both OR REPLACE|IGNORE and an ON CONFLICT clause, please remove "
			                      "the first if you want to have more granual control");
		}
		result->on_conflict_info = TransformOnConflictClause(stmt.onConflictClause, result->schema);
		result->table_ref = TransformRangeVar(*stmt.relation);
	}
	if (stmt.onConflictAlias != sabot_sql_libpgquery::PG_ONCONFLICT_ALIAS_NONE) {
		D_ASSERT(!stmt.onConflictClause);
		result->on_conflict_info = DummyOnConflictClause(stmt.onConflictAlias, result->schema);
		result->table_ref = TransformRangeVar(*stmt.relation);
	}
	result->column_order = TransformColumnOrder(stmt.insert_column_order);
	result->catalog = qname.catalog;
	return result;
}

} // namespace sabot_sql
