#include "sabot_sql/parser/statement/create_statement.hpp"
#include "sabot_sql/parser/expression/constant_expression.hpp"
#include "sabot_sql/parser/parsed_data/create_table_info.hpp"
#include "sabot_sql/parser/query_node/select_node.hpp"
#include "sabot_sql/parser/tableref/subqueryref.hpp"
#include "sabot_sql/parser/transformer.hpp"

namespace sabot_sql {

unique_ptr<CreateStatement> Transformer::TransformCreateTableAs(sabot_sql_libpgquery::PGCreateTableAsStmt &stmt) {
	if (stmt.relkind == sabot_sql_libpgquery::PG_OBJECT_MATVIEW) {
		throw NotImplementedException("Materialized view not implemented");
	}
	if (stmt.is_select_into || stmt.into->options) {
		throw NotImplementedException("Unimplemented features for CREATE TABLE as");
	}
	if (stmt.query->type != sabot_sql_libpgquery::T_PGSelectStmt) {
		throw ParserException("CREATE TABLE AS requires a SELECT clause");
	}

	auto result = make_uniq<CreateStatement>();
	auto info = make_uniq<CreateTableInfo>();
	auto qname = TransformQualifiedName(*stmt.into->rel);
	auto query = TransformSelectStmt(*stmt.query, false);

	// push a LIMIT 0 if 'WITH NO DATA' is specified
	if (stmt.into->skipData) {
		auto limit_modifier = make_uniq<LimitModifier>();
		limit_modifier->limit = make_uniq<ConstantExpression>(Value::BIGINT(0));
		auto limit_node = make_uniq<SelectNode>();
		limit_node->modifiers.push_back(std::move(limit_modifier));
		limit_node->from_table = make_uniq<SubqueryRef>(std::move(query));
		limit_node->select_list.push_back(make_uniq<StarExpression>());
		auto limit_stmt = make_uniq<SelectStatement>();
		limit_stmt->node = std::move(limit_node);
		query = std::move(limit_stmt);
	}

	if (stmt.into->colNames) {
		auto cols = TransformStringList(stmt.into->colNames);
		for (idx_t i = 0; i < cols.size(); i++) {
			// We really don't know the type of the columns during parsing, so we just use UNKNOWN
			info->columns.AddColumn(ColumnDefinition(cols[i], LogicalType::UNKNOWN));
		}
	}
	info->catalog = qname.catalog;
	info->schema = qname.schema;
	info->table = qname.name;
	info->on_conflict = TransformOnConflict(stmt.onconflict);
	info->temporary =
	    stmt.into->rel->relpersistence == sabot_sql_libpgquery::PGPostgresRelPersistence::PG_RELPERSISTENCE_TEMP;
	info->query = std::move(query);
	result->info = std::move(info);
	return result;
}

} // namespace sabot_sql
