#include "sabot_sql/parser/statement/create_statement.hpp"
#include "sabot_sql/parser/parsed_data/create_schema_info.hpp"
#include "sabot_sql/parser/transformer.hpp"

namespace sabot_sql {

unique_ptr<CreateStatement> Transformer::TransformCreateSchema(sabot_sql_libpgquery::PGCreateSchemaStmt &stmt) {
	auto result = make_uniq<CreateStatement>();
	auto info = make_uniq<CreateSchemaInfo>();

	D_ASSERT(stmt.schemaname);
	info->catalog = stmt.catalogname ? stmt.catalogname : INVALID_CATALOG;
	info->schema = stmt.schemaname;
	info->on_conflict = TransformOnConflict(stmt.onconflict);

	if (stmt.schemaElts) {
		// schema elements
		for (auto cell = stmt.schemaElts->head; cell != nullptr; cell = cell->next) {
			auto node = PGPointerCast<sabot_sql_libpgquery::PGNode>(cell->data.ptr_value);
			switch (node->type) {
			case sabot_sql_libpgquery::T_PGCreateStmt:
			case sabot_sql_libpgquery::T_PGViewStmt:
			default:
				throw NotImplementedException("Schema element not supported yet!");
			}
		}
	}
	result->info = std::move(info);
	return result;
}

} // namespace sabot_sql
