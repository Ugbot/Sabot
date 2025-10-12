#include "sabot_sql/parser/transformer.hpp"
#include "sabot_sql/parser/statement/detach_statement.hpp"
#include "sabot_sql/parser/expression/constant_expression.hpp"
#include "sabot_sql/common/string_util.hpp"

namespace sabot_sql {

unique_ptr<DetachStatement> Transformer::TransformDetach(sabot_sql_libpgquery::PGDetachStmt &stmt) {
	auto result = make_uniq<DetachStatement>();
	auto info = make_uniq<DetachInfo>();
	info->name = stmt.db_name;
	info->if_not_found = TransformOnEntryNotFound(stmt.missing_ok);

	result->info = std::move(info);
	return result;
}

} // namespace sabot_sql
