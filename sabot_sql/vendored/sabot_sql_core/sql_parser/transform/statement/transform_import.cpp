#include "sabot_sql/parser/statement/pragma_statement.hpp"
#include "sabot_sql/parser/transformer.hpp"
#include "sabot_sql/parser/expression/constant_expression.hpp"

namespace sabot_sql {

unique_ptr<PragmaStatement> Transformer::TransformImport(sabot_sql_libpgquery::PGImportStmt &stmt) {
	auto result = make_uniq<PragmaStatement>();
	result->info->name = "import_database";
	result->info->parameters.emplace_back(make_uniq<ConstantExpression>(Value(stmt.filename)));
	return result;
}

} // namespace sabot_sql
