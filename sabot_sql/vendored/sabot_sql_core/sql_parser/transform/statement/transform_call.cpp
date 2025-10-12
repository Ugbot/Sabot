#include "sabot_sql/parser/statement/call_statement.hpp"
#include "sabot_sql/parser/transformer.hpp"

namespace sabot_sql {

unique_ptr<CallStatement> Transformer::TransformCall(sabot_sql_libpgquery::PGCallStmt &stmt) {
	auto result = make_uniq<CallStatement>();
	result->function = TransformFuncCall(*PGPointerCast<sabot_sql_libpgquery::PGFuncCall>(stmt.func));
	return result;
}

} // namespace sabot_sql
