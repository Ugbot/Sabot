#include "sabot_sql/parser/transformer.hpp"
#include "sabot_sql/parser/statement/attach_statement.hpp"
#include "sabot_sql/parser/expression/constant_expression.hpp"
#include "sabot_sql/common/string_util.hpp"

namespace sabot_sql {

unique_ptr<AttachStatement> Transformer::TransformAttach(sabot_sql_libpgquery::PGAttachStmt &stmt) {
	auto result = make_uniq<AttachStatement>();
	auto info = make_uniq<AttachInfo>();
	info->name = stmt.name ? stmt.name : string();
	info->path = stmt.path;
	info->on_conflict = TransformOnConflict(stmt.onconflict);

	if (stmt.options) {
		sabot_sql_libpgquery::PGListCell *cell;
		for_each_cell(cell, stmt.options->head) {
			auto def_elem = PGPointerCast<sabot_sql_libpgquery::PGDefElem>(cell->data.ptr_value);
			unique_ptr<ParsedExpression> expr;
			if (def_elem->arg) {
				expr = TransformExpression(def_elem->arg);
			} else {
				expr = make_uniq<ConstantExpression>(Value::BOOLEAN(true));
			}
			info->parsed_options[StringUtil::Lower(def_elem->defname)] = std::move(expr);
		}
	}
	result->info = std::move(info);
	return result;
}

} // namespace sabot_sql
