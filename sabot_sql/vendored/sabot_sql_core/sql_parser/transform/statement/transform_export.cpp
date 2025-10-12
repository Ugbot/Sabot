#include "sabot_sql/parser/statement/export_statement.hpp"
#include "sabot_sql/parser/transformer.hpp"

namespace sabot_sql {

unique_ptr<ExportStatement> Transformer::TransformExport(sabot_sql_libpgquery::PGExportStmt &stmt) {
	auto info = make_uniq<CopyInfo>();
	info->file_path = stmt.filename;
	info->format = "csv";
	info->is_from = false;
	// handle export options
	TransformCopyOptions(*info, stmt.options);

	auto result = make_uniq<ExportStatement>(std::move(info));
	if (stmt.database) {
		result->database = stmt.database;
	}
	return result;
}

} // namespace sabot_sql
