#include "sabot_sql/parser/statement/load_statement.hpp"
#include "sabot_sql/parser/transformer.hpp"

namespace sabot_sql {

unique_ptr<LoadStatement> Transformer::TransformLoad(sabot_sql_libpgquery::PGLoadStmt &stmt) {
	D_ASSERT(stmt.type == sabot_sql_libpgquery::T_PGLoadStmt);

	auto load_stmt = make_uniq<LoadStatement>();
	auto load_info = make_uniq<LoadInfo>();
	load_info->filename = stmt.filename ? string(stmt.filename) : "";
	load_info->repository = stmt.repository ? string(stmt.repository) : "";
	load_info->repo_is_alias = stmt.repo_is_alias;
	load_info->version = stmt.version ? string(stmt.version) : "";
	switch (stmt.load_type) {
	case sabot_sql_libpgquery::PG_LOAD_TYPE_LOAD:
		load_info->load_type = LoadType::LOAD;
		break;
	case sabot_sql_libpgquery::PG_LOAD_TYPE_INSTALL:
		load_info->load_type = LoadType::INSTALL;
		break;
	case sabot_sql_libpgquery::PG_LOAD_TYPE_FORCE_INSTALL:
		load_info->load_type = LoadType::FORCE_INSTALL;
		break;
	}
	load_stmt->info = std::move(load_info);
	return load_stmt;
}

} // namespace sabot_sql
