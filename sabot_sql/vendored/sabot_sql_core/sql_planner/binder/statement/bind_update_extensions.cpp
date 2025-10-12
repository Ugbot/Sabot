#include "sabot_sql/parser/statement/load_statement.hpp"
#include "sabot_sql/planner/binder.hpp"
#include "sabot_sql/parser/statement/update_extensions_statement.hpp"
#include "sabot_sql/planner/operator/logical_simple.hpp"
#include <algorithm>

namespace sabot_sql {

BoundStatement Binder::Bind(UpdateExtensionsStatement &stmt) {
	BoundStatement result;

	result.names.emplace_back("extension_name");
	result.types.emplace_back(LogicalType::VARCHAR);
	result.names.emplace_back("repository");
	result.types.emplace_back(LogicalType::VARCHAR);
	result.names.emplace_back("update_result");
	result.types.emplace_back(LogicalType::VARCHAR);
	result.names.emplace_back("previous_version");
	result.types.emplace_back(LogicalType::VARCHAR);
	result.names.emplace_back("current_version");
	result.types.emplace_back(LogicalType::VARCHAR);

	result.plan = make_uniq<LogicalSimple>(LogicalOperatorType::LOGICAL_UPDATE_EXTENSIONS, std::move(stmt.info));

	return result;
}

} // namespace sabot_sql
