#include "sabot_sql/planner/operator/logical_copy_database.hpp"
#include "sabot_sql/parser/parsed_data/create_table_info.hpp"

namespace sabot_sql {

LogicalCopyDatabase::LogicalCopyDatabase(unique_ptr<CopyDatabaseInfo> info_p)
    : LogicalOperator(LogicalOperatorType::LOGICAL_COPY_DATABASE), info(std::move(info_p)) {
}

LogicalCopyDatabase::LogicalCopyDatabase(unique_ptr<ParseInfo> info_p)
    : LogicalOperator(LogicalOperatorType::LOGICAL_COPY_DATABASE),
      info(unique_ptr_cast<ParseInfo, CopyDatabaseInfo>(std::move(info_p))) {
}

LogicalCopyDatabase::~LogicalCopyDatabase() {
}

void LogicalCopyDatabase::ResolveTypes() {
	types.emplace_back(LogicalType::BOOLEAN);
}

} // namespace sabot_sql
