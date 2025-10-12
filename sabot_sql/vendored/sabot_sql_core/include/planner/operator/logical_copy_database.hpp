//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/planner/operator/logical_copy_database.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/planner/logical_operator.hpp"
#include "sabot_sql/parser/parsed_data/copy_database_info.hpp"

namespace sabot_sql {

class LogicalCopyDatabase : public LogicalOperator {
public:
	static constexpr const LogicalOperatorType TYPE = LogicalOperatorType::LOGICAL_COPY_DATABASE;

public:
	explicit LogicalCopyDatabase(unique_ptr<CopyDatabaseInfo> info_p);
	~LogicalCopyDatabase() override;

	unique_ptr<CopyDatabaseInfo> info;

public:
	void Serialize(Serializer &serializer) const override;
	static unique_ptr<LogicalOperator> Deserialize(Deserializer &deserializer);

protected:
	void ResolveTypes() override;

private:
	explicit LogicalCopyDatabase(unique_ptr<ParseInfo> info_p);
};

} // namespace sabot_sql
