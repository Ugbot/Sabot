//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/parser/statement/alter_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/parser/column_definition.hpp"
#include "sabot_sql/parser/parsed_data/alter_table_info.hpp"
#include "sabot_sql/parser/sql_statement.hpp"

namespace sabot_sql {

class AlterStatement : public SQLStatement {
public:
	static constexpr const StatementType TYPE = StatementType::ALTER_STATEMENT;

public:
	AlterStatement();

	unique_ptr<AlterInfo> info;

protected:
	AlterStatement(const AlterStatement &other);

public:
	unique_ptr<SQLStatement> Copy() const override;
	string ToString() const override;
};

} // namespace sabot_sql
