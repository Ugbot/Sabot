//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/parser/statement/drop_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/parser/parsed_data/drop_info.hpp"
#include "sabot_sql/parser/sql_statement.hpp"

namespace sabot_sql {

class DropStatement : public SQLStatement {
public:
	static constexpr const StatementType TYPE = StatementType::DROP_STATEMENT;

public:
	DropStatement();

	unique_ptr<DropInfo> info;

protected:
	DropStatement(const DropStatement &other);

public:
	unique_ptr<SQLStatement> Copy() const override;
	string ToString() const override;
};

} // namespace sabot_sql
