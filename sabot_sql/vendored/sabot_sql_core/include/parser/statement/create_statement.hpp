//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/parser/statement/create_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/parser/parsed_data/create_info.hpp"
#include "sabot_sql/parser/sql_statement.hpp"

namespace sabot_sql {

class CreateStatement : public SQLStatement {
public:
	static constexpr const StatementType TYPE = StatementType::CREATE_STATEMENT;

public:
	CreateStatement();

	unique_ptr<CreateInfo> info;

protected:
	CreateStatement(const CreateStatement &other);

public:
	unique_ptr<SQLStatement> Copy() const override;
	string ToString() const override;
};

} // namespace sabot_sql
