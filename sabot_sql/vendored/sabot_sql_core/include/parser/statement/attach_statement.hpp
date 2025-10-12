//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/parser/statement/attach_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/parser/parsed_data/attach_info.hpp"
#include "sabot_sql/parser/sql_statement.hpp"

namespace sabot_sql {

class AttachStatement : public SQLStatement {
public:
	static constexpr const StatementType TYPE = StatementType::ATTACH_STATEMENT;

public:
	AttachStatement();

	unique_ptr<AttachInfo> info;

protected:
	AttachStatement(const AttachStatement &other);

public:
	unique_ptr<SQLStatement> Copy() const override;
	string ToString() const override;
};

} // namespace sabot_sql
