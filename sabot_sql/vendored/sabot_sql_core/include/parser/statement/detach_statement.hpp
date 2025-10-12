//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/parser/statement/detach_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/parser/parsed_data/detach_info.hpp"
#include "sabot_sql/parser/sql_statement.hpp"

namespace sabot_sql {

class DetachStatement : public SQLStatement {
public:
	static constexpr const StatementType TYPE = StatementType::DETACH_STATEMENT;

public:
	DetachStatement();

	unique_ptr<DetachInfo> info;

protected:
	DetachStatement(const DetachStatement &other);

public:
	unique_ptr<SQLStatement> Copy() const override;
	string ToString() const override;
};

} // namespace sabot_sql
