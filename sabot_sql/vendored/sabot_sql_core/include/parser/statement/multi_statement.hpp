//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/parser/statement/multi_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/parser/sql_statement.hpp"

namespace sabot_sql {

class MultiStatement : public SQLStatement {
public:
	static constexpr const StatementType TYPE = StatementType::MULTI_STATEMENT;

public:
	MultiStatement();

	vector<unique_ptr<SQLStatement>> statements;

protected:
	MultiStatement(const MultiStatement &other);

public:
	unique_ptr<SQLStatement> Copy() const override;
	string ToString() const override;
};

} // namespace sabot_sql
