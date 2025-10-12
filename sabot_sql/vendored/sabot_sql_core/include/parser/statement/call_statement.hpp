//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/parser/statement/call_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/parser/parsed_expression.hpp"
#include "sabot_sql/parser/sql_statement.hpp"
#include "sabot_sql/common/vector.hpp"

namespace sabot_sql {

class CallStatement : public SQLStatement {
public:
	static constexpr const StatementType TYPE = StatementType::CALL_STATEMENT;

public:
	CallStatement();

	unique_ptr<ParsedExpression> function;

protected:
	CallStatement(const CallStatement &other);

public:
	unique_ptr<SQLStatement> Copy() const override;
	string ToString() const override;
};
} // namespace sabot_sql
