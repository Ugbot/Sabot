//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/parser/statement/execute_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/parser/parsed_expression.hpp"
#include "sabot_sql/parser/sql_statement.hpp"
#include "sabot_sql/common/vector.hpp"

namespace sabot_sql {

class ExecuteStatement : public SQLStatement {
public:
	static constexpr const StatementType TYPE = StatementType::EXECUTE_STATEMENT;

public:
	ExecuteStatement();

	string name;
	case_insensitive_map_t<unique_ptr<ParsedExpression>> named_values;

protected:
	ExecuteStatement(const ExecuteStatement &other);

public:
	unique_ptr<SQLStatement> Copy() const override;
	string ToString() const override;
};
} // namespace sabot_sql
