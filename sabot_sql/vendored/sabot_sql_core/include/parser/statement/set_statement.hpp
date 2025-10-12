//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/parser/statement/set_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/common/enums/set_scope.hpp"
#include "sabot_sql/common/enums/set_type.hpp"
#include "sabot_sql/parser/sql_statement.hpp"
#include "sabot_sql/common/types/value.hpp"
#include "sabot_sql/parser/parsed_expression.hpp"

namespace sabot_sql {

class SetStatement : public SQLStatement {
public:
	static constexpr const StatementType TYPE = StatementType::SET_STATEMENT;

protected:
	SetStatement(string name_p, SetScope scope_p, SetType type_p);
	SetStatement(const SetStatement &other) = default;

public:
	string name;
	SetScope scope;
	SetType set_type;
};

class SetVariableStatement : public SetStatement {
public:
	SetVariableStatement(string name_p, unique_ptr<ParsedExpression> value_p, SetScope scope_p);

protected:
	SetVariableStatement(const SetVariableStatement &other);

public:
	unique_ptr<SQLStatement> Copy() const override;
	string ToString() const override;

public:
	unique_ptr<ParsedExpression> value;
};

class ResetVariableStatement : public SetStatement {
public:
	ResetVariableStatement(std::string name_p, SetScope scope_p);

protected:
	ResetVariableStatement(const ResetVariableStatement &other) = default;

public:
	unique_ptr<SQLStatement> Copy() const override;
	string ToString() const override;
};

} // namespace sabot_sql
