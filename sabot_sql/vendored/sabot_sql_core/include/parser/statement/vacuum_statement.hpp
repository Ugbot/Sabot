//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/parser/statement/vacuum_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/parser/parsed_expression.hpp"
#include "sabot_sql/parser/sql_statement.hpp"
#include "sabot_sql/parser/parsed_data/vacuum_info.hpp"

namespace sabot_sql {

class VacuumStatement : public SQLStatement {
public:
	static constexpr const StatementType TYPE = StatementType::VACUUM_STATEMENT;

public:
	explicit VacuumStatement(const VacuumOptions &options);

	unique_ptr<VacuumInfo> info;

protected:
	VacuumStatement(const VacuumStatement &other);

public:
	unique_ptr<SQLStatement> Copy() const override;
	string ToString() const override;
};

} // namespace sabot_sql
