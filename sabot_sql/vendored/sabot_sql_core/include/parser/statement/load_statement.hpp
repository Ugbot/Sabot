//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/parser/statement/load_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/parser/sql_statement.hpp"
#include "sabot_sql/parser/parsed_data/load_info.hpp"

namespace sabot_sql {

class LoadStatement : public SQLStatement {
public:
	static constexpr const StatementType TYPE = StatementType::LOAD_STATEMENT;

public:
	LoadStatement();

protected:
	LoadStatement(const LoadStatement &other);

public:
	unique_ptr<SQLStatement> Copy() const override;
	string ToString() const override;

	unique_ptr<LoadInfo> info;
};
} // namespace sabot_sql
