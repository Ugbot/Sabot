//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/parser/statement/pragma_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/parser/sql_statement.hpp"
#include "sabot_sql/parser/parsed_data/pragma_info.hpp"
#include "sabot_sql/parser/parsed_expression.hpp"

namespace sabot_sql {

class PragmaStatement : public SQLStatement {
public:
	static constexpr const StatementType TYPE = StatementType::PRAGMA_STATEMENT;

public:
	PragmaStatement();

	unique_ptr<PragmaInfo> info;

protected:
	PragmaStatement(const PragmaStatement &other);

public:
	unique_ptr<SQLStatement> Copy() const override;
	string ToString() const override;
};

} // namespace sabot_sql
