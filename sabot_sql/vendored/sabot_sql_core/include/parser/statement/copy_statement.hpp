//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/parser/statement/copy_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/parser/parsed_data/copy_info.hpp"
#include "sabot_sql/parser/query_node.hpp"
#include "sabot_sql/parser/sql_statement.hpp"

namespace sabot_sql {

enum class CopyToType : uint8_t { COPY_TO_FILE, EXPORT_DATABASE };

class CopyStatement : public SQLStatement {
public:
	static constexpr const StatementType TYPE = StatementType::COPY_STATEMENT;

public:
	CopyStatement();

	unique_ptr<CopyInfo> info;

	string ToString() const override;

protected:
	CopyStatement(const CopyStatement &other);

public:
	SABOT_SQL_API unique_ptr<SQLStatement> Copy() const override;

private:
};
} // namespace sabot_sql
