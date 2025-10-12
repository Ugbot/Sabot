//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/parser/statement/update_extensions_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/parser/parsed_expression.hpp"
#include "sabot_sql/parser/sql_statement.hpp"
#include "sabot_sql/parser/tableref.hpp"
#include "sabot_sql/common/vector.hpp"
#include "sabot_sql/parser/query_node.hpp"
#include "sabot_sql/parser/parsed_data/update_extensions_info.hpp"

namespace sabot_sql {

class UpdateExtensionsStatement : public SQLStatement {
public:
	static constexpr const StatementType TYPE = StatementType::UPDATE_EXTENSIONS_STATEMENT;

public:
	UpdateExtensionsStatement();
	unique_ptr<UpdateExtensionsInfo> info;

protected:
	UpdateExtensionsStatement(const UpdateExtensionsStatement &other);

public:
	string ToString() const override;
	unique_ptr<SQLStatement> Copy() const override;
};

} // namespace sabot_sql
