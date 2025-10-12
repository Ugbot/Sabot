//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/parser/statement/delete_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/parser/parsed_expression.hpp"
#include "sabot_sql/parser/sql_statement.hpp"
#include "sabot_sql/parser/tableref.hpp"
#include "sabot_sql/parser/query_node.hpp"

namespace sabot_sql {

class DeleteStatement : public SQLStatement {
public:
	static constexpr const StatementType TYPE = StatementType::DELETE_STATEMENT;

public:
	DeleteStatement();

	unique_ptr<ParsedExpression> condition;
	unique_ptr<TableRef> table;
	vector<unique_ptr<TableRef>> using_clauses;
	vector<unique_ptr<ParsedExpression>> returning_list;
	//! CTEs
	CommonTableExpressionMap cte_map;

protected:
	DeleteStatement(const DeleteStatement &other);

public:
	string ToString() const override;
	unique_ptr<SQLStatement> Copy() const override;
};

} // namespace sabot_sql
