//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/parser/statement/update_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/parser/parsed_expression.hpp"
#include "sabot_sql/parser/sql_statement.hpp"
#include "sabot_sql/parser/tableref.hpp"
#include "sabot_sql/common/vector.hpp"
#include "sabot_sql/parser/query_node.hpp"

namespace sabot_sql {

class UpdateSetInfo {
public:
	UpdateSetInfo();

public:
	unique_ptr<UpdateSetInfo> Copy() const;
	string ToString() const;

public:
	// The condition that needs to be met to perform the update
	unique_ptr<ParsedExpression> condition;
	// The columns to update
	vector<string> columns;
	// The set expressions to execute
	vector<unique_ptr<ParsedExpression>> expressions;

protected:
	UpdateSetInfo(const UpdateSetInfo &other);
};

class UpdateStatement : public SQLStatement {
public:
	static constexpr const StatementType TYPE = StatementType::UPDATE_STATEMENT;

public:
	UpdateStatement();

	unique_ptr<TableRef> table;
	unique_ptr<TableRef> from_table;
	//! keep track of optional returningList if statement contains a RETURNING keyword
	vector<unique_ptr<ParsedExpression>> returning_list;
	unique_ptr<UpdateSetInfo> set_info;
	//! CTEs
	CommonTableExpressionMap cte_map;

protected:
	UpdateStatement(const UpdateStatement &other);

public:
	string ToString() const override;
	unique_ptr<SQLStatement> Copy() const override;
};

} // namespace sabot_sql
