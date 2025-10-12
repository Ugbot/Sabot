//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/parser/statement/select_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/parser/query_node.hpp"
#include "sabot_sql/parser/sql_statement.hpp"
#include "sabot_sql/parser/tableref.hpp"

namespace sabot_sql {

class Serializer;
class Deserializer;

//! SelectStatement is a typical SELECT clause
class SelectStatement : public SQLStatement {
public:
	static constexpr const StatementType TYPE = StatementType::SELECT_STATEMENT;

public:
	SelectStatement() : SQLStatement(StatementType::SELECT_STATEMENT) {
	}

	//! The main query node
	unique_ptr<QueryNode> node;

protected:
	SelectStatement(const SelectStatement &other);

public:
	//! Convert the SELECT statement to a string

	SABOT_SQL_API string ToString() const override;
	//! Create a copy of this SelectStatement
	SABOT_SQL_API unique_ptr<SQLStatement> Copy() const override;
	//! Whether or not the statements are equivalent
	bool Equals(const SQLStatement &other) const;

	void Serialize(Serializer &serializer) const;
	static unique_ptr<SelectStatement> Deserialize(Deserializer &deserializer);
};
} // namespace sabot_sql
