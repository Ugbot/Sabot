//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/parser/tableref/subqueryref.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/parser/statement/select_statement.hpp"
#include "sabot_sql/parser/tableref.hpp"

namespace sabot_sql {
//! Represents a subquery
class SubqueryRef : public TableRef {
public:
	static constexpr const TableReferenceType TYPE = TableReferenceType::SUBQUERY;

private:
	SubqueryRef();

public:
	SABOT_SQL_API explicit SubqueryRef(unique_ptr<SelectStatement> subquery, string alias = string());

	//! The subquery
	unique_ptr<SelectStatement> subquery;

public:
	string ToString() const override;
	bool Equals(const TableRef &other_p) const override;

	unique_ptr<TableRef> Copy() override;

	//! Deserializes a blob back into a SubqueryRef
	void Serialize(Serializer &serializer) const override;
	static unique_ptr<TableRef> Deserialize(Deserializer &source);
};
} // namespace sabot_sql
