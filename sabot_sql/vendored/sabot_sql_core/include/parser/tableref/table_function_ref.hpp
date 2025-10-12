//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/parser/tableref/table_function_ref.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/parser/parsed_expression.hpp"
#include "sabot_sql/parser/tableref.hpp"
#include "sabot_sql/common/vector.hpp"
#include "sabot_sql/parser/statement/select_statement.hpp"
#include "sabot_sql/common/enums/ordinality_request_type.hpp"

namespace sabot_sql {
//! Represents a Table producing function
class TableFunctionRef : public TableRef {
public:
	static constexpr const TableReferenceType TYPE = TableReferenceType::TABLE_FUNCTION;

public:
	SABOT_SQL_API TableFunctionRef();

	unique_ptr<ParsedExpression> function;

	// if the function takes a subquery as argument its in here
	unique_ptr<SelectStatement> subquery;

	//! Whether or not WITH ORDINALITY has been invoked
	OrdinalityType with_ordinality = OrdinalityType::WITHOUT_ORDINALITY;

public:
	string ToString() const override;

	bool Equals(const TableRef &other_p) const override;

	unique_ptr<TableRef> Copy() override;

	//! Deserializes a blob back into a BaseTableRef
	void Serialize(Serializer &serializer) const override;
	static unique_ptr<TableRef> Deserialize(Deserializer &source);
};
} // namespace sabot_sql
