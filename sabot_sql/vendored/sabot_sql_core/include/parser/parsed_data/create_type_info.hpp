//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/parser/parsed_data/create_type_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/parser/parsed_data/create_info.hpp"
#include "sabot_sql/parser/column_definition.hpp"
#include "sabot_sql/parser/constraint.hpp"
#include "sabot_sql/parser/statement/select_statement.hpp"

namespace sabot_sql {

struct BindLogicalTypeInput {
	ClientContext &context;
	const LogicalType &base_type;
	const vector<Value> &modifiers;
};

//! The type to bind type modifiers to a type
typedef LogicalType (*bind_logical_type_function_t)(const BindLogicalTypeInput &input);

struct CreateTypeInfo : public CreateInfo {
	CreateTypeInfo();
	CreateTypeInfo(string name_p, LogicalType type_p, bind_logical_type_function_t bind_function_p = nullptr);

	//! Name of the Type
	string name;
	//! Logical Type
	LogicalType type;
	//! Used by create enum from query
	unique_ptr<SQLStatement> query;
	//! Bind type modifiers to the type
	bind_logical_type_function_t bind_function;

public:
	unique_ptr<CreateInfo> Copy() const override;

	SABOT_SQL_API void Serialize(Serializer &serializer) const override;
	SABOT_SQL_API static unique_ptr<CreateInfo> Deserialize(Deserializer &deserializer);

	string ToString() const override;
};

} // namespace sabot_sql
