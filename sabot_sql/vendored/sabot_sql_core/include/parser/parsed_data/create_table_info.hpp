//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/parser/parsed_data/create_table_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/parser/parsed_data/create_info.hpp"
#include "sabot_sql/common/unordered_set.hpp"
#include "sabot_sql/parser/column_definition.hpp"
#include "sabot_sql/parser/constraint.hpp"
#include "sabot_sql/parser/statement/select_statement.hpp"
#include "sabot_sql/catalog/catalog_entry/column_dependency_manager.hpp"
#include "sabot_sql/parser/column_list.hpp"

namespace sabot_sql {
class SchemaCatalogEntry;

struct CreateTableInfo : public CreateInfo {
	SABOT_SQL_API CreateTableInfo();
	SABOT_SQL_API CreateTableInfo(string catalog, string schema, string name);
	SABOT_SQL_API CreateTableInfo(SchemaCatalogEntry &schema, string name);

	//! Table name to insert to
	string table;
	//! List of columns of the table
	ColumnList columns;
	//! List of constraints on the table
	vector<unique_ptr<Constraint>> constraints;
	//! CREATE TABLE as QUERY
	unique_ptr<SelectStatement> query;

public:
	SABOT_SQL_API unique_ptr<CreateInfo> Copy() const override;

	SABOT_SQL_API void Serialize(Serializer &serializer) const override;
	SABOT_SQL_API static unique_ptr<CreateInfo> Deserialize(Deserializer &deserializer);

	string ToString() const override;
};

} // namespace sabot_sql
