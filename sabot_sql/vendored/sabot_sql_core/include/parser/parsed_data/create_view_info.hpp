//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/parser/parsed_data/create_view_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/parser/parsed_data/create_info.hpp"
#include "sabot_sql/parser/statement/select_statement.hpp"

namespace sabot_sql {
class SchemaCatalogEntry;

struct CreateViewInfo : public CreateInfo {
public:
	CreateViewInfo();
	CreateViewInfo(SchemaCatalogEntry &schema, string view_name);
	CreateViewInfo(string catalog_p, string schema_p, string view_name);

public:
	//! View name
	string view_name;
	//! Aliases of the view
	vector<string> aliases;
	//! Return types
	vector<LogicalType> types;
	//! Names of the query
	vector<string> names;
	//! Comments on columns of the query. Note: vector can be empty when no comments are set
	vector<Value> column_comments;
	//! The SelectStatement of the view
	unique_ptr<SelectStatement> query;

public:
	unique_ptr<CreateInfo> Copy() const override;

	//! Gets a bound CreateViewInfo object from a SELECT statement and a view name, schema name, etc
	SABOT_SQL_API static unique_ptr<CreateViewInfo> FromSelect(ClientContext &context, unique_ptr<CreateViewInfo> info);
	//! Gets a bound CreateViewInfo object from a CREATE VIEW statement
	SABOT_SQL_API static unique_ptr<CreateViewInfo> FromCreateView(ClientContext &context, SchemaCatalogEntry &schema,
	                                                            const string &sql);
	//! Parse a SELECT statement from a SQL string
	SABOT_SQL_API static unique_ptr<SelectStatement> ParseSelect(const string &sql);

	SABOT_SQL_API void Serialize(Serializer &serializer) const override;
	SABOT_SQL_API static unique_ptr<CreateInfo> Deserialize(Deserializer &deserializer);

	string ToString() const override;
};

} // namespace sabot_sql
