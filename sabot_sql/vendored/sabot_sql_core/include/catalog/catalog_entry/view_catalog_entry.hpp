//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/catalog/catalog_entry/view_catalog_entry.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/catalog/standard_entry.hpp"
#include "sabot_sql/parser/statement/select_statement.hpp"
#include "sabot_sql/common/types.hpp"
#include "sabot_sql/common/vector.hpp"

namespace sabot_sql {

class DataTable;
struct CreateViewInfo;

//! A view catalog entry
class ViewCatalogEntry : public StandardEntry {
public:
	static constexpr const CatalogType Type = CatalogType::VIEW_ENTRY;
	static constexpr const char *Name = "view";

public:
	//! Create a real TableCatalogEntry and initialize storage for it
	ViewCatalogEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateViewInfo &info);

	//! The query of the view
	unique_ptr<SelectStatement> query;
	//! The SQL query (if any)
	string sql;
	//! The set of aliases associated with the view
	vector<string> aliases;
	//! The returned types of the view
	vector<LogicalType> types;
	//! The returned names of the view
	vector<string> names;
	//! The comments on the columns of the view: can be empty if there are no comments
	vector<Value> column_comments;

public:
	unique_ptr<CreateInfo> GetInfo() const override;

	unique_ptr<CatalogEntry> AlterEntry(ClientContext &context, AlterInfo &info) override;

	unique_ptr<CatalogEntry> Copy(ClientContext &context) const override;

	virtual const SelectStatement &GetQuery();

	virtual bool HasTypes() const {
		// Whether or not the view has types/names defined
		return true;
	}

	string ToSQL() const override;

private:
	void Initialize(CreateViewInfo &info);
};
} // namespace sabot_sql
