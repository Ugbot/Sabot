//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/catalog/catalog_search_path.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <functional>
#include "sabot_sql/common/enums/catalog_type.hpp"
#include "sabot_sql/common/string.hpp"
#include "sabot_sql/common/vector.hpp"
#include "sabot_sql/common/types/value.hpp"

namespace sabot_sql {

class ClientContext;

struct CatalogSearchEntry {
	CatalogSearchEntry(string catalog, string schema);

	string catalog;
	string schema;

public:
	string ToString() const;
	static string ListToString(const vector<CatalogSearchEntry> &input);
	static CatalogSearchEntry Parse(const string &input);
	static vector<CatalogSearchEntry> ParseList(const string &input);

private:
	static CatalogSearchEntry ParseInternal(const string &input, idx_t &pos);
	static string WriteOptionallyQuoted(const string &input);
};

enum class CatalogSetPathType { SET_SCHEMA, SET_SCHEMAS, SET_DIRECTLY };

//! The schema search path, in order by which entries are searched if no schema entry is provided
class CatalogSearchPath {
public:
	SABOT_SQL_API explicit CatalogSearchPath(ClientContext &client_p);
	SABOT_SQL_API CatalogSearchPath(ClientContext &client_p, vector<CatalogSearchEntry> entries);
	CatalogSearchPath(const CatalogSearchPath &other) = delete;

	SABOT_SQL_API void Set(CatalogSearchEntry new_value, CatalogSetPathType set_type);
	SABOT_SQL_API void Set(vector<CatalogSearchEntry> new_paths, CatalogSetPathType set_type);
	SABOT_SQL_API void Reset();

	SABOT_SQL_API const vector<CatalogSearchEntry> &Get() const;
	const vector<CatalogSearchEntry> &GetSetPaths() const {
		return set_paths;
	}
	SABOT_SQL_API const CatalogSearchEntry &GetDefault() const;
	//! FIXME: this method is deprecated
	SABOT_SQL_API string GetDefaultSchema(const string &catalog) const;
	SABOT_SQL_API string GetDefaultSchema(ClientContext &context, const string &catalog) const;
	SABOT_SQL_API string GetDefaultCatalog(const string &schema) const;

	SABOT_SQL_API vector<string> GetSchemasForCatalog(const string &catalog) const;
	SABOT_SQL_API vector<string> GetCatalogsForSchema(const string &schema) const;

	SABOT_SQL_API bool SchemaInSearchPath(ClientContext &context, const string &catalog_name,
	                                   const string &schema_name) const;

private:
	//! Set paths without checking if they exist
	void SetPathsInternal(vector<CatalogSearchEntry> new_paths);
	string GetSetName(CatalogSetPathType set_type);

private:
	ClientContext &context;
	vector<CatalogSearchEntry> paths;
	//! Only the paths that were explicitly set (minus the always included paths)
	vector<CatalogSearchEntry> set_paths;
};

} // namespace sabot_sql
