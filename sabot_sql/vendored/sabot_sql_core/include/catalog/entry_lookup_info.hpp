//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/catalog/entry_lookup_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/catalog/catalog_entry.hpp"
#include "sabot_sql/common/error_data.hpp"
#include "sabot_sql/parser/query_error_context.hpp"

namespace sabot_sql {
class BoundAtClause;

struct EntryLookupInfo {
public:
	EntryLookupInfo(CatalogType catalog_type, const string &name,
	                QueryErrorContext error_context = QueryErrorContext());
	EntryLookupInfo(CatalogType catalog_type, const string &name, optional_ptr<BoundAtClause> at_clause,
	                QueryErrorContext error_context);
	EntryLookupInfo(const EntryLookupInfo &parent, const string &name);
	EntryLookupInfo(const EntryLookupInfo &parent, optional_ptr<BoundAtClause> at_clause);

public:
	CatalogType GetCatalogType() const;
	const string &GetEntryName() const;
	const QueryErrorContext &GetErrorContext() const;
	const optional_ptr<BoundAtClause> GetAtClause() const;

	static EntryLookupInfo SchemaLookup(const EntryLookupInfo &parent, const string &schema_name);

private:
	CatalogType catalog_type;
	const string &name;
	optional_ptr<BoundAtClause> at_clause;
	QueryErrorContext error_context;
};

//! Return value of Catalog::LookupEntry
struct CatalogEntryLookup {
	optional_ptr<SchemaCatalogEntry> schema;
	optional_ptr<CatalogEntry> entry;
	ErrorData error;

	SABOT_SQL_API bool Found() const {
		return entry;
	}
};

} // namespace sabot_sql
