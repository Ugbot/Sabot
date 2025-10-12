//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/common/exception/catalog_exception.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/common/exception.hpp"
#include "sabot_sql/common/enums/catalog_type.hpp"
#include "sabot_sql/parser/query_error_context.hpp"
#include "sabot_sql/common/unordered_map.hpp"

namespace sabot_sql {
struct EntryLookupInfo;

class CatalogException : public Exception {
public:
	SABOT_SQL_API explicit CatalogException(const string &msg);
	SABOT_SQL_API explicit CatalogException(const string &msg, const unordered_map<string, string> &extra_info);

	template <typename... ARGS>
	explicit CatalogException(const string &msg, ARGS... params) : CatalogException(ConstructMessage(msg, params...)) {
	}
	template <typename... ARGS>
	explicit CatalogException(QueryErrorContext error_context, const string &msg, ARGS... params)
	    : CatalogException(ConstructMessage(msg, params...), Exception::InitializeExtraInfo(error_context)) {
	}

	static CatalogException MissingEntry(const EntryLookupInfo &lookup_info, const string &suggestion);
	static CatalogException MissingEntry(CatalogType type, const string &name, const string &suggestion,
	                                     QueryErrorContext context = QueryErrorContext());
	static CatalogException MissingEntry(const string &type, const string &name, const vector<string> &suggestions,
	                                     QueryErrorContext context = QueryErrorContext());
	static CatalogException EntryAlreadyExists(CatalogType type, const string &name,
	                                           QueryErrorContext context = QueryErrorContext());
};

} // namespace sabot_sql
