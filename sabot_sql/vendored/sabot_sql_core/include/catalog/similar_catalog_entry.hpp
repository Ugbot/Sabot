//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/catalog/similar_catalog_entry.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/common/common.hpp"
#include "sabot_sql/common/optional_ptr.hpp"

namespace sabot_sql {
class SchemaCatalogEntry;

//! Return value of SimilarEntryInSchemas
struct SimilarCatalogEntry {
	//! The entry name. Empty if absent
	string name;
	//! The similarity score of the given name (between 0.0 and 1.0, higher is better)
	double score = 0.0;
	//! The schema of the entry.
	optional_ptr<SchemaCatalogEntry> schema;

	bool Found() const {
		return !name.empty();
	}

	SABOT_SQL_API string GetQualifiedName(bool qualify_catalog, bool qualify_schema) const;
};

} // namespace sabot_sql
