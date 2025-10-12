//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/parser/qualified_name_set.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/parser/qualified_name.hpp"
#include "sabot_sql/common/types/hash.hpp"
#include "sabot_sql/common/unordered_set.hpp"

namespace sabot_sql {

struct QualifiedColumnHashFunction {
	uint64_t operator()(const QualifiedColumnName &a) const {
		// hash only on the column name - since we match based on the shortest possible match
		return StringUtil::CIHash(a.column);
	}
};

struct QualifiedColumnEquality {
	bool operator()(const QualifiedColumnName &a, const QualifiedColumnName &b) const {
		// qualified column names follow a prefix comparison
		// so "tbl.i"  and "i" are equivalent, as are "schema.tbl.i" and "i"
		// but "tbl.i" and "tbl2.i" are not equivalent
		if (!a.catalog.empty() && !b.catalog.empty() && !StringUtil::CIEquals(a.catalog, b.catalog)) {
			return false;
		}
		if (!a.schema.empty() && !b.schema.empty() && !StringUtil::CIEquals(a.schema, b.schema)) {
			return false;
		}
		if (!a.table.empty() && !b.table.empty() && !StringUtil::CIEquals(a.table, b.table)) {
			return false;
		}
		return StringUtil::CIEquals(a.column, b.column);
	}
};

using qualified_column_set_t = unordered_set<QualifiedColumnName, QualifiedColumnHashFunction, QualifiedColumnEquality>;

template <class T>
using qualified_column_map_t =
    unordered_map<QualifiedColumnName, T, QualifiedColumnHashFunction, QualifiedColumnEquality>;

} // namespace sabot_sql
