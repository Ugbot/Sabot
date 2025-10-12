//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/catalog/dependency_list.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/catalog/catalog_entry_map.hpp"
#include "sabot_sql/common/types/hash.hpp"
#include "sabot_sql/common/enums/catalog_type.hpp"
#include "sabot_sql/catalog/dependency.hpp"

namespace sabot_sql {
class Catalog;
class CatalogEntry;
struct CreateInfo;
class SchemaCatalogEntry;
struct CatalogTransaction;
class LogicalDependencyList;

//! A minimal representation of a CreateInfo / CatalogEntry
//! enough to look up the entry inside SchemaCatalogEntry::GetEntry
struct LogicalDependency {
public:
	CatalogEntryInfo entry;
	string catalog;

public:
	explicit LogicalDependency(CatalogEntry &entry);
	LogicalDependency();
	LogicalDependency(optional_ptr<Catalog> catalog, CatalogEntryInfo entry, string catalog_str);
	bool operator==(const LogicalDependency &other) const;

public:
	void Serialize(Serializer &serializer) const;
	static LogicalDependency Deserialize(Deserializer &deserializer);
};

struct LogicalDependencyHashFunction {
	uint64_t operator()(const LogicalDependency &a) const;
};

struct LogicalDependencyEquality {
	bool operator()(const LogicalDependency &a, const LogicalDependency &b) const;
};

//! The LogicalDependencyList containing LogicalDependency objects, not looked up in the catalog yet
class LogicalDependencyList {
	using create_info_set_t =
	    unordered_set<LogicalDependency, LogicalDependencyHashFunction, LogicalDependencyEquality>;

public:
	SABOT_SQL_API void AddDependency(CatalogEntry &entry);
	SABOT_SQL_API void AddDependency(const LogicalDependency &entry);
	SABOT_SQL_API bool Contains(CatalogEntry &entry);

public:
	SABOT_SQL_API void VerifyDependencies(Catalog &catalog, const string &name);
	void Serialize(Serializer &serializer) const;
	static LogicalDependencyList Deserialize(Deserializer &deserializer);
	bool operator==(const LogicalDependencyList &other) const;
	const create_info_set_t &Set() const;

private:
	create_info_set_t set;
};

} // namespace sabot_sql
