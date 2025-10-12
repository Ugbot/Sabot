#pragma once

#include "sabot_sql/catalog/catalog_set.hpp"
#include "sabot_sql/catalog/dependency_manager.hpp"

namespace sabot_sql {

//! This class mocks the CatalogSet interface, but does not actually store CatalogEntries
class DependencyCatalogSet {
public:
	DependencyCatalogSet(CatalogSet &set, const CatalogEntryInfo &info)
	    : set(set), info(info), mangled_name(DependencyManager::MangleName(info)) {
	}

public:
	bool CreateEntry(CatalogTransaction transaction, const MangledEntryName &name, unique_ptr<CatalogEntry> value);
	CatalogSet::EntryLookup GetEntryDetailed(CatalogTransaction transaction, const MangledEntryName &name);
	optional_ptr<CatalogEntry> GetEntry(CatalogTransaction transaction, const MangledEntryName &name);
	void Scan(CatalogTransaction transaction, const std::function<void(CatalogEntry &)> &callback);
	bool DropEntry(CatalogTransaction transaction, const MangledEntryName &name, bool cascade,
	               bool allow_drop_internal = false);

private:
	MangledDependencyName ApplyPrefix(const MangledEntryName &name) const;

public:
	CatalogSet &set;
	CatalogEntryInfo info;
	MangledEntryName mangled_name;
};

} // namespace sabot_sql
