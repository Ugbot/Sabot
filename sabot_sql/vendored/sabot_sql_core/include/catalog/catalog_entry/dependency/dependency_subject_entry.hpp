//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/catalog/catalog_entry.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/catalog/catalog_entry/dependency/dependency_entry.hpp"

namespace sabot_sql {

class DependencySubjectEntry : public DependencyEntry {
public:
	~DependencySubjectEntry() override;
	DependencySubjectEntry(Catalog &catalog, const DependencyInfo &info);

public:
	const CatalogEntryInfo &EntryInfo() const override;
	const MangledEntryName &EntryMangledName() const override;
	const CatalogEntryInfo &SourceInfo() const override;
	const MangledEntryName &SourceMangledName() const override;
};

} // namespace sabot_sql
