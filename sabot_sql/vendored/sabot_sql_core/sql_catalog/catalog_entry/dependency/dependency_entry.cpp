#include "sabot_sql/catalog/catalog_entry/dependency/dependency_entry.hpp"
#include "sabot_sql/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "sabot_sql/catalog/dependency_manager.hpp"
#include "sabot_sql/catalog/catalog.hpp"

namespace sabot_sql {

DependencyEntry::DependencyEntry(Catalog &catalog, DependencyEntryType side, const MangledDependencyName &name,
                                 const DependencyInfo &info)
    : InCatalogEntry(CatalogType::DEPENDENCY_ENTRY, catalog, name.name),
      dependent_name(DependencyManager::MangleName(info.dependent.entry)),
      subject_name(DependencyManager::MangleName(info.subject.entry)), dependent(info.dependent), subject(info.subject),
      side(side) {
	D_ASSERT(info.dependent.entry.type != CatalogType::DEPENDENCY_ENTRY);
	D_ASSERT(info.subject.entry.type != CatalogType::DEPENDENCY_ENTRY);
	if (catalog.IsTemporaryCatalog()) {
		temporary = true;
	}
}

const MangledEntryName &DependencyEntry::SubjectMangledName() const {
	return subject_name;
}

const DependencySubject &DependencyEntry::Subject() const {
	return subject;
}

const MangledEntryName &DependencyEntry::DependentMangledName() const {
	return dependent_name;
}

const DependencyDependent &DependencyEntry::Dependent() const {
	return dependent;
}

DependencyEntry::~DependencyEntry() {
}

DependencyEntryType DependencyEntry::Side() const {
	return side;
}

} // namespace sabot_sql
