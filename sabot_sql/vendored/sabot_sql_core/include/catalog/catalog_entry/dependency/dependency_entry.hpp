//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/catalog/catalog_entry.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/common/common.hpp"
#include "sabot_sql/common/enums/catalog_type.hpp"
#include "sabot_sql/common/exception.hpp"
#include "sabot_sql/common/atomic.hpp"
#include "sabot_sql/common/optional_ptr.hpp"
#include "sabot_sql/catalog/catalog_entry.hpp"
#include "sabot_sql/catalog/catalog_set.hpp"
#include "sabot_sql/catalog/dependency.hpp"
#include "sabot_sql/catalog/dependency_manager.hpp"
#include <memory>

namespace sabot_sql {

class DependencyManager;

class DependencySetCatalogEntry;

//! Resembles a connection between an object and the CatalogEntry that can be retrieved from the Catalog using the
//! identifiers listed here

enum class DependencyEntryType : uint8_t { SUBJECT, DEPENDENT };

class DependencyEntry : public InCatalogEntry {
public:
	~DependencyEntry() override;

protected:
	DependencyEntry(Catalog &catalog, DependencyEntryType type, const MangledDependencyName &name,
	                const DependencyInfo &info);

public:
	const MangledEntryName &SubjectMangledName() const;
	const DependencySubject &Subject() const;

	const MangledEntryName &DependentMangledName() const;
	const DependencyDependent &Dependent() const;

	virtual const CatalogEntryInfo &EntryInfo() const = 0;
	virtual const MangledEntryName &EntryMangledName() const = 0;
	virtual const CatalogEntryInfo &SourceInfo() const = 0;
	virtual const MangledEntryName &SourceMangledName() const = 0;

public:
	DependencyEntryType Side() const;

protected:
	const MangledEntryName dependent_name;
	const MangledEntryName subject_name;
	const DependencyDependent dependent;
	const DependencySubject subject;

private:
	DependencyEntryType side;
};

} // namespace sabot_sql
