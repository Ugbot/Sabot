//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/parser/parsed_data/create_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/common/enums/catalog_type.hpp"
#include "sabot_sql/parser/parsed_data/parse_info.hpp"
#include "sabot_sql/common/enum_util.hpp"
#include "sabot_sql/common/enums/on_create_conflict.hpp"
#include "sabot_sql/common/types/value.hpp"
#include "sabot_sql/catalog/dependency_list.hpp"

namespace sabot_sql {
struct AlterInfo;

struct CreateInfo : public ParseInfo {
public:
	static constexpr const ParseInfoType TYPE = ParseInfoType::CREATE_INFO;

public:
	explicit CreateInfo(CatalogType type, string schema = DEFAULT_SCHEMA, string catalog_p = INVALID_CATALOG)
	    : ParseInfo(TYPE), type(type), catalog(std::move(catalog_p)), schema(std::move(schema)),
	      on_conflict(OnCreateConflict::ERROR_ON_CONFLICT), temporary(false), internal(false) {
	}
	~CreateInfo() override {
	}

	//! The to-be-created catalog type
	CatalogType type;
	//! The catalog name of the entry
	string catalog;
	//! The schema name of the entry
	string schema;
	//! What to do on create conflict
	OnCreateConflict on_conflict;
	//! Whether or not the entry is temporary
	bool temporary;
	//! Whether or not the entry is an internal entry
	bool internal;
	//! The SQL string of the CREATE statement
	string sql;
	//! The inherent dependencies of the created entry
	LogicalDependencyList dependencies;
	//! User provided comment
	Value comment;
	//! Key-value tags with additional metadata
	InsertionOrderPreservingMap<string> tags;

public:
	void Serialize(Serializer &serializer) const override;
	static unique_ptr<CreateInfo> Deserialize(Deserializer &deserializer);

	virtual unique_ptr<CreateInfo> Copy() const = 0;

	SABOT_SQL_API void CopyProperties(CreateInfo &other) const;
	//! Generates an alter statement from the create statement - used for OnCreateConflict::ALTER_ON_CONFLICT
	SABOT_SQL_API virtual unique_ptr<AlterInfo> GetAlterInfo() const;
	//! Returns a string like "CREATE (OR REPLACE) (TEMPORARY) <entry> (IF NOT EXISTS) " for TABLE/VIEW/TYPE/MACRO
	SABOT_SQL_API string GetCreatePrefix(const string &entry) const;

	virtual string ToString() const {
		throw NotImplementedException("ToString not supported for this type of CreateInfo: '%s'",
		                              EnumUtil::ToString(info_type));
	}
};

} // namespace sabot_sql
