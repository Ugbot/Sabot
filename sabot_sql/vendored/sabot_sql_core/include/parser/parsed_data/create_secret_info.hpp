//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/parser/parsed_data/create_secret_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/main/secret/secret.hpp"
#include "sabot_sql/common/enums/catalog_type.hpp"
#include "sabot_sql/parser/column_definition.hpp"
#include "sabot_sql/parser/parsed_data/parse_info.hpp"
#include "sabot_sql/parser/parsed_data/create_info.hpp"
#include "sabot_sql/common/enums/on_entry_not_found.hpp"
#include "sabot_sql/common/named_parameter_map.hpp"

namespace sabot_sql {

struct CreateSecretInfo : public CreateInfo { // NOLINT: work-around bug in clang-tidy
public:
	static constexpr const ParseInfoType TYPE = ParseInfoType::CREATE_SECRET_INFO;

public:
	explicit CreateSecretInfo(OnCreateConflict on_conflict, SecretPersistType persist_type);
	~CreateSecretInfo() override;

	//! How to handle conflict
	OnCreateConflict on_conflict;
	//! Whether the secret can be persisted
	SecretPersistType persist_type;
	//! The type of secret
	unique_ptr<ParsedExpression> type;
	//! Which storage to use (empty for default)
	string storage_type;
	//! (optionally) the provider of the secret credentials
	unique_ptr<ParsedExpression> provider;
	//! (optionally) the name of the secret
	string name;
	//! (optionally) the scope of the secret
	unique_ptr<ParsedExpression> scope;
	//! Named parameter list (if any)
	case_insensitive_map_t<unique_ptr<ParsedExpression>> options;

	unique_ptr<CreateInfo> Copy() const override;
};
} // namespace sabot_sql
