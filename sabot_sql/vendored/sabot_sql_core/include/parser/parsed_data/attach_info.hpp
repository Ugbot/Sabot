//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/parser/parsed_data/attach_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/parser/parsed_data/parse_info.hpp"
#include "sabot_sql/common/unordered_map.hpp"
#include "sabot_sql/common/types/value.hpp"
#include "sabot_sql/common/enums/on_create_conflict.hpp"
#include "sabot_sql/parser/parsed_expression.hpp"

namespace sabot_sql {

struct AttachInfo : public ParseInfo {
public:
	static constexpr const ParseInfoType TYPE = ParseInfoType::ATTACH_INFO;

public:
	AttachInfo() : ParseInfo(TYPE) {
	}

	//! The alias of the attached database
	string name;
	//! The path to the attached database
	string path;
	//! Set of (key, value) options
	case_insensitive_map_t<unique_ptr<ParsedExpression>> parsed_options;
	//! Set of bound (key, value) options
	unordered_map<string, Value> options;
	//! What to do on create conflict
	OnCreateConflict on_conflict = OnCreateConflict::ERROR_ON_CONFLICT;

public:
	//! Copies this AttachInfo and returns an unique pointer to the new AttachInfo.
	unique_ptr<AttachInfo> Copy() const;
	string ToString() const;

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<ParseInfo> Deserialize(Deserializer &deserializer);
};

} // namespace sabot_sql
