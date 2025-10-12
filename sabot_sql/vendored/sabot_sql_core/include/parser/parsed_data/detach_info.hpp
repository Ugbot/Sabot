//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/parser/parsed_data/detach_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/parser/parsed_data/parse_info.hpp"
#include "sabot_sql/common/enums/on_entry_not_found.hpp"

namespace sabot_sql {

struct DetachInfo : public ParseInfo {
public:
	static constexpr const ParseInfoType TYPE = ParseInfoType::DETACH_INFO;

public:
	DetachInfo();

	//! The alias of the attached database
	string name;
	//! Whether to throw an exception if alias is not found
	OnEntryNotFound if_not_found;

public:
	unique_ptr<DetachInfo> Copy() const;
	string ToString() const;

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<ParseInfo> Deserialize(Deserializer &deserializer);
};
} // namespace sabot_sql
