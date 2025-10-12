//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/parser/parsed_data/create_schema_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/parser/parsed_data/create_info.hpp"

namespace sabot_sql {

struct CreateSchemaInfo : public CreateInfo {
	CreateSchemaInfo();

public:
	SABOT_SQL_API void Serialize(Serializer &serializer) const override;
	SABOT_SQL_API static unique_ptr<CreateInfo> Deserialize(Deserializer &deserializer);

	unique_ptr<CreateInfo> Copy() const override;
	string ToString() const override;
};

} // namespace sabot_sql
