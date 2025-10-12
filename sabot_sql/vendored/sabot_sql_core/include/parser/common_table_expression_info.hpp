//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/parser/common_table_expression_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/common/vector.hpp"
#include "sabot_sql/common/enums/cte_materialize.hpp"

namespace sabot_sql {

class SelectStatement;

struct CommonTableExpressionInfo {
	vector<string> aliases;
	vector<unique_ptr<ParsedExpression>> key_targets;
	unique_ptr<SelectStatement> query;
	CTEMaterialize materialized = CTEMaterialize::CTE_MATERIALIZE_DEFAULT;

	void Serialize(Serializer &serializer) const;
	static unique_ptr<CommonTableExpressionInfo> Deserialize(Deserializer &deserializer);
	unique_ptr<CommonTableExpressionInfo> Copy();

	~CommonTableExpressionInfo();
};

} // namespace sabot_sql
