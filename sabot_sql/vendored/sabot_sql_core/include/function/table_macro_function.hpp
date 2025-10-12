//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/function/table_macro_function.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/function/macro_function.hpp"
#include "sabot_sql/parser/query_node.hpp"
#include "sabot_sql/function/function.hpp"
#include "sabot_sql/main/client_context.hpp"
#include "sabot_sql/planner/binder.hpp"
#include "sabot_sql/planner/expression_binder.hpp"
#include "sabot_sql/parser/expression/constant_expression.hpp"

namespace sabot_sql {

class TableMacroFunction : public MacroFunction {
public:
	static constexpr const MacroType TYPE = MacroType::TABLE_MACRO;

public:
	explicit TableMacroFunction(unique_ptr<QueryNode> query_node);
	TableMacroFunction(void);

	//! The main query node
	unique_ptr<QueryNode> query_node;

public:
	unique_ptr<MacroFunction> Copy() const override;

	string ToSQL() const override;

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<MacroFunction> Deserialize(Deserializer &deserializer);
};

} // namespace sabot_sql
