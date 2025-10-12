//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/function/table_macro_function.hpp
//
//
//===----------------------------------------------------------------------===//
//! The SelectStatement of the view
#include "sabot_sql/function/table_macro_function.hpp"

#include "sabot_sql/parser/expression/constant_expression.hpp"
#include "sabot_sql/parser/query_node.hpp"

namespace sabot_sql {

TableMacroFunction::TableMacroFunction(unique_ptr<QueryNode> query_node)
    : MacroFunction(MacroType::TABLE_MACRO), query_node(std::move(query_node)) {
}

TableMacroFunction::TableMacroFunction(void) : MacroFunction(MacroType::TABLE_MACRO) {
}

unique_ptr<MacroFunction> TableMacroFunction::Copy() const {
	auto result = make_uniq<TableMacroFunction>();
	result->query_node = query_node->Copy();
	this->CopyProperties(*result);
	return std::move(result);
}

string TableMacroFunction::ToSQL() const {
	return MacroFunction::ToSQL() + StringUtil::Format("TABLE (%s)", query_node->ToString());
}

} // namespace sabot_sql
