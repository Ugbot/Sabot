#include "sabot_sql/common/exception.hpp"
#include "sabot_sql/parser/tableref/table_function_ref.hpp"
#include "sabot_sql/parser/transformer.hpp"

namespace sabot_sql {

unique_ptr<TableRef> Transformer::TransformRangeFunction(sabot_sql_libpgquery::PGRangeFunction &root) {
	if (root.is_rowsfrom) {
		throw NotImplementedException("ROWS FROM() not implemented");
	}
	if (root.functions->length != 1) {
		throw NotImplementedException("Need exactly one function");
	}
	auto function_sublist = PGPointerCast<sabot_sql_libpgquery::PGList>(root.functions->head->data.ptr_value);
	D_ASSERT(function_sublist->length == 2);
	auto call_tree = PGPointerCast<sabot_sql_libpgquery::PGNode>(function_sublist->head->data.ptr_value);
	auto coldef = function_sublist->head->next->data.ptr_value;

	if (coldef) {
		throw NotImplementedException("Explicit column definition not supported yet");
	}
	// transform the function call
	auto result = make_uniq<TableFunctionRef>();
	if (root.ordinality) {
		result->with_ordinality = OrdinalityType::WITH_ORDINALITY;
	}
	switch (call_tree->type) {
	case sabot_sql_libpgquery::T_PGFuncCall: {
		auto func_call = PGPointerCast<sabot_sql_libpgquery::PGFuncCall>(call_tree.get());
		result->function = TransformFuncCall(*func_call);
		SetQueryLocation(*result, func_call->location);
		break;
	}
	case sabot_sql_libpgquery::T_PGSQLValueFunction:
		result->function =
		    TransformSQLValueFunction(*PGPointerCast<sabot_sql_libpgquery::PGSQLValueFunction>(call_tree.get()));
		break;
	default:
		throw ParserException("Not a function call or value function");
	}
	result->alias = TransformAlias(root.alias, result->column_name_alias);
	if (root.sample) {
		result->sample = TransformSampleOptions(root.sample);
	}
	return std::move(result);
}

} // namespace sabot_sql
