#include "sabot_sql/function/scalar/system_functions.hpp"
#include "sabot_sql/execution/expression_executor.hpp"
#include "sabot_sql/main/client_data.hpp"
#include "sabot_sql/planner/expression/bound_function_expression.hpp"

#include "utf8proc.hpp"

namespace sabot_sql {

namespace {

struct CurrentQueryIdData : FunctionData {
	explicit CurrentQueryIdData(Value query_id_p) : query_id(std::move(query_id_p)) {
	}
	Value query_id;

	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<CurrentQueryIdData>(query_id);
	}
	bool Equals(const FunctionData &other_p) const override {
		return query_id == other_p.Cast<CurrentQueryIdData>().query_id;
	}
};

unique_ptr<FunctionData> CurrentQueryIdBind(ClientContext &context, ScalarFunction &bound_function,
                                            vector<unique_ptr<Expression>> &arguments) {
	Value query_id;
	if (context.transaction.HasActiveTransaction()) {
		query_id = Value::UBIGINT(context.transaction.GetActiveQuery());
	} else {
		query_id = Value();
	}
	return make_uniq<CurrentQueryIdData>(query_id);
}

void CurrentQueryIdFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &func_expr = state.expr.Cast<BoundFunctionExpression>();
	const auto &info = func_expr.bind_info->Cast<CurrentQueryIdData>();
	result.Reference(info.query_id);
}

} // namespace

ScalarFunction CurrentQueryId::GetFunction() {
	return ScalarFunction({}, LogicalType::UBIGINT, CurrentQueryIdFunction, CurrentQueryIdBind, nullptr, nullptr,
	                      nullptr, LogicalType(LogicalTypeId::INVALID), FunctionStability::VOLATILE);
}

} // namespace sabot_sql
