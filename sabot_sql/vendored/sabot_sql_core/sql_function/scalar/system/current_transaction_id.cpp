#include "sabot_sql/function/scalar/system_functions.hpp"
#include "sabot_sql/execution/expression_executor.hpp"
#include "sabot_sql/main/client_data.hpp"
#include "sabot_sql/planner/expression/bound_function_expression.hpp"
#include "sabot_sql/common/types/value.hpp"

#include "utf8proc.hpp"

namespace sabot_sql {

namespace {

struct CurrentTransactionIdData : FunctionData {
	explicit CurrentTransactionIdData(Value transaction_id_p) : transaction_id(std::move(transaction_id_p)) {
	}
	Value transaction_id;

	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<CurrentTransactionIdData>(transaction_id);
	}
	bool Equals(const FunctionData &other_p) const override {
		return transaction_id == other_p.Cast<CurrentTransactionIdData>().transaction_id;
	}
};

unique_ptr<FunctionData> CurrentTransactionIdBind(ClientContext &context, ScalarFunction &bound_function,
                                                  vector<unique_ptr<Expression>> &arguments) {
	Value transaction_id;
	if (context.transaction.HasActiveTransaction()) {
		transaction_id = Value::UBIGINT(context.transaction.ActiveTransaction().global_transaction_id);
	} else {
		transaction_id = Value();
	}
	return make_uniq<CurrentTransactionIdData>(transaction_id);
}

void CurrentTransactionIdFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &func_expr = state.expr.Cast<BoundFunctionExpression>();
	const auto &info = func_expr.bind_info->Cast<CurrentTransactionIdData>();
	result.Reference(info.transaction_id);
}

} // namespace

ScalarFunction CurrentTransactionId::GetFunction() {
	return ScalarFunction({}, LogicalType::UBIGINT, CurrentTransactionIdFunction, CurrentTransactionIdBind, nullptr,
	                      nullptr, nullptr, LogicalType(LogicalTypeId::INVALID), FunctionStability::VOLATILE);
}

} // namespace sabot_sql
