#include "sabot_sql/planner/binder.hpp"
#include "sabot_sql/parser/statement/attach_statement.hpp"
#include "sabot_sql/parser/tableref/table_function_ref.hpp"
#include "sabot_sql/planner/tableref/bound_table_function.hpp"
#include "sabot_sql/planner/operator/logical_simple.hpp"
#include "sabot_sql/planner/expression_binder/table_function_binder.hpp"
#include "sabot_sql/execution/expression_executor.hpp"

namespace sabot_sql {

BoundStatement Binder::Bind(AttachStatement &stmt) {
	BoundStatement result;
	result.types = {LogicalType::BOOLEAN};
	result.names = {"Success"};

	// bind the options
	TableFunctionBinder option_binder(*this, context, "Attach", "Attach parameter");
	unordered_map<string, Value> kv_options;
	for (auto &entry : stmt.info->parsed_options) {
		auto bound_expr = option_binder.Bind(entry.second);
		auto val = ExpressionExecutor::EvaluateScalar(context, *bound_expr);
		if (val.IsNull()) {
			throw BinderException("NULL is not supported as a valid option for ATTACH option \"" + entry.first + "\"");
		}
		stmt.info->options[entry.first] = std::move(val);
	}
	stmt.info->parsed_options.clear();

	result.plan = make_uniq<LogicalSimple>(LogicalOperatorType::LOGICAL_ATTACH, std::move(stmt.info));

	auto &properties = GetStatementProperties();
	properties.allow_stream_result = false;
	properties.return_type = StatementReturnType::NOTHING;
	return result;
}

} // namespace sabot_sql
