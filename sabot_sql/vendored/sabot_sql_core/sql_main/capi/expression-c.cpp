#include "sabot_sql/main/capi/capi_internal.hpp"

#include "sabot_sql/execution/expression_executor.hpp"

using sabot_sql::CClientContextWrapper;
using sabot_sql::ExpressionWrapper;

void sabot_sql_destroy_expression(sabot_sql_expression *expr) {
	if (!expr || !*expr) {
		return;
	}
	auto wrapper = reinterpret_cast<ExpressionWrapper *>(*expr);
	delete wrapper;
	*expr = nullptr;
}

sabot_sql_logical_type sabot_sql_expression_return_type(sabot_sql_expression expr) {
	if (!expr) {
		return nullptr;
	}
	auto wrapper = reinterpret_cast<ExpressionWrapper *>(expr);
	auto logical_type = new sabot_sql::LogicalType(wrapper->expr->return_type);
	return reinterpret_cast<sabot_sql_logical_type>(logical_type);
}

bool sabot_sql_expression_is_foldable(sabot_sql_expression expr) {
	if (!expr) {
		return false;
	}
	auto wrapper = reinterpret_cast<ExpressionWrapper *>(expr);
	return wrapper->expr->IsFoldable();
}

sabot_sql_error_data sabot_sql_expression_fold(sabot_sql_client_context context, sabot_sql_expression expr,
                                         sabot_sql_value *out_value) {
	if (!expr || !sabot_sql_expression_is_foldable(expr)) {
		return nullptr;
	}

	auto value = new sabot_sql::Value;
	try {
		auto context_wrapper = reinterpret_cast<CClientContextWrapper *>(context);
		auto expr_wrapper = reinterpret_cast<ExpressionWrapper *>(expr);
		*value = sabot_sql::ExpressionExecutor::EvaluateScalar(context_wrapper->context, *expr_wrapper->expr);
		*out_value = reinterpret_cast<sabot_sql_value>(value);
	} catch (const sabot_sql::Exception &ex) {
		delete value;
		return sabot_sql_create_error_data(SABOT_SQL_ERROR_INVALID_INPUT, ex.what());
	} catch (const std::exception &ex) {
		delete value;
		return sabot_sql_create_error_data(SABOT_SQL_ERROR_INVALID_INPUT, ex.what());
	} catch (...) {
		delete value;
		return sabot_sql_create_error_data(SABOT_SQL_ERROR_INVALID_INPUT, "unknown error occurred during folding");
	}
	return nullptr;
}
