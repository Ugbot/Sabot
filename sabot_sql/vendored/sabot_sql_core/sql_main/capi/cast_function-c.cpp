#include "sabot_sql/common/type_visitor.hpp"
#include "sabot_sql/common/types.hpp"
#include "sabot_sql/common/helper.hpp"
#include "sabot_sql/common/operator/cast_operators.hpp"
#include "sabot_sql/function/cast/cast_function_set.hpp"
#include "sabot_sql/main/capi/capi_internal.hpp"

namespace sabot_sql {
struct CCastExecuteInfo {
	CastParameters &parameters;
	string error_message;

	explicit CCastExecuteInfo(CastParameters &parameters) : parameters(parameters), error_message() {
	}
};

struct CCastFunction {
	unique_ptr<LogicalType> source_type;
	unique_ptr<LogicalType> target_type;
	int64_t implicit_cast_cost = -1;

	sabot_sql_cast_function_t function;
	sabot_sql_function_info extra_info = nullptr;
	sabot_sql_delete_callback_t delete_callback = nullptr;
};

struct CCastFunctionUserData {

	sabot_sql_function_info data_ptr = nullptr;
	sabot_sql_delete_callback_t delete_callback = nullptr;

	CCastFunctionUserData(sabot_sql_function_info data_ptr_p, sabot_sql_delete_callback_t delete_callback_p)
	    : data_ptr(data_ptr_p), delete_callback(delete_callback_p) {
	}

	~CCastFunctionUserData() {
		if (data_ptr && delete_callback) {
			delete_callback(data_ptr);
		}
		data_ptr = nullptr;
		delete_callback = nullptr;
	}
};

struct CCastFunctionData final : public BoundCastData {
	sabot_sql_cast_function_t function;
	shared_ptr<CCastFunctionUserData> extra_info;

	explicit CCastFunctionData(sabot_sql_cast_function_t function_p, shared_ptr<CCastFunctionUserData> extra_info_p)
	    : function(function_p), extra_info(std::move(extra_info_p)) {
	}

	unique_ptr<BoundCastData> Copy() const override {
		return make_uniq<CCastFunctionData>(function, extra_info);
	}
};

static bool CAPICastFunction(Vector &input, Vector &output, idx_t count, CastParameters &parameters) {

	const auto is_const = input.GetVectorType() == VectorType::CONSTANT_VECTOR;
	input.Flatten(count);

	CCastExecuteInfo exec_info(parameters);
	const auto &data = parameters.cast_data->Cast<CCastFunctionData>();

	auto c_input = reinterpret_cast<sabot_sql_vector>(&input);
	auto c_output = reinterpret_cast<sabot_sql_vector>(&output);
	auto c_info = reinterpret_cast<sabot_sql_function_info>(&exec_info);

	const auto success = data.function(c_info, count, c_input, c_output);

	if (!success) {
		HandleCastError::AssignError(exec_info.error_message, parameters);
	}

	if (is_const && count == 1 && (success || !parameters.strict)) {
		output.SetVectorType(VectorType::CONSTANT_VECTOR);
	}

	return success;
}

} // namespace sabot_sql

sabot_sql_cast_function sabot_sql_create_cast_function() {
	const auto function = new sabot_sql::CCastFunction();
	return reinterpret_cast<sabot_sql_cast_function>(function);
}

void sabot_sql_cast_function_set_source_type(sabot_sql_cast_function cast_function, sabot_sql_logical_type source_type) {
	if (!cast_function || !source_type) {
		return;
	}
	const auto &logical_type = *(reinterpret_cast<sabot_sql::LogicalType *>(source_type));
	auto &cast = *(reinterpret_cast<sabot_sql::CCastFunction *>(cast_function));
	cast.source_type = sabot_sql::make_uniq<sabot_sql::LogicalType>(logical_type);
}

void sabot_sql_cast_function_set_target_type(sabot_sql_cast_function cast_function, sabot_sql_logical_type target_type) {
	if (!cast_function || !target_type) {
		return;
	}
	const auto &logical_type = *(reinterpret_cast<sabot_sql::LogicalType *>(target_type));
	auto &cast = *(reinterpret_cast<sabot_sql::CCastFunction *>(cast_function));
	cast.target_type = sabot_sql::make_uniq<sabot_sql::LogicalType>(logical_type);
}

void sabot_sql_cast_function_set_implicit_cast_cost(sabot_sql_cast_function cast_function, int64_t cost) {
	if (!cast_function) {
		return;
	}
	auto &custom_type = *(reinterpret_cast<sabot_sql::CCastFunction *>(cast_function));
	custom_type.implicit_cast_cost = cost;
}

void sabot_sql_cast_function_set_function(sabot_sql_cast_function cast_function, sabot_sql_cast_function_t function) {
	if (!cast_function || !function) {
		return;
	}
	auto &cast = *(reinterpret_cast<sabot_sql::CCastFunction *>(cast_function));
	cast.function = function;
}

sabot_sql_cast_mode sabot_sql_cast_function_get_cast_mode(sabot_sql_function_info info) {
	const auto &cast_info = *reinterpret_cast<sabot_sql::CCastExecuteInfo *>(info);
	return cast_info.parameters.error_message == nullptr ? SABOT_SQL_CAST_NORMAL : SABOT_SQL_CAST_TRY;
}

void sabot_sql_cast_function_set_error(sabot_sql_function_info info, const char *error) {
	auto &cast_info = *reinterpret_cast<sabot_sql::CCastExecuteInfo *>(info);
	cast_info.error_message = error;
}

void sabot_sql_cast_function_set_row_error(sabot_sql_function_info info, const char *error, idx_t row, sabot_sql_vector output) {
	auto &cast_info = *reinterpret_cast<sabot_sql::CCastExecuteInfo *>(info);
	cast_info.error_message = error;
	if (!output) {
		return;
	}
	auto &output_vector = *reinterpret_cast<sabot_sql::Vector *>(output);
	sabot_sql::FlatVector::SetNull(output_vector, row, true);
}

void sabot_sql_cast_function_set_extra_info(sabot_sql_cast_function cast_function, void *extra_info,
                                         sabot_sql_delete_callback_t destroy) {
	if (!cast_function || !extra_info) {
		return;
	}
	auto &cast = *reinterpret_cast<sabot_sql::CCastFunction *>(cast_function);
	cast.extra_info = static_cast<sabot_sql_function_info>(extra_info);
	cast.delete_callback = destroy;
}

void *sabot_sql_cast_function_get_extra_info(sabot_sql_function_info info) {
	if (!info) {
		return nullptr;
	}
	auto &cast_info = *reinterpret_cast<sabot_sql::CCastExecuteInfo *>(info);
	auto &cast_data = cast_info.parameters.cast_data->Cast<sabot_sql::CCastFunctionData>();
	return cast_data.extra_info->data_ptr;
}

sabot_sql_state sabot_sql_register_cast_function(sabot_sql_connection connection, sabot_sql_cast_function cast_function) {
	if (!connection || !cast_function) {
		return SabotSQLError;
	}
	auto &cast = *reinterpret_cast<sabot_sql::CCastFunction *>(cast_function);
	if (!cast.source_type || !cast.target_type || !cast.function) {
		return SabotSQLError;
	}

	const auto &source_type = *cast.source_type;
	const auto &target_type = *cast.target_type;

	if (sabot_sql::TypeVisitor::Contains(source_type, sabot_sql::LogicalTypeId::INVALID) ||
	    sabot_sql::TypeVisitor::Contains(source_type, sabot_sql::LogicalTypeId::ANY)) {
		return SabotSQLError;
	}

	if (sabot_sql::TypeVisitor::Contains(target_type, sabot_sql::LogicalTypeId::INVALID) ||
	    sabot_sql::TypeVisitor::Contains(target_type, sabot_sql::LogicalTypeId::ANY)) {
		return SabotSQLError;
	}

	try {
		const auto con = reinterpret_cast<sabot_sql::Connection *>(connection);
		con->context->RunFunctionInTransaction([&]() {
			auto &config = sabot_sql::DBConfig::GetConfig(*con->context);
			auto &casts = config.GetCastFunctions();

			auto extra_info =
			    sabot_sql::make_shared_ptr<sabot_sql::CCastFunctionUserData>(cast.extra_info, cast.delete_callback);
			auto cast_data = sabot_sql::make_uniq<sabot_sql::CCastFunctionData>(cast.function, std::move(extra_info));
			sabot_sql::BoundCastInfo cast_info(sabot_sql::CAPICastFunction, std::move(cast_data));
			casts.RegisterCastFunction(source_type, target_type, std::move(cast_info), cast.implicit_cast_cost);
		});
	} catch (...) {
		return SabotSQLError;
	}
	return SabotSQLSuccess;
}

void sabot_sql_destroy_cast_function(sabot_sql_cast_function *cast_function) {
	if (!cast_function || !*cast_function) {
		return;
	}
	const auto function = reinterpret_cast<sabot_sql::CCastFunction *>(*cast_function);
	delete function;
	*cast_function = nullptr;
}
