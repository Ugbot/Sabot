#include "sabot_sql/catalog/catalog.hpp"
#include "sabot_sql/common/type_visitor.hpp"
#include "sabot_sql/common/types.hpp"
#include "sabot_sql/function/function.hpp"
#include "sabot_sql/function/scalar_function.hpp"
#include "sabot_sql/main/capi/capi_internal.hpp"
#include "sabot_sql/main/client_context.hpp"
#include "sabot_sql/parser/parsed_data/create_scalar_function_info.hpp"
#include "sabot_sql/planner/expression/bound_function_expression.hpp"

namespace sabot_sql {

struct CScalarFunctionInfo : public ScalarFunctionInfo {
	~CScalarFunctionInfo() override {
		if (extra_info && delete_callback) {
			delete_callback(extra_info);
		}
		extra_info = nullptr;
		delete_callback = nullptr;
	}

	sabot_sql_scalar_function_bind_t bind = nullptr;
	sabot_sql_scalar_function_t function = nullptr;
	sabot_sql_function_info extra_info = nullptr;
	sabot_sql_delete_callback_t delete_callback = nullptr;
};

struct CScalarFunctionBindData : public FunctionData {
	explicit CScalarFunctionBindData(CScalarFunctionInfo &info) : info(info) {
	}
	~CScalarFunctionBindData() override {
		if (bind_data && delete_callback) {
			delete_callback(bind_data);
		}
		bind_data = nullptr;
		delete_callback = nullptr;
	}

	unique_ptr<FunctionData> Copy() const override {
		auto copy = make_uniq<CScalarFunctionBindData>(info);
		if (copy_callback) {
			copy->bind_data = copy_callback(bind_data);
			copy->delete_callback = delete_callback;
			copy->copy_callback = copy_callback;
		}
		return std::move(copy);
	}
	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<CScalarFunctionBindData>();
		return info.extra_info == other.info.extra_info && info.function == other.info.function;
	}

	CScalarFunctionInfo &info;
	void *bind_data = nullptr;
	sabot_sql_delete_callback_t delete_callback = nullptr;
	sabot_sql_copy_callback_t copy_callback = nullptr;
};

struct CScalarFunctionInternalBindInfo {
	CScalarFunctionInternalBindInfo(ClientContext &context, ScalarFunction &bound_function,
	                                vector<unique_ptr<Expression>> &arguments, CScalarFunctionBindData &bind_data)
	    : context(context), bound_function(bound_function), arguments(arguments), bind_data(bind_data) {
	}

	ClientContext &context;
	ScalarFunction &bound_function;
	vector<unique_ptr<Expression>> &arguments;
	CScalarFunctionBindData &bind_data;

	bool success = true;
	string error = "";
};

struct CScalarFunctionInternalFunctionInfo {
	explicit CScalarFunctionInternalFunctionInfo(const CScalarFunctionBindData &bind_data)
	    : bind_data(bind_data), success(true) {};

	const CScalarFunctionBindData &bind_data;

	bool success;
	string error = "";
};

//===--------------------------------------------------------------------===//
// Helper Functions
//===--------------------------------------------------------------------===//

sabot_sql::ScalarFunction &GetCScalarFunction(sabot_sql_scalar_function function) {
	return *reinterpret_cast<sabot_sql::ScalarFunction *>(function);
}

sabot_sql::ScalarFunctionSet &GetCScalarFunctionSet(sabot_sql_scalar_function_set set) {
	return *reinterpret_cast<sabot_sql::ScalarFunctionSet *>(set);
}

sabot_sql::CScalarFunctionInternalBindInfo &GetCScalarFunctionBindInfo(sabot_sql_bind_info info) {
	D_ASSERT(info);
	return *reinterpret_cast<sabot_sql::CScalarFunctionInternalBindInfo *>(info);
}

sabot_sql_bind_info ToCScalarFunctionBindInfo(sabot_sql::CScalarFunctionInternalBindInfo &info) {
	return reinterpret_cast<sabot_sql_bind_info>(&info);
}

sabot_sql::CScalarFunctionInternalFunctionInfo &GetCScalarFunctionInfo(sabot_sql_function_info info) {
	D_ASSERT(info);
	return *reinterpret_cast<sabot_sql::CScalarFunctionInternalFunctionInfo *>(info);
}

sabot_sql_function_info ToCScalarFunctionInfo(sabot_sql::CScalarFunctionInternalFunctionInfo &info) {
	return reinterpret_cast<sabot_sql_function_info>(&info);
}

//===--------------------------------------------------------------------===//
// Scalar Function Callbacks
//===--------------------------------------------------------------------===//

unique_ptr<FunctionData> CScalarFunctionBind(ClientContext &context, ScalarFunction &bound_function,
                                             vector<unique_ptr<Expression>> &arguments) {
	auto &info = bound_function.function_info->Cast<CScalarFunctionInfo>();
	D_ASSERT(info.function);

	auto result = make_uniq<CScalarFunctionBindData>(info);
	if (info.bind) {
		CScalarFunctionInternalBindInfo bind_info(context, bound_function, arguments, *result);
		info.bind(ToCScalarFunctionBindInfo(bind_info));
		if (!bind_info.success) {
			throw BinderException(bind_info.error);
		}
	}

	return std::move(result);
}

void CAPIScalarFunction(DataChunk &input, ExpressionState &state, Vector &result) {
	auto &function = state.expr.Cast<BoundFunctionExpression>();
	auto &bind_info = function.bind_info;
	auto &c_bind_info = bind_info->Cast<CScalarFunctionBindData>();

	auto all_const = input.AllConstant();
	input.Flatten();
	auto c_input = reinterpret_cast<sabot_sql_data_chunk>(&input);
	auto c_result = reinterpret_cast<sabot_sql_vector>(&result);

	CScalarFunctionInternalFunctionInfo function_info(c_bind_info);
	auto c_function_info = ToCScalarFunctionInfo(function_info);
	c_bind_info.info.function(c_function_info, c_input, c_result);
	if (!function_info.success) {
		throw InvalidInputException(function_info.error);
	}
	if (all_const && (input.size() == 1 || function.function.stability != FunctionStability::VOLATILE)) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

} // namespace sabot_sql

using sabot_sql::ExpressionWrapper;
using sabot_sql::GetCScalarFunction;
using sabot_sql::GetCScalarFunctionBindInfo;
using sabot_sql::GetCScalarFunctionInfo;
using sabot_sql::GetCScalarFunctionSet;

sabot_sql_scalar_function sabot_sql_create_scalar_function() {
	auto function = new sabot_sql::ScalarFunction("", {}, sabot_sql::LogicalType::INVALID, sabot_sql::CAPIScalarFunction,
	                                           sabot_sql::CScalarFunctionBind);
	function->function_info = sabot_sql::make_shared_ptr<sabot_sql::CScalarFunctionInfo>();
	return reinterpret_cast<sabot_sql_scalar_function>(function);
}

void sabot_sql_destroy_scalar_function(sabot_sql_scalar_function *function) {
	if (function && *function) {
		auto scalar_function = reinterpret_cast<sabot_sql::ScalarFunction *>(*function);
		delete scalar_function;
		*function = nullptr;
	}
}

void sabot_sql_scalar_function_set_name(sabot_sql_scalar_function function, const char *name) {
	if (!function || !name) {
		return;
	}
	auto &scalar_function = GetCScalarFunction(function);
	scalar_function.name = name;
}

void sabot_sql_scalar_function_set_varargs(sabot_sql_scalar_function function, sabot_sql_logical_type type) {
	if (!function || !type) {
		return;
	}
	auto &scalar_function = GetCScalarFunction(function);
	auto logical_type = reinterpret_cast<sabot_sql::LogicalType *>(type);
	scalar_function.varargs = *logical_type;
}

void sabot_sql_scalar_function_set_special_handling(sabot_sql_scalar_function function) {
	if (!function) {
		return;
	}
	auto &scalar_function = GetCScalarFunction(function);
	scalar_function.null_handling = sabot_sql::FunctionNullHandling::SPECIAL_HANDLING;
}

void sabot_sql_scalar_function_set_volatile(sabot_sql_scalar_function function) {
	if (!function) {
		return;
	}
	auto &scalar_function = GetCScalarFunction(function);
	scalar_function.stability = sabot_sql::FunctionStability::VOLATILE;
}

void sabot_sql_scalar_function_add_parameter(sabot_sql_scalar_function function, sabot_sql_logical_type type) {
	if (!function || !type) {
		return;
	}
	auto &scalar_function = GetCScalarFunction(function);
	auto logical_type = reinterpret_cast<sabot_sql::LogicalType *>(type);
	scalar_function.arguments.push_back(*logical_type);
}

void sabot_sql_scalar_function_set_return_type(sabot_sql_scalar_function function, sabot_sql_logical_type type) {
	if (!function || !type) {
		return;
	}
	auto &scalar_function = GetCScalarFunction(function);
	auto logical_type = reinterpret_cast<sabot_sql::LogicalType *>(type);
	scalar_function.return_type = *logical_type;
}

void *sabot_sql_scalar_function_get_extra_info(sabot_sql_function_info info) {
	if (!info) {
		return nullptr;
	}
	auto &function_info = GetCScalarFunctionInfo(info);
	return function_info.bind_data.info.extra_info;
}

void *sabot_sql_scalar_function_bind_get_extra_info(sabot_sql_bind_info info) {
	if (!info) {
		return nullptr;
	}
	auto &bind_info = GetCScalarFunctionBindInfo(info);
	return bind_info.bind_data.info.extra_info;
}

void *sabot_sql_scalar_function_get_bind_data(sabot_sql_function_info info) {
	if (!info) {
		return nullptr;
	}
	auto &function_info = GetCScalarFunctionInfo(info);
	return function_info.bind_data.bind_data;
}

void sabot_sql_scalar_function_get_client_context(sabot_sql_bind_info info, sabot_sql_client_context *out_context) {
	if (!info || !out_context) {
		return;
	}
	auto &bind_info = GetCScalarFunctionBindInfo(info);
	auto wrapper = new sabot_sql::CClientContextWrapper(bind_info.context);
	*out_context = reinterpret_cast<sabot_sql_client_context>(wrapper);
}

void sabot_sql_scalar_function_set_error(sabot_sql_function_info info, const char *error) {
	if (!info || !error) {
		return;
	}
	auto &scalar_function = sabot_sql::GetCScalarFunctionInfo(info);
	scalar_function.error = error;
	scalar_function.success = false;
}

void sabot_sql_scalar_function_bind_set_error(sabot_sql_bind_info info, const char *error) {
	if (!info || !error) {
		return;
	}
	auto &bind_info = GetCScalarFunctionBindInfo(info);
	bind_info.error = error;
	bind_info.success = false;
}

idx_t sabot_sql_scalar_function_bind_get_argument_count(sabot_sql_bind_info info) {
	if (!info) {
		return 0;
	}
	auto &bind_info = GetCScalarFunctionBindInfo(info);
	return bind_info.arguments.size();
}

sabot_sql_expression sabot_sql_scalar_function_bind_get_argument(sabot_sql_bind_info info, idx_t index) {
	if (!info || index >= sabot_sql_scalar_function_bind_get_argument_count(info)) {
		return nullptr;
	}
	auto &bind_info = GetCScalarFunctionBindInfo(info);
	auto wrapper = new ExpressionWrapper();
	wrapper->expr = bind_info.arguments[index]->Copy();
	return reinterpret_cast<sabot_sql_expression>(wrapper);
}

void sabot_sql_scalar_function_set_extra_info(sabot_sql_scalar_function function, void *extra_info,
                                           sabot_sql_delete_callback_t destroy) {
	if (!function || !extra_info) {
		return;
	}
	auto &scalar_function = GetCScalarFunction(function);
	auto &info = scalar_function.function_info->Cast<sabot_sql::CScalarFunctionInfo>();
	info.extra_info = reinterpret_cast<sabot_sql_function_info>(extra_info);
	info.delete_callback = destroy;
}

void sabot_sql_scalar_function_set_bind(sabot_sql_scalar_function scalar_function, sabot_sql_scalar_function_bind_t bind) {
	if (!scalar_function || !bind) {
		return;
	}
	auto &sf = GetCScalarFunction(scalar_function);
	auto &info = sf.function_info->Cast<sabot_sql::CScalarFunctionInfo>();
	info.bind = bind;
}

void sabot_sql_scalar_function_set_bind_data(sabot_sql_bind_info info, void *bind_data, sabot_sql_delete_callback_t destroy) {
	if (!info) {
		return;
	}
	auto &bind_info = GetCScalarFunctionBindInfo(info);
	bind_info.bind_data.bind_data = bind_data;
	bind_info.bind_data.delete_callback = destroy;
}

void sabot_sql_scalar_function_set_bind_data_copy(sabot_sql_bind_info info, sabot_sql_copy_callback_t copy) {
	if (!info) {
		return;
	}
	auto &bind_info = GetCScalarFunctionBindInfo(info);
	bind_info.bind_data.copy_callback = copy;
}

void sabot_sql_scalar_function_set_function(sabot_sql_scalar_function function, sabot_sql_scalar_function_t execute_func) {
	if (!function || !execute_func) {
		return;
	}
	auto &scalar_function = GetCScalarFunction(function);
	auto &info = scalar_function.function_info->Cast<sabot_sql::CScalarFunctionInfo>();
	info.function = execute_func;
}

sabot_sql_state sabot_sql_register_scalar_function(sabot_sql_connection connection, sabot_sql_scalar_function function) {
	if (!connection || !function) {
		return SabotSQLError;
	}
	auto &scalar_function = GetCScalarFunction(function);
	sabot_sql::ScalarFunctionSet set(scalar_function.name);
	set.AddFunction(scalar_function);
	return sabot_sql_register_scalar_function_set(connection, reinterpret_cast<sabot_sql_scalar_function_set>(&set));
}

sabot_sql_scalar_function_set sabot_sql_create_scalar_function_set(const char *name) {
	if (!name || !*name) {
		return nullptr;
	}
	auto function = new sabot_sql::ScalarFunctionSet(name);
	return reinterpret_cast<sabot_sql_scalar_function_set>(function);
}

void sabot_sql_destroy_scalar_function_set(sabot_sql_scalar_function_set *set) {
	if (set && *set) {
		auto scalar_function_set = reinterpret_cast<sabot_sql::ScalarFunctionSet *>(*set);
		delete scalar_function_set;
		*set = nullptr;
	}
}

sabot_sql_state sabot_sql_add_scalar_function_to_set(sabot_sql_scalar_function_set set, sabot_sql_scalar_function function) {
	if (!set || !function) {
		return SabotSQLError;
	}
	auto &scalar_function_set = GetCScalarFunctionSet(set);
	auto &scalar_function = GetCScalarFunction(function);
	scalar_function_set.AddFunction(scalar_function);
	return SabotSQLSuccess;
}

sabot_sql_state sabot_sql_register_scalar_function_set(sabot_sql_connection connection, sabot_sql_scalar_function_set set) {
	if (!connection || !set) {
		return SabotSQLError;
	}
	auto &scalar_function_set = GetCScalarFunctionSet(set);
	for (idx_t idx = 0; idx < scalar_function_set.Size(); idx++) {
		auto &scalar_function = scalar_function_set.GetFunctionReferenceByOffset(idx);
		auto &info = scalar_function.function_info->Cast<sabot_sql::CScalarFunctionInfo>();

		if (scalar_function.name.empty() || !info.function) {
			return SabotSQLError;
		}
		if (sabot_sql::TypeVisitor::Contains(scalar_function.return_type, sabot_sql::LogicalTypeId::INVALID) ||
		    sabot_sql::TypeVisitor::Contains(scalar_function.return_type, sabot_sql::LogicalTypeId::ANY)) {
			return SabotSQLError;
		}
		for (const auto &argument : scalar_function.arguments) {
			if (sabot_sql::TypeVisitor::Contains(argument, sabot_sql::LogicalTypeId::INVALID)) {
				return SabotSQLError;
			}
		}
	}

	try {
		auto con = reinterpret_cast<sabot_sql::Connection *>(connection);
		con->context->RunFunctionInTransaction([&]() {
			auto &catalog = sabot_sql::Catalog::GetSystemCatalog(*con->context);
			sabot_sql::CreateScalarFunctionInfo sf_info(scalar_function_set);
			sf_info.on_conflict = sabot_sql::OnCreateConflict::ALTER_ON_CONFLICT;
			catalog.CreateFunction(*con->context, sf_info);
		});
	} catch (...) {
		return SabotSQLError;
	}
	return SabotSQLSuccess;
}
