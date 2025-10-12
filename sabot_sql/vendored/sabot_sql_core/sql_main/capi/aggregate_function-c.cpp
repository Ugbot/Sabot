#include "sabot_sql/catalog/catalog.hpp"
#include "sabot_sql/common/type_visitor.hpp"
#include "sabot_sql/common/types.hpp"
#include "sabot_sql/function/function.hpp"
#include "sabot_sql/function/scalar_function.hpp"
#include "sabot_sql/main/capi/capi_internal.hpp"
#include "sabot_sql/main/client_context.hpp"
#include "sabot_sql/parser/parsed_data/create_aggregate_function_info.hpp"
#include "sabot_sql/planner/expression/bound_function_expression.hpp"

namespace sabot_sql {

struct CAggregateFunctionInfo : public AggregateFunctionInfo {
	~CAggregateFunctionInfo() override {
		if (extra_info && delete_callback) {
			delete_callback(extra_info);
		}
		extra_info = nullptr;
		delete_callback = nullptr;
	}

	sabot_sql_aggregate_state_size state_size = nullptr;
	sabot_sql_aggregate_init_t state_init = nullptr;
	sabot_sql_aggregate_update_t update = nullptr;
	sabot_sql_aggregate_combine_t combine = nullptr;
	sabot_sql_aggregate_finalize_t finalize = nullptr;
	sabot_sql_aggregate_destroy_t destroy = nullptr;
	sabot_sql_function_info extra_info = nullptr;
	sabot_sql_delete_callback_t delete_callback = nullptr;
};

struct CAggregateExecuteInfo {
	explicit CAggregateExecuteInfo(CAggregateFunctionInfo &info) : info(info) {
	}

	CAggregateFunctionInfo &info;
	bool success = true;
	string error;
};

struct CAggregateFunctionBindData : public FunctionData {
	explicit CAggregateFunctionBindData(CAggregateFunctionInfo &info) : info(info) {
	}

	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<CAggregateFunctionBindData>(info);
	}
	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<CAggregateFunctionBindData>();
		return info.extra_info == other.info.extra_info && info.update == other.info.update &&
		       info.combine == other.info.combine && info.finalize == other.info.finalize;
	}

	CAggregateFunctionInfo &info;
};

sabot_sql::AggregateFunction &GetCAggregateFunction(sabot_sql_aggregate_function function) {
	return *reinterpret_cast<sabot_sql::AggregateFunction *>(function);
}

sabot_sql::AggregateFunctionSet &GetCAggregateFunctionSet(sabot_sql_aggregate_function_set function_set) {
	return *reinterpret_cast<sabot_sql::AggregateFunctionSet *>(function_set);
}

unique_ptr<FunctionData> CAPIAggregateBind(ClientContext &context, AggregateFunction &function,
                                           vector<unique_ptr<Expression>> &arguments) {
	auto &info = function.function_info->Cast<CAggregateFunctionInfo>();
	return make_uniq<CAggregateFunctionBindData>(info);
}

idx_t CAPIAggregateStateSize(const AggregateFunction &function) {
	auto &function_info = function.function_info->Cast<sabot_sql::CAggregateFunctionInfo>();
	CAggregateExecuteInfo exec_info(function_info);
	auto c_function_info = reinterpret_cast<sabot_sql_function_info>(&exec_info);
	auto result = function_info.state_size(c_function_info);
	if (!exec_info.success) {
		throw InvalidInputException(exec_info.error);
	}
	return result;
}

void CAPIAggregateStateInit(const AggregateFunction &function, data_ptr_t state) {
	auto &function_info = function.function_info->Cast<sabot_sql::CAggregateFunctionInfo>();
	CAggregateExecuteInfo exec_info(function_info);
	auto c_function_info = reinterpret_cast<sabot_sql_function_info>(&exec_info);
	function_info.state_init(c_function_info, reinterpret_cast<sabot_sql_aggregate_state>(state));
	if (!exec_info.success) {
		throw InvalidInputException(exec_info.error);
	}
}

void CAPIAggregateUpdate(Vector inputs[], AggregateInputData &aggr_input_data, idx_t input_count, Vector &state,
                         idx_t count) {
	DataChunk chunk;
	for (idx_t c = 0; c < input_count; c++) {
		inputs[c].Flatten(count);
		chunk.data.emplace_back(inputs[c]);
	}
	chunk.SetCardinality(count);

	auto &bind_data = aggr_input_data.bind_data->Cast<CAggregateFunctionBindData>();
	auto state_data = FlatVector::GetDataUnsafe<sabot_sql_aggregate_state>(state);
	auto c_input_chunk = reinterpret_cast<sabot_sql_data_chunk>(&chunk);

	CAggregateExecuteInfo exec_info(bind_data.info);
	auto c_function_info = reinterpret_cast<sabot_sql_function_info>(&exec_info);
	bind_data.info.update(c_function_info, c_input_chunk, state_data);
	if (!exec_info.success) {
		throw InvalidInputException(exec_info.error);
	}
}

void CAPIAggregateCombine(Vector &state, Vector &combined, AggregateInputData &aggr_input_data, idx_t count) {
	state.Flatten(count);
	auto &bind_data = aggr_input_data.bind_data->Cast<CAggregateFunctionBindData>();
	auto input_state_data = FlatVector::GetDataUnsafe<sabot_sql_aggregate_state>(state);
	auto result_state_data = FlatVector::GetDataUnsafe<sabot_sql_aggregate_state>(combined);
	CAggregateExecuteInfo exec_info(bind_data.info);
	auto c_function_info = reinterpret_cast<sabot_sql_function_info>(&exec_info);
	bind_data.info.combine(c_function_info, input_state_data, result_state_data, count);
	if (!exec_info.success) {
		throw InvalidInputException(exec_info.error);
	}
}

void CAPIAggregateFinalize(Vector &state, AggregateInputData &aggr_input_data, Vector &result, idx_t count,
                           idx_t offset) {
	state.Flatten(count);
	auto &bind_data = aggr_input_data.bind_data->Cast<CAggregateFunctionBindData>();
	auto input_state_data = FlatVector::GetDataUnsafe<sabot_sql_aggregate_state>(state);
	auto result_vector = reinterpret_cast<sabot_sql_vector>(&result);

	CAggregateExecuteInfo exec_info(bind_data.info);
	auto c_function_info = reinterpret_cast<sabot_sql_function_info>(&exec_info);
	bind_data.info.finalize(c_function_info, input_state_data, result_vector, count, offset);
	if (!exec_info.success) {
		throw InvalidInputException(exec_info.error);
	}
}

void CAPIAggregateDestructor(Vector &state, AggregateInputData &aggr_input_data, idx_t count) {
	auto &bind_data = aggr_input_data.bind_data->Cast<CAggregateFunctionBindData>();
	auto input_state_data = FlatVector::GetDataUnsafe<sabot_sql_aggregate_state>(state);
	bind_data.info.destroy(input_state_data, count);
}

} // namespace sabot_sql

using sabot_sql::GetCAggregateFunction;

sabot_sql_aggregate_function sabot_sql_create_aggregate_function() {
	auto function = new sabot_sql::AggregateFunction("", {}, sabot_sql::LogicalType::INVALID, sabot_sql::CAPIAggregateStateSize,
	                                              sabot_sql::CAPIAggregateStateInit, sabot_sql::CAPIAggregateUpdate,
	                                              sabot_sql::CAPIAggregateCombine, sabot_sql::CAPIAggregateFinalize, nullptr,
	                                              sabot_sql::CAPIAggregateBind);
	try {
		function->function_info = sabot_sql::make_shared_ptr<sabot_sql::CAggregateFunctionInfo>();
		return reinterpret_cast<sabot_sql_aggregate_function>(function);
	} catch (...) {
		delete function;
		return nullptr;
	}
}

void sabot_sql_destroy_aggregate_function(sabot_sql_aggregate_function *function) {
	if (function && *function) {
		auto aggregate_function = reinterpret_cast<sabot_sql::AggregateFunction *>(*function);
		delete aggregate_function;
		*function = nullptr;
	}
}

void sabot_sql_aggregate_function_set_name(sabot_sql_aggregate_function function, const char *name) {
	if (!function || !name) {
		return;
	}
	auto &aggregate_function = GetCAggregateFunction(function);
	aggregate_function.name = name;
}

void sabot_sql_aggregate_function_add_parameter(sabot_sql_aggregate_function function, sabot_sql_logical_type type) {
	if (!function || !type) {
		return;
	}
	auto &aggregate_function = GetCAggregateFunction(function);
	auto logical_type = reinterpret_cast<sabot_sql::LogicalType *>(type);
	aggregate_function.arguments.push_back(*logical_type);
}

void sabot_sql_aggregate_function_set_return_type(sabot_sql_aggregate_function function, sabot_sql_logical_type type) {
	if (!function || !type) {
		return;
	}
	auto &aggregate_function = GetCAggregateFunction(function);
	auto logical_type = reinterpret_cast<sabot_sql::LogicalType *>(type);
	aggregate_function.return_type = *logical_type;
}

void sabot_sql_aggregate_function_set_functions(sabot_sql_aggregate_function function, sabot_sql_aggregate_state_size state_size,
                                             sabot_sql_aggregate_init_t state_init, sabot_sql_aggregate_update_t update,
                                             sabot_sql_aggregate_combine_t combine, sabot_sql_aggregate_finalize_t finalize) {
	if (!function || !state_size || !state_init || !update || !combine || !finalize) {
		return;
	}
	auto &aggregate_function = GetCAggregateFunction(function);
	auto &function_info = aggregate_function.function_info->Cast<sabot_sql::CAggregateFunctionInfo>();
	function_info.state_size = state_size;
	function_info.state_init = state_init;
	function_info.update = update;
	function_info.combine = combine;
	function_info.finalize = finalize;
}

void sabot_sql_aggregate_function_set_destructor(sabot_sql_aggregate_function function, sabot_sql_aggregate_destroy_t destroy) {
	if (!function || !destroy) {
		return;
	}
	auto &aggregate_function = GetCAggregateFunction(function);
	auto &function_info = aggregate_function.function_info->Cast<sabot_sql::CAggregateFunctionInfo>();
	function_info.destroy = destroy;
	aggregate_function.destructor = sabot_sql::CAPIAggregateDestructor;
}

sabot_sql_state sabot_sql_register_aggregate_function(sabot_sql_connection connection, sabot_sql_aggregate_function function) {
	if (!connection || !function) {
		return SabotSQLError;
	}

	auto &aggregate_function = GetCAggregateFunction(function);
	sabot_sql::AggregateFunctionSet set(aggregate_function.name);
	set.AddFunction(aggregate_function);
	return sabot_sql_register_aggregate_function_set(connection, reinterpret_cast<sabot_sql_aggregate_function_set>(&set));
}

void sabot_sql_aggregate_function_set_special_handling(sabot_sql_aggregate_function function) {
	if (!function) {
		return;
	}
	auto &aggregate_function = GetCAggregateFunction(function);
	aggregate_function.null_handling = sabot_sql::FunctionNullHandling::SPECIAL_HANDLING;
}

void sabot_sql_aggregate_function_set_extra_info(sabot_sql_aggregate_function function, void *extra_info,
                                              sabot_sql_delete_callback_t destroy) {
	if (!function || !extra_info) {
		return;
	}
	auto &aggregate_function = GetCAggregateFunction(function);
	auto &function_info = aggregate_function.function_info->Cast<sabot_sql::CAggregateFunctionInfo>();
	function_info.extra_info = static_cast<sabot_sql_function_info>(extra_info);
	function_info.delete_callback = destroy;
}

sabot_sql::CAggregateExecuteInfo &GetCAggregateExecuteInfo(sabot_sql_function_info info) {
	D_ASSERT(info);
	return *reinterpret_cast<sabot_sql::CAggregateExecuteInfo *>(info);
}

void *sabot_sql_aggregate_function_get_extra_info(sabot_sql_function_info info_p) {
	auto &exec_info = GetCAggregateExecuteInfo(info_p);
	return exec_info.info.extra_info;
}

void sabot_sql_aggregate_function_set_error(sabot_sql_function_info info_p, const char *error) {
	auto &exec_info = GetCAggregateExecuteInfo(info_p);
	exec_info.error = error;
	exec_info.success = false;
}

sabot_sql_aggregate_function_set sabot_sql_create_aggregate_function_set(const char *name) {
	if (!name || !*name) {
		return nullptr;
	}
	try {
		auto function_set = new sabot_sql::AggregateFunctionSet(name);
		return reinterpret_cast<sabot_sql_aggregate_function_set>(function_set);
	} catch (...) {
		return nullptr;
	}
}

void sabot_sql_destroy_aggregate_function_set(sabot_sql_aggregate_function_set *set) {
	if (set && *set) {
		auto aggregate_function_set = reinterpret_cast<sabot_sql::AggregateFunctionSet *>(*set);
		delete aggregate_function_set;
		*set = nullptr;
	}
}

sabot_sql_state sabot_sql_add_aggregate_function_to_set(sabot_sql_aggregate_function_set set,
                                                  sabot_sql_aggregate_function function) {
	if (!set || !function) {
		return SabotSQLError;
	}
	auto &aggregate_function_set = sabot_sql::GetCAggregateFunctionSet(set);
	auto &aggregate_function = GetCAggregateFunction(function);
	aggregate_function_set.AddFunction(aggregate_function);
	return SabotSQLSuccess;
}

sabot_sql_state sabot_sql_register_aggregate_function_set(sabot_sql_connection connection,
                                                    sabot_sql_aggregate_function_set function_set) {
	if (!connection || !function_set) {
		return SabotSQLError;
	}
	auto &set = sabot_sql::GetCAggregateFunctionSet(function_set);
	for (idx_t idx = 0; idx < set.Size(); idx++) {
		auto &aggregate_function = set.GetFunctionReferenceByOffset(idx);
		auto &info = aggregate_function.function_info->Cast<sabot_sql::CAggregateFunctionInfo>();

		if (aggregate_function.name.empty() || !info.update || !info.combine || !info.finalize) {
			return SabotSQLError;
		}
		if (sabot_sql::TypeVisitor::Contains(aggregate_function.return_type, sabot_sql::LogicalTypeId::INVALID) ||
		    sabot_sql::TypeVisitor::Contains(aggregate_function.return_type, sabot_sql::LogicalTypeId::ANY)) {
			return SabotSQLError;
		}
		for (const auto &argument : aggregate_function.arguments) {
			if (sabot_sql::TypeVisitor::Contains(argument, sabot_sql::LogicalTypeId::INVALID)) {
				return SabotSQLError;
			}
		}
	}

	try {
		auto con = reinterpret_cast<sabot_sql::Connection *>(connection);
		con->context->RunFunctionInTransaction([&]() {
			auto &catalog = sabot_sql::Catalog::GetSystemCatalog(*con->context);
			sabot_sql::CreateAggregateFunctionInfo sf_info(set);
			sf_info.on_conflict = sabot_sql::OnCreateConflict::ALTER_ON_CONFLICT;
			catalog.CreateFunction(*con->context, sf_info);
		});
	} catch (...) {
		return SabotSQLError;
	}
	return SabotSQLSuccess;
}
