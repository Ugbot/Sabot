#include "sabot_sql/catalog/catalog.hpp"
#include "sabot_sql/common/type_visitor.hpp"
#include "sabot_sql/common/types.hpp"
#include "sabot_sql/function/table_function.hpp"
#include "sabot_sql/main/capi/capi_internal.hpp"
#include "sabot_sql/main/client_context.hpp"
#include "sabot_sql/parser/parsed_data/create_table_function_info.hpp"
#include "sabot_sql/storage/statistics/node_statistics.hpp"

namespace sabot_sql {

//===--------------------------------------------------------------------===//
// Structures
//===--------------------------------------------------------------------===//
struct CTableFunctionInfo : public TableFunctionInfo {
	~CTableFunctionInfo() override {
		if (extra_info && delete_callback) {
			delete_callback(extra_info);
		}
		extra_info = nullptr;
		delete_callback = nullptr;
	}

	sabot_sql_table_function_bind_t bind = nullptr;
	sabot_sql_table_function_init_t init = nullptr;
	sabot_sql_table_function_init_t local_init = nullptr;
	sabot_sql_table_function_t function = nullptr;
	void *extra_info = nullptr;
	sabot_sql_delete_callback_t delete_callback = nullptr;
};

struct CTableBindData : public TableFunctionData {
	explicit CTableBindData(CTableFunctionInfo &info) : info(info) {
	}
	~CTableBindData() override {
		if (bind_data && delete_callback) {
			delete_callback(bind_data);
		}
		bind_data = nullptr;
		delete_callback = nullptr;
	}

	CTableFunctionInfo &info;
	void *bind_data = nullptr;
	sabot_sql_delete_callback_t delete_callback = nullptr;
	unique_ptr<NodeStatistics> stats;
};

struct CTableInternalBindInfo {
	CTableInternalBindInfo(ClientContext &context, TableFunctionBindInput &input, vector<LogicalType> &return_types,
	                       vector<string> &names, CTableBindData &bind_data, CTableFunctionInfo &function_info)
	    : context(context), input(input), return_types(return_types), names(names), bind_data(bind_data),
	      function_info(function_info), success(true) {
	}

	ClientContext &context;
	TableFunctionBindInput &input;
	vector<LogicalType> &return_types;
	vector<string> &names;
	CTableBindData &bind_data;
	CTableFunctionInfo &function_info;
	bool success;
	string error;
};

struct CTableInitData {
	~CTableInitData() {
		if (init_data && delete_callback) {
			delete_callback(init_data);
		}
		init_data = nullptr;
		delete_callback = nullptr;
	}

	void *init_data = nullptr;
	sabot_sql_delete_callback_t delete_callback = nullptr;
	idx_t max_threads = 1;
};

struct CTableGlobalInitData : public GlobalTableFunctionState {
	CTableInitData init_data;

	idx_t MaxThreads() const override {
		return init_data.max_threads;
	}
};

struct CTableLocalInitData : public LocalTableFunctionState {
	CTableInitData init_data;
};

struct CTableInternalInitInfo {
	CTableInternalInitInfo(const CTableBindData &bind_data, CTableInitData &init_data,
	                       const vector<column_t> &column_ids, optional_ptr<TableFilterSet> filters)
	    : bind_data(bind_data), init_data(init_data), column_ids(column_ids), filters(filters), success(true) {
	}

	const CTableBindData &bind_data;
	CTableInitData &init_data;
	const vector<column_t> &column_ids;
	optional_ptr<TableFilterSet> filters;
	bool success;
	string error;
};

struct CTableInternalFunctionInfo {
	CTableInternalFunctionInfo(const CTableBindData &bind_data, CTableInitData &init_data, CTableInitData &local_data)
	    : bind_data(bind_data), init_data(init_data), local_data(local_data), success(true) {
	}

	const CTableBindData &bind_data;
	CTableInitData &init_data;
	CTableInitData &local_data;
	bool success;
	string error;
};

//===--------------------------------------------------------------------===//
// Helper Functions
//===--------------------------------------------------------------------===//

sabot_sql::TableFunction &GetCTableFunction(sabot_sql_table_function function) {
	return *reinterpret_cast<sabot_sql::TableFunction *>(function);
}

sabot_sql::CTableInternalBindInfo &GetCTableFunctionBindInfo(sabot_sql_bind_info info) {
	D_ASSERT(info);
	return *reinterpret_cast<sabot_sql::CTableInternalBindInfo *>(info);
}

sabot_sql_bind_info ToCTableFunctionBindInfo(sabot_sql::CTableInternalBindInfo &info) {
	return reinterpret_cast<sabot_sql_bind_info>(&info);
}

sabot_sql::CTableInternalInitInfo &GetCInitInfo(sabot_sql_init_info info) {
	D_ASSERT(info);
	return *reinterpret_cast<sabot_sql::CTableInternalInitInfo *>(info);
}

sabot_sql_init_info ToCInitInfo(sabot_sql::CTableInternalInitInfo &info) {
	return reinterpret_cast<sabot_sql_init_info>(&info);
}

sabot_sql::CTableInternalFunctionInfo &GetCTableFunctionInfo(sabot_sql_function_info info) {
	D_ASSERT(info);
	return *reinterpret_cast<sabot_sql::CTableInternalFunctionInfo *>(info);
}

sabot_sql_function_info ToCTableFunctionInfo(sabot_sql::CTableInternalFunctionInfo &info) {
	return reinterpret_cast<sabot_sql_function_info>(&info);
}

//===--------------------------------------------------------------------===//
// Table Function Callbacks
//===--------------------------------------------------------------------===//

unique_ptr<FunctionData> CTableFunctionBind(ClientContext &context, TableFunctionBindInput &input,
                                            vector<LogicalType> &return_types, vector<string> &names) {
	auto &info = input.info->Cast<CTableFunctionInfo>();
	D_ASSERT(info.bind && info.function && info.init);

	auto result = make_uniq<CTableBindData>(info);
	CTableInternalBindInfo bind_info(context, input, return_types, names, *result, info);
	info.bind(ToCTableFunctionBindInfo(bind_info));
	if (!bind_info.success) {
		throw BinderException(bind_info.error);
	}

	return std::move(result);
}

unique_ptr<GlobalTableFunctionState> CTableFunctionInit(ClientContext &context, TableFunctionInitInput &data_p) {
	auto &bind_data = data_p.bind_data->Cast<CTableBindData>();
	auto result = make_uniq<CTableGlobalInitData>();

	CTableInternalInitInfo init_info(bind_data, result->init_data, data_p.column_ids, data_p.filters);
	bind_data.info.init(ToCInitInfo(init_info));
	if (!init_info.success) {
		throw InvalidInputException(init_info.error);
	}
	return std::move(result);
}

unique_ptr<LocalTableFunctionState> CTableFunctionLocalInit(ExecutionContext &context, TableFunctionInitInput &data_p,
                                                            GlobalTableFunctionState *gstate) {
	auto &bind_data = data_p.bind_data->Cast<CTableBindData>();
	auto result = make_uniq<CTableLocalInitData>();
	if (!bind_data.info.local_init) {
		return std::move(result);
	}

	CTableInternalInitInfo init_info(bind_data, result->init_data, data_p.column_ids, data_p.filters);
	bind_data.info.local_init(ToCInitInfo(init_info));
	if (!init_info.success) {
		throw InvalidInputException(init_info.error);
	}
	return std::move(result);
}

unique_ptr<NodeStatistics> CTableFunctionCardinality(ClientContext &context, const FunctionData *bind_data_p) {
	auto &bind_data = bind_data_p->Cast<CTableBindData>();
	if (!bind_data.stats) {
		return nullptr;
	}
	return make_uniq<NodeStatistics>(*bind_data.stats);
}

void CTableFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &bind_data = data_p.bind_data->Cast<CTableBindData>();
	auto &global_data = data_p.global_state->Cast<CTableGlobalInitData>();
	auto &local_data = data_p.local_state->Cast<CTableLocalInitData>();
	CTableInternalFunctionInfo function_info(bind_data, global_data.init_data, local_data.init_data);
	bind_data.info.function(ToCTableFunctionInfo(function_info), reinterpret_cast<sabot_sql_data_chunk>(&output));
	if (!function_info.success) {
		throw InvalidInputException(function_info.error);
	}
}

} // namespace sabot_sql

//===--------------------------------------------------------------------===//
// Table Function
//===--------------------------------------------------------------------===//
using sabot_sql::GetCTableFunction;

sabot_sql_table_function sabot_sql_create_table_function() {
	auto function = new sabot_sql::TableFunction("", {}, sabot_sql::CTableFunction, sabot_sql::CTableFunctionBind,
	                                          sabot_sql::CTableFunctionInit, sabot_sql::CTableFunctionLocalInit);
	function->function_info = sabot_sql::make_shared_ptr<sabot_sql::CTableFunctionInfo>();
	function->cardinality = sabot_sql::CTableFunctionCardinality;
	return reinterpret_cast<sabot_sql_table_function>(function);
}

void sabot_sql_destroy_table_function(sabot_sql_table_function *function) {
	if (function && *function) {
		auto tf = reinterpret_cast<sabot_sql::TableFunction *>(*function);
		delete tf;
		*function = nullptr;
	}
}

void sabot_sql_table_function_set_name(sabot_sql_table_function function, const char *name) {
	if (!function || !name) {
		return;
	}
	auto &tf = GetCTableFunction(function);
	tf.name = name;
}

void sabot_sql_table_function_add_parameter(sabot_sql_table_function function, sabot_sql_logical_type type) {
	if (!function || !type) {
		return;
	}
	auto &tf = GetCTableFunction(function);
	auto logical_type = reinterpret_cast<sabot_sql::LogicalType *>(type);
	tf.arguments.push_back(*logical_type);
}

void sabot_sql_table_function_add_named_parameter(sabot_sql_table_function function, const char *name,
                                               sabot_sql_logical_type type) {
	if (!function || !type) {
		return;
	}
	auto &tf = GetCTableFunction(function);
	auto logical_type = reinterpret_cast<sabot_sql::LogicalType *>(type);
	tf.named_parameters.insert({name, *logical_type});
}

void sabot_sql_table_function_set_extra_info(sabot_sql_table_function function, void *extra_info,
                                          sabot_sql_delete_callback_t destroy) {
	if (!function) {
		return;
	}
	auto &tf = GetCTableFunction(function);
	auto &info = tf.function_info->Cast<sabot_sql::CTableFunctionInfo>();
	info.extra_info = extra_info;
	info.delete_callback = destroy;
}

void sabot_sql_table_function_set_bind(sabot_sql_table_function function, sabot_sql_table_function_bind_t bind) {
	if (!function || !bind) {
		return;
	}
	auto &tf = GetCTableFunction(function);
	auto &info = tf.function_info->Cast<sabot_sql::CTableFunctionInfo>();
	info.bind = bind;
}

void sabot_sql_table_function_set_init(sabot_sql_table_function function, sabot_sql_table_function_init_t init) {
	if (!function || !init) {
		return;
	}
	auto &tf = GetCTableFunction(function);
	auto &info = tf.function_info->Cast<sabot_sql::CTableFunctionInfo>();
	info.init = init;
}

void sabot_sql_table_function_set_local_init(sabot_sql_table_function function, sabot_sql_table_function_init_t init) {
	if (!function || !init) {
		return;
	}
	auto &tf = GetCTableFunction(function);
	auto &info = tf.function_info->Cast<sabot_sql::CTableFunctionInfo>();
	info.local_init = init;
}

void sabot_sql_table_function_set_function(sabot_sql_table_function table_function, sabot_sql_table_function_t function) {
	if (!table_function || !function) {
		return;
	}
	auto &tf = GetCTableFunction(table_function);
	auto &info = tf.function_info->Cast<sabot_sql::CTableFunctionInfo>();
	info.function = function;
}

void sabot_sql_table_function_supports_projection_pushdown(sabot_sql_table_function table_function, bool pushdown) {
	if (!table_function) {
		return;
	}
	auto &tf = GetCTableFunction(table_function);
	tf.projection_pushdown = pushdown;
}

sabot_sql_state sabot_sql_register_table_function(sabot_sql_connection connection, sabot_sql_table_function function) {
	if (!connection || !function) {
		return SabotSQLError;
	}
	auto con = reinterpret_cast<sabot_sql::Connection *>(connection);
	auto &tf = GetCTableFunction(function);
	auto &info = tf.function_info->Cast<sabot_sql::CTableFunctionInfo>();

	if (tf.name.empty() || !info.bind || !info.init || !info.function) {
		return SabotSQLError;
	}
	for (auto it = tf.named_parameters.begin(); it != tf.named_parameters.end(); it++) {
		if (sabot_sql::TypeVisitor::Contains(it->second, sabot_sql::LogicalTypeId::INVALID)) {
			return SabotSQLError;
		}
	}
	for (const auto &argument : tf.arguments) {
		if (sabot_sql::TypeVisitor::Contains(argument, sabot_sql::LogicalTypeId::INVALID)) {
			return SabotSQLError;
		}
	}

	try {
		con->context->RunFunctionInTransaction([&]() {
			auto &catalog = sabot_sql::Catalog::GetSystemCatalog(*con->context);
			sabot_sql::CreateTableFunctionInfo tf_info(tf);
			tf_info.on_conflict = sabot_sql::OnCreateConflict::ALTER_ON_CONFLICT;
			catalog.CreateTableFunction(*con->context, tf_info);
		});
	} catch (...) { // LCOV_EXCL_START
		return SabotSQLError;
	} // LCOV_EXCL_STOP
	return SabotSQLSuccess;
}

//===--------------------------------------------------------------------===//
// Bind Interface
//===--------------------------------------------------------------------===//
using sabot_sql::GetCTableFunctionBindInfo;

void *sabot_sql_bind_get_extra_info(sabot_sql_bind_info info) {
	if (!info) {
		return nullptr;
	}
	auto &bind_info = GetCTableFunctionBindInfo(info);
	return bind_info.function_info.extra_info;
}

void sabot_sql_table_function_get_client_context(sabot_sql_bind_info info, sabot_sql_client_context *out_context) {
	if (!info || !out_context) {
		return;
	}
	auto &bind_info = GetCTableFunctionBindInfo(info);
	auto wrapper = new sabot_sql::CClientContextWrapper(bind_info.context);
	*out_context = reinterpret_cast<sabot_sql_client_context>(wrapper);
}

void sabot_sql_bind_add_result_column(sabot_sql_bind_info info, const char *name, sabot_sql_logical_type type) {
	if (!info || !name || !type) {
		return;
	}
	auto logical_type = reinterpret_cast<sabot_sql::LogicalType *>(type);
	if (sabot_sql::TypeVisitor::Contains(*logical_type, sabot_sql::LogicalTypeId::INVALID) ||
	    sabot_sql::TypeVisitor::Contains(*logical_type, sabot_sql::LogicalTypeId::ANY)) {
		return;
	}

	auto &bind_info = GetCTableFunctionBindInfo(info);
	bind_info.names.push_back(name);
	bind_info.return_types.push_back(*logical_type);
}

idx_t sabot_sql_bind_get_parameter_count(sabot_sql_bind_info info) {
	if (!info) {
		return 0;
	}
	auto &bind_info = GetCTableFunctionBindInfo(info);
	return bind_info.input.inputs.size();
}

sabot_sql_value sabot_sql_bind_get_parameter(sabot_sql_bind_info info, idx_t index) {
	if (!info || index >= sabot_sql_bind_get_parameter_count(info)) {
		return nullptr;
	}
	auto &bind_info = GetCTableFunctionBindInfo(info);
	return reinterpret_cast<sabot_sql_value>(new sabot_sql::Value(bind_info.input.inputs[index]));
}

sabot_sql_value sabot_sql_bind_get_named_parameter(sabot_sql_bind_info info, const char *name) {
	if (!info || !name) {
		return nullptr;
	}
	auto &bind_info = GetCTableFunctionBindInfo(info);
	auto t = bind_info.input.named_parameters.find(name);
	if (t == bind_info.input.named_parameters.end()) {
		return nullptr;
	} else {
		return reinterpret_cast<sabot_sql_value>(new sabot_sql::Value(t->second));
	}
}

void sabot_sql_bind_set_bind_data(sabot_sql_bind_info info, void *bind_data, sabot_sql_delete_callback_t destroy) {
	if (!info) {
		return;
	}
	auto &bind_info = GetCTableFunctionBindInfo(info);
	bind_info.bind_data.bind_data = bind_data;
	bind_info.bind_data.delete_callback = destroy;
}

void sabot_sql_bind_set_cardinality(sabot_sql_bind_info info, idx_t cardinality, bool is_exact) {
	if (!info) {
		return;
	}
	auto &bind_info = GetCTableFunctionBindInfo(info);
	if (is_exact) {
		bind_info.bind_data.stats = sabot_sql::make_uniq<sabot_sql::NodeStatistics>(cardinality);
	} else {
		bind_info.bind_data.stats = sabot_sql::make_uniq<sabot_sql::NodeStatistics>(cardinality, cardinality);
	}
}

void sabot_sql_bind_set_error(sabot_sql_bind_info info, const char *error) {
	if (!info || !error) {
		return;
	}
	auto &bind_info = GetCTableFunctionBindInfo(info);
	bind_info.error = error;
	bind_info.success = false;
}

//===--------------------------------------------------------------------===//
// Init Interface
//===--------------------------------------------------------------------===//
using sabot_sql::GetCInitInfo;

void *sabot_sql_init_get_extra_info(sabot_sql_init_info info) {
	if (!info) {
		return nullptr;
	}
	auto init_info = reinterpret_cast<sabot_sql::CTableInternalInitInfo *>(info);
	return init_info->bind_data.info.extra_info;
}

void *sabot_sql_init_get_bind_data(sabot_sql_init_info info) {
	if (!info) {
		return nullptr;
	}
	auto &init_info = GetCInitInfo(info);
	return init_info.bind_data.bind_data;
}

void sabot_sql_init_set_init_data(sabot_sql_init_info info, void *init_data, sabot_sql_delete_callback_t destroy) {
	if (!info) {
		return;
	}
	auto &init_info = GetCInitInfo(info);
	init_info.init_data.init_data = init_data;
	init_info.init_data.delete_callback = destroy;
}

void sabot_sql_init_set_error(sabot_sql_init_info info, const char *error) {
	if (!info || !error) {
		return;
	}
	auto &init_info = GetCInitInfo(info);
	init_info.error = error;
	init_info.success = false;
}

idx_t sabot_sql_init_get_column_count(sabot_sql_init_info info) {
	if (!info) {
		return 0;
	}
	auto &init_info = GetCInitInfo(info);
	return init_info.column_ids.size();
}

idx_t sabot_sql_init_get_column_index(sabot_sql_init_info info, idx_t column_index) {
	if (!info) {
		return 0;
	}
	auto &init_info = GetCInitInfo(info);
	if (column_index >= init_info.column_ids.size()) {
		return 0;
	}
	return init_info.column_ids[column_index];
}

void sabot_sql_init_set_max_threads(sabot_sql_init_info info, idx_t max_threads) {
	if (!info) {
		return;
	}
	auto &init_info = GetCInitInfo(info);
	init_info.init_data.max_threads = max_threads;
}

//===--------------------------------------------------------------------===//
// Function Interface
//===--------------------------------------------------------------------===//
using sabot_sql::GetCTableFunctionInfo;

void *sabot_sql_function_get_extra_info(sabot_sql_function_info info) {
	if (!info) {
		return nullptr;
	}
	auto &function_info = GetCTableFunctionInfo(info);
	return function_info.bind_data.info.extra_info;
}

void *sabot_sql_function_get_bind_data(sabot_sql_function_info info) {
	if (!info) {
		return nullptr;
	}
	auto &function_info = GetCTableFunctionInfo(info);
	return function_info.bind_data.bind_data;
}

void *sabot_sql_function_get_init_data(sabot_sql_function_info info) {
	if (!info) {
		return nullptr;
	}
	auto &function_info = GetCTableFunctionInfo(info);
	return function_info.init_data.init_data;
}

void *sabot_sql_function_get_local_init_data(sabot_sql_function_info info) {
	if (!info) {
		return nullptr;
	}
	auto &function_info = GetCTableFunctionInfo(info);
	return function_info.local_data.init_data;
}

void sabot_sql_function_set_error(sabot_sql_function_info info, const char *error) {
	if (!info || !error) {
		return;
	}
	auto &function_info = GetCTableFunctionInfo(info);
	function_info.error = error;
	function_info.success = false;
}
