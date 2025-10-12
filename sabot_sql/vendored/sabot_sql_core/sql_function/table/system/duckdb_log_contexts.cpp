#include "sabot_sql/function/table/system_functions.hpp"

#include "sabot_sql/catalog/catalog.hpp"
#include "sabot_sql/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "sabot_sql/common/exception.hpp"
#include "sabot_sql/main/client_context.hpp"
#include "sabot_sql/main/client_data.hpp"
#include "sabot_sql/logging/log_manager.hpp"
#include "sabot_sql/logging/log_storage.hpp"

namespace sabot_sql {

struct SabotSQLLogContextData : public GlobalTableFunctionState {
	explicit SabotSQLLogContextData(shared_ptr<LogStorage> log_storage_p) : log_storage(std::move(log_storage_p)) {
		scan_state = log_storage->CreateScanState(LoggingTargetTable::LOG_CONTEXTS);
		log_storage->InitializeScan(*scan_state);
	}
	SabotSQLLogContextData() : log_storage(nullptr) {
	}

	//! The log storage we are scanning
	shared_ptr<LogStorage> log_storage;
	unique_ptr<LogStorageScanState> scan_state;
};

static unique_ptr<FunctionData> SabotSQLLogContextBind(ClientContext &context, TableFunctionBindInput &input,
                                                     vector<LogicalType> &return_types, vector<string> &names) {
	names.emplace_back("context_id");
	return_types.emplace_back(LogicalType::UBIGINT);

	names.emplace_back("scope");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("connection_id");
	return_types.emplace_back(LogicalType::UBIGINT);

	names.emplace_back("transaction_id");
	return_types.emplace_back(LogicalType::UBIGINT);

	names.emplace_back("query_id");
	return_types.emplace_back(LogicalType::UBIGINT);

	names.emplace_back("thread_id");
	return_types.emplace_back(LogicalType::UBIGINT);

	return nullptr;
}

unique_ptr<GlobalTableFunctionState> SabotSQLLogContextInit(ClientContext &context, TableFunctionInitInput &input) {
	if (LogManager::Get(context).CanScan(LoggingTargetTable::LOG_CONTEXTS)) {
		return make_uniq<SabotSQLLogContextData>(LogManager::Get(context).GetLogStorage());
	}
	return make_uniq<SabotSQLLogContextData>();
}

void SabotSQLLogContextFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.global_state->Cast<SabotSQLLogContextData>();
	if (data.log_storage) {
		data.log_storage->Scan(*data.scan_state, output);
	}
}

static unique_ptr<TableRef> SabotSQLLogContextsBindReplace(ClientContext &context, TableFunctionBindInput &input) {
	auto log_storage = LogManager::Get(context).GetLogStorage();

	// Attempt to let the storage BindReplace the scan function
	return log_storage->BindReplace(context, input, LoggingTargetTable::LOG_CONTEXTS);
}

void SabotSQLLogContextFun::RegisterFunction(BuiltinFunctions &set) {
	auto fun =
	    TableFunction("sabot_sql_log_contexts", {}, SabotSQLLogContextFunction, SabotSQLLogContextBind, SabotSQLLogContextInit);
	fun.bind_replace = SabotSQLLogContextsBindReplace;
	set.AddFunction(fun);
}

} // namespace sabot_sql
