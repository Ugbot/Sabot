#include "sabot_sql/function/table/system_functions.hpp"

#include "sabot_sql/main/client_context.hpp"
#include "sabot_sql/parser/parser.hpp"
#include "sabot_sql/parser/parser_options.hpp"
#include "sabot_sql/logging/log_manager.hpp"
#include "sabot_sql/logging/log_storage.hpp"
#include "sabot_sql/parser/tableref/subqueryref.hpp"

namespace sabot_sql {

struct SabotSQLLogData : public GlobalTableFunctionState {
	explicit SabotSQLLogData(shared_ptr<LogStorage> log_storage_p) : log_storage(std::move(log_storage_p)) {
		scan_state = log_storage->CreateScanState(LoggingTargetTable::LOG_ENTRIES);
		log_storage->InitializeScan(*scan_state);
	}
	SabotSQLLogData() : log_storage(nullptr) {
	}

	//! The log storage we are scanning
	shared_ptr<LogStorage> log_storage;
	unique_ptr<LogStorageScanState> scan_state;
};

static unique_ptr<FunctionData> SabotSQLLogBind(ClientContext &context, TableFunctionBindInput &input,
                                              vector<LogicalType> &return_types, vector<string> &names) {
	names.emplace_back("context_id");
	return_types.emplace_back(LogicalType::UBIGINT);

	names.emplace_back("timestamp");
	return_types.emplace_back(LogicalType::TIMESTAMP_TZ);

	names.emplace_back("type");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("log_level");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("message");
	return_types.emplace_back(LogicalType::VARCHAR);

	return nullptr;
}

unique_ptr<GlobalTableFunctionState> SabotSQLLogInit(ClientContext &context, TableFunctionInitInput &input) {
	if (LogManager::Get(context).CanScan(LoggingTargetTable::LOG_ENTRIES)) {
		return make_uniq<SabotSQLLogData>(LogManager::Get(context).GetLogStorage());
	}
	return make_uniq<SabotSQLLogData>();
}

void SabotSQLLogFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.global_state->Cast<SabotSQLLogData>();
	if (data.log_storage) {
		data.log_storage->Scan(*data.scan_state, output);
	}
}

unique_ptr<TableRef> SabotSQLLogBindReplace(ClientContext &context, TableFunctionBindInput &input) {
	auto log_storage = LogManager::Get(context).GetLogStorage();

	bool denormalized_table = false;
	auto denormalized_table_setting = input.named_parameters.find("denormalized_table");
	if (denormalized_table_setting != input.named_parameters.end()) {
		denormalized_table = denormalized_table_setting->second.GetValue<bool>();
	}

	// Without join contexts we simply scan the LOG_ENTRIES tables
	if (!denormalized_table) {
		auto res = log_storage->BindReplace(context, input, LoggingTargetTable::LOG_ENTRIES);
		return res;
	}

	// If the storage can bind replace for LoggingTargetTable::ALL_LOGS, we use that since that will be most efficient
	auto all_log_scan = log_storage->BindReplace(context, input, LoggingTargetTable::ALL_LOGS);
	if (all_log_scan) {
		return all_log_scan;
	}

	// We cannot scan ALL_LOGS but denormalized_table was requested: we need to inject the join between LOG_ENTRIES and
	// LOG_CONTEXTS
	string sub_query_string = "SELECT l.context_id, scope, connection_id, transaction_id, query_id, thread_id, "
	                          "timestamp, type, log_level, message"
	                          " FROM (SELECT row_number() OVER () AS rowid, * FROM sabot_sql_logs()) as l JOIN "
	                          "sabot_sql_log_contexts() as c ON l.context_id=c.context_id order by timestamp, l.rowid;";
	Parser parser(context.GetParserOptions());
	parser.ParseQuery(sub_query_string);
	auto select_stmt = unique_ptr_cast<SQLStatement, SelectStatement>(std::move(parser.statements[0]));

	return sabot_sql::make_uniq<SubqueryRef>(std::move(select_stmt));
}

void SabotSQLLogFun::RegisterFunction(BuiltinFunctions &set) {
	TableFunction logs_fun("sabot_sql_logs", {}, SabotSQLLogFunction, SabotSQLLogBind, SabotSQLLogInit);
	logs_fun.bind_replace = SabotSQLLogBindReplace;
	logs_fun.named_parameters["denormalized_table"] = LogicalType::BOOLEAN;
	set.AddFunction(logs_fun);
}

} // namespace sabot_sql
