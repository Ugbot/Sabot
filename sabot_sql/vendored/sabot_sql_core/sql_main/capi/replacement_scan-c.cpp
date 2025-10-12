#include "sabot_sql/main/capi/capi_internal.hpp"
#include "sabot_sql/main/config.hpp"
#include "sabot_sql/parser/tableref/table_function_ref.hpp"
#include "sabot_sql/parser/expression/constant_expression.hpp"
#include "sabot_sql/parser/expression/function_expression.hpp"

namespace sabot_sql {

struct CAPIReplacementScanData : public ReplacementScanData {
	~CAPIReplacementScanData() {
		if (delete_callback) {
			delete_callback(extra_data);
		}
	}

	sabot_sql_replacement_callback_t callback;
	void *extra_data;
	sabot_sql_delete_callback_t delete_callback;
};

struct CAPIReplacementScanInfo {
	CAPIReplacementScanInfo(optional_ptr<CAPIReplacementScanData> data) : data(data) {
	}

	optional_ptr<CAPIReplacementScanData> data;
	string function_name;
	vector<Value> parameters;
	string error;
};

unique_ptr<TableRef> sabot_sql_capi_replacement_callback(ClientContext &context, ReplacementScanInput &input,
                                                      optional_ptr<ReplacementScanData> data) {
	auto &table_name = input.table_name;
	auto &scan_data = data->Cast<CAPIReplacementScanData>();

	CAPIReplacementScanInfo info(&scan_data);
	scan_data.callback((sabot_sql_replacement_scan_info)&info, table_name.c_str(), scan_data.extra_data);
	if (!info.error.empty()) {
		throw BinderException("Error in replacement scan: %s\n", info.error);
	}
	if (info.function_name.empty()) {
		// no function provided: bail-out
		return nullptr;
	}
	auto table_function = make_uniq<TableFunctionRef>();
	vector<unique_ptr<ParsedExpression>> children;
	for (auto &param : info.parameters) {
		children.push_back(make_uniq<ConstantExpression>(std::move(param)));
	}
	table_function->function = make_uniq<FunctionExpression>(info.function_name, std::move(children));
	return std::move(table_function);
}

} // namespace sabot_sql

void sabot_sql_add_replacement_scan(sabot_sql_database db, sabot_sql_replacement_callback_t replacement, void *extra_data,
                                 sabot_sql_delete_callback_t delete_callback) {
	if (!db || !replacement) {
		return;
	}
	auto wrapper = reinterpret_cast<sabot_sql::DatabaseWrapper *>(db);
	auto scan_info = sabot_sql::make_uniq<sabot_sql::CAPIReplacementScanData>();
	scan_info->callback = replacement;
	scan_info->extra_data = extra_data;
	scan_info->delete_callback = delete_callback;

	auto &config = sabot_sql::DBConfig::GetConfig(*wrapper->database->instance);
	config.replacement_scans.push_back(
	    sabot_sql::ReplacementScan(sabot_sql::sabot_sql_capi_replacement_callback, std::move(scan_info)));
}

void sabot_sql_replacement_scan_set_function_name(sabot_sql_replacement_scan_info info_p, const char *function_name) {
	if (!info_p || !function_name) {
		return;
	}
	auto info = reinterpret_cast<sabot_sql::CAPIReplacementScanInfo *>(info_p);
	info->function_name = function_name;
}

void sabot_sql_replacement_scan_add_parameter(sabot_sql_replacement_scan_info info_p, sabot_sql_value parameter) {
	if (!info_p || !parameter) {
		return;
	}
	auto info = reinterpret_cast<sabot_sql::CAPIReplacementScanInfo *>(info_p);
	auto val = reinterpret_cast<sabot_sql::Value *>(parameter);
	info->parameters.push_back(*val);
}

void sabot_sql_replacement_scan_set_error(sabot_sql_replacement_scan_info info_p, const char *error) {
	if (!info_p || !error) {
		return;
	}
	auto info = reinterpret_cast<sabot_sql::CAPIReplacementScanInfo *>(info_p);
	info->error = error;
}
