#include "sabot_sql/function/table/system_functions.hpp"

#include "sabot_sql/main/client_context.hpp"
#include "sabot_sql/main/database.hpp"
#include "sabot_sql/main/connection_manager.hpp"

namespace sabot_sql {

struct SabotSQLConnectionCountData : public GlobalTableFunctionState {
	SabotSQLConnectionCountData() : count(0), finished(false) {
	}
	idx_t count;
	bool finished;
};

static unique_ptr<FunctionData> SabotSQLConnectionCountBind(ClientContext &context, TableFunctionBindInput &input,
                                                          vector<LogicalType> &return_types, vector<string> &names) {
	names.emplace_back("count");
	return_types.emplace_back(LogicalType::UBIGINT);
	return nullptr;
}

unique_ptr<GlobalTableFunctionState> SabotSQLConnectionCountInit(ClientContext &context, TableFunctionInitInput &input) {
	auto result = make_uniq<SabotSQLConnectionCountData>();
	auto &conn_manager = context.db->GetConnectionManager();
	result->count = conn_manager.GetConnectionCount();
	return std::move(result);
}

void SabotSQLConnectionCountFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.global_state->Cast<SabotSQLConnectionCountData>();
	if (data.finished) {
		return;
	}
	output.SetValue(0, 0, Value::UBIGINT(data.count));
	output.SetCardinality(1);
	data.finished = true;
}

void SabotSQLConnectionCountFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(TableFunction("sabot_sql_connection_count", {}, SabotSQLConnectionCountFunction,
	                              SabotSQLConnectionCountBind, SabotSQLConnectionCountInit));
}

} // namespace sabot_sql
