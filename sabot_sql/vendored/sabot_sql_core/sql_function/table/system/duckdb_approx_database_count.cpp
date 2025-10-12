#include "sabot_sql/function/table/system_functions.hpp"
#include "sabot_sql/main/database_manager.hpp"

namespace sabot_sql {

struct SabotSQLApproxDatabaseCountData : public GlobalTableFunctionState {
	SabotSQLApproxDatabaseCountData() : count(0), finished(false) {
	}
	idx_t count;
	bool finished;
};

static unique_ptr<FunctionData> SabotSQLApproxDatabaseCountBind(ClientContext &context, TableFunctionBindInput &input,
                                                              vector<LogicalType> &return_types,
                                                              vector<string> &names) {
	names.emplace_back("approx_count");
	return_types.emplace_back(LogicalType::UBIGINT);
	return nullptr;
}

unique_ptr<GlobalTableFunctionState> SabotSQLApproxDatabaseCountInit(ClientContext &context,
                                                                   TableFunctionInitInput &input) {
	auto result = make_uniq<SabotSQLApproxDatabaseCountData>();
	result->count = DatabaseManager::Get(context).ApproxDatabaseCount();
	return std::move(result);
}

void SabotSQLApproxDatabaseCountFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.global_state->Cast<SabotSQLApproxDatabaseCountData>();
	if (data.finished) {
		return;
	}
	output.SetValue(0, 0, Value::UBIGINT(data.count));
	output.SetCardinality(1);
	data.finished = true;
}

void SabotSQLApproxDatabaseCountFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(TableFunction("sabot_sql_approx_database_count", {}, SabotSQLApproxDatabaseCountFunction,
	                              SabotSQLApproxDatabaseCountBind, SabotSQLApproxDatabaseCountInit));
}

} // namespace sabot_sql
