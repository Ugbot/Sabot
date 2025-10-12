#include "sabot_sql/function/table/system_functions.hpp"

#include "sabot_sql/main/config.hpp"
#include "sabot_sql/main/client_context.hpp"
#include "sabot_sql/common/enum_util.hpp"
#include "sabot_sql/common/enums/optimizer_type.hpp"

namespace sabot_sql {

struct SabotSQLOptimizersData : public GlobalTableFunctionState {
	SabotSQLOptimizersData() : offset(0) {
	}

	vector<string> optimizers;
	idx_t offset;
};

static unique_ptr<FunctionData> SabotSQLOptimizersBind(ClientContext &context, TableFunctionBindInput &input,
                                                     vector<LogicalType> &return_types, vector<string> &names) {
	names.emplace_back("name");
	return_types.emplace_back(LogicalType::VARCHAR);

	return nullptr;
}

unique_ptr<GlobalTableFunctionState> SabotSQLOptimizersInit(ClientContext &context, TableFunctionInitInput &input) {
	auto result = make_uniq<SabotSQLOptimizersData>();
	result->optimizers = ListAllOptimizers();
	return std::move(result);
}

void SabotSQLOptimizersFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.global_state->Cast<SabotSQLOptimizersData>();
	if (data.offset >= data.optimizers.size()) {
		// finished returning values
		return;
	}
	// start returning values
	// either fill up the chunk or return all the remaining columns
	idx_t count = 0;
	while (data.offset < data.optimizers.size() && count < STANDARD_VECTOR_SIZE) {
		auto &entry = data.optimizers[data.offset++];

		// return values:
		// name, LogicalType::VARCHAR
		output.SetValue(0, count, Value(entry));
		count++;
	}
	output.SetCardinality(count);
}

void SabotSQLOptimizersFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(
	    TableFunction("sabot_sql_optimizers", {}, SabotSQLOptimizersFunction, SabotSQLOptimizersBind, SabotSQLOptimizersInit));
}

} // namespace sabot_sql
