#include "sabot_sql/function/table/system_functions.hpp"

#include "sabot_sql/catalog/catalog.hpp"
#include "sabot_sql/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "sabot_sql/catalog/catalog_entry/type_catalog_entry.hpp"
#include "sabot_sql/common/enum_util.hpp"
#include "sabot_sql/common/exception.hpp"
#include "sabot_sql/main/client_context.hpp"
#include "sabot_sql/main/client_config.hpp"
#include "sabot_sql/main/client_data.hpp"

namespace sabot_sql {

struct VariableData {
	string name;
	Value value;
};

struct SabotSQLVariablesData : public GlobalTableFunctionState {
	SabotSQLVariablesData() : offset(0) {
	}

	vector<VariableData> variables;
	idx_t offset;
};

static unique_ptr<FunctionData> SabotSQLVariablesBind(ClientContext &context, TableFunctionBindInput &input,
                                                    vector<LogicalType> &return_types, vector<string> &names) {
	names.emplace_back("name");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("value");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("type");
	return_types.emplace_back(LogicalType::VARCHAR);

	return nullptr;
}

unique_ptr<GlobalTableFunctionState> SabotSQLVariablesInit(ClientContext &context, TableFunctionInitInput &input) {
	auto result = make_uniq<SabotSQLVariablesData>();
	auto &config = ClientConfig::GetConfig(context);

	for (auto &entry : config.user_variables) {
		VariableData data;
		data.name = entry.first;
		data.value = entry.second;
		result->variables.push_back(std::move(data));
	}
	return std::move(result);
}

void SabotSQLVariablesFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.global_state->Cast<SabotSQLVariablesData>();
	if (data.offset >= data.variables.size()) {
		// finished returning values
		return;
	}
	// start returning values
	// either fill up the chunk or return all the remaining columns
	idx_t count = 0;
	while (data.offset < data.variables.size() && count < STANDARD_VECTOR_SIZE) {
		auto &variable_entry = data.variables[data.offset++];

		// return values:
		idx_t col = 0;
		// name, VARCHAR
		output.SetValue(col++, count, Value(variable_entry.name));
		// value, BIGINT
		output.SetValue(col++, count, Value(variable_entry.value.ToString()));
		// type, VARCHAR
		output.SetValue(col, count, Value(variable_entry.value.type().ToString()));
		count++;
	}
	output.SetCardinality(count);
}

void SabotSQLVariablesFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(
	    TableFunction("sabot_sql_variables", {}, SabotSQLVariablesFunction, SabotSQLVariablesBind, SabotSQLVariablesInit));
}

} // namespace sabot_sql
