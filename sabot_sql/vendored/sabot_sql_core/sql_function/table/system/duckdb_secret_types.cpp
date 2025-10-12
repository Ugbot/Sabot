#include "sabot_sql/function/table/system_functions.hpp"
#include "sabot_sql/main/config.hpp"
#include "sabot_sql/main/client_context.hpp"
#include "sabot_sql/common/enum_util.hpp"
#include "sabot_sql/main/secret/secret_manager.hpp"
#include "sabot_sql/main/secret/secret.hpp"

namespace sabot_sql {

struct SabotSQLSecretTypesData : public GlobalTableFunctionState {
	SabotSQLSecretTypesData() : offset(0) {
	}

	vector<SecretType> types;
	idx_t offset;
};

static unique_ptr<FunctionData> SabotSQLSecretTypesBind(ClientContext &context, TableFunctionBindInput &input,
                                                      vector<LogicalType> &return_types, vector<string> &names) {
	names.emplace_back("type");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("default_provider");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("extension");
	return_types.emplace_back(LogicalType::VARCHAR);

	return nullptr;
}

unique_ptr<GlobalTableFunctionState> SabotSQLSecretTypesInit(ClientContext &context, TableFunctionInitInput &input) {
	auto result = make_uniq<SabotSQLSecretTypesData>();

	auto &secret_manager = SecretManager::Get(context);
	result->types = secret_manager.AllSecretTypes();

	return std::move(result);
}

void SabotSQLSecretTypesFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.global_state->Cast<SabotSQLSecretTypesData>();
	if (data.offset >= data.types.size()) {
		// finished returning values
		return;
	}
	// start returning values
	// either fill up the chunk or return all the remaining columns
	idx_t count = 0;
	while (data.offset < data.types.size() && count < STANDARD_VECTOR_SIZE) {
		auto &entry = data.types[data.offset++];

		// return values:
		// type, LogicalType::VARCHAR
		output.SetValue(0, count, Value(entry.name));
		// default_provider, LogicalType::VARCHAR
		output.SetValue(1, count, Value(entry.default_provider));
		// extension, LogicalType::VARCHAR
		output.SetValue(2, count, Value(entry.extension));

		count++;
	}
	output.SetCardinality(count);
}

void SabotSQLSecretTypesFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(TableFunction("sabot_sql_secret_types", {}, SabotSQLSecretTypesFunction, SabotSQLSecretTypesBind,
	                              SabotSQLSecretTypesInit));
}

} // namespace sabot_sql
