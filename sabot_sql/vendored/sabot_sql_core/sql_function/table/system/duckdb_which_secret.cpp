#include "sabot_sql/function/table/system_functions.hpp"

#include "sabot_sql/common/file_system.hpp"
#include "sabot_sql/common/map.hpp"
#include "sabot_sql/common/string_util.hpp"
#include "sabot_sql/common/multi_file/multi_file_reader.hpp"
#include "sabot_sql/function/function_set.hpp"
#include "sabot_sql/main/client_context.hpp"
#include "sabot_sql/main/database.hpp"
#include "sabot_sql/main/extension_helper.hpp"
#include "sabot_sql/main/secret/secret_manager.hpp"

namespace sabot_sql {

struct SabotSQLWhichSecretData : public GlobalTableFunctionState {
	SabotSQLWhichSecretData() : finished(false) {
	}
	bool finished;
};

struct SabotSQLWhichSecretBindData : public TableFunctionData {
	explicit SabotSQLWhichSecretBindData(TableFunctionBindInput &tf_input) : inputs(tf_input.inputs) {};

	sabot_sql::vector<sabot_sql::Value> inputs;
};

static unique_ptr<FunctionData> SabotSQLWhichSecretBind(ClientContext &context, TableFunctionBindInput &input,
                                                      vector<LogicalType> &return_types, vector<string> &names) {
	names.emplace_back("name");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("persistent");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("storage");
	return_types.emplace_back(LogicalType::VARCHAR);

	return make_uniq<SabotSQLWhichSecretBindData>(input);
}

unique_ptr<GlobalTableFunctionState> SabotSQLWhichSecretInit(ClientContext &context, TableFunctionInitInput &input) {
	return make_uniq<SabotSQLWhichSecretData>();
}

void SabotSQLWhichSecretFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.global_state->Cast<SabotSQLWhichSecretData>();
	if (data.finished) {
		// finished returning values
		return;
	}
	auto &bind_data = data_p.bind_data->Cast<SabotSQLWhichSecretBindData>();

	auto &secret_manager = SecretManager::Get(context);
	auto transaction = CatalogTransaction::GetSystemCatalogTransaction(context);

	auto &inputs = bind_data.inputs;
	auto path = inputs[0].ToString();
	auto type = inputs[1].ToString();
	auto secret_match = secret_manager.LookupSecret(transaction, path, type);
	if (secret_match.HasMatch()) {
		auto &secret_entry = *secret_match.secret_entry;
		output.SetCardinality(1);
		output.SetValue(0, 0, secret_entry.secret->GetName());
		output.SetValue(1, 0, EnumUtil::ToString(secret_entry.persist_type));
		output.SetValue(2, 0, secret_entry.storage_mode);
	}
	data.finished = true;
}

void SabotSQLWhichSecretFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(TableFunction("which_secret", {sabot_sql::LogicalType::VARCHAR, sabot_sql::LogicalType::VARCHAR},
	                              SabotSQLWhichSecretFunction, SabotSQLWhichSecretBind, SabotSQLWhichSecretInit));
}

} // namespace sabot_sql
