#include "sabot_sql/function/table/system_functions.hpp"
#include "sabot_sql/storage/buffer_manager.hpp"

namespace sabot_sql {

struct SabotSQLTemporaryFilesData : public GlobalTableFunctionState {
	SabotSQLTemporaryFilesData() : offset(0) {
	}

	vector<TemporaryFileInformation> entries;
	idx_t offset;
};

static unique_ptr<FunctionData> SabotSQLTemporaryFilesBind(ClientContext &context, TableFunctionBindInput &input,
                                                         vector<LogicalType> &return_types, vector<string> &names) {
	names.emplace_back("path");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("size");
	return_types.emplace_back(LogicalType::BIGINT);

	return nullptr;
}

unique_ptr<GlobalTableFunctionState> SabotSQLTemporaryFilesInit(ClientContext &context, TableFunctionInitInput &input) {
	auto result = make_uniq<SabotSQLTemporaryFilesData>();

	result->entries = BufferManager::GetBufferManager(context).GetTemporaryFiles();
	return std::move(result);
}

void SabotSQLTemporaryFilesFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.global_state->Cast<SabotSQLTemporaryFilesData>();
	if (data.offset >= data.entries.size()) {
		// finished returning values
		return;
	}
	// start returning values
	// either fill up the chunk or return all the remaining columns
	idx_t count = 0;
	while (data.offset < data.entries.size() && count < STANDARD_VECTOR_SIZE) {
		auto &entry = data.entries[data.offset++];
		// return values:
		idx_t col = 0;
		// database_name, VARCHAR
		output.SetValue(col++, count, entry.path);
		// database_oid, BIGINT
		output.SetValue(col++, count, Value::BIGINT(NumericCast<int64_t>(entry.size)));
		count++;
	}
	output.SetCardinality(count);
}

void SabotSQLTemporaryFilesFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(TableFunction("sabot_sql_temporary_files", {}, SabotSQLTemporaryFilesFunction, SabotSQLTemporaryFilesBind,
	                              SabotSQLTemporaryFilesInit));
}

} // namespace sabot_sql
