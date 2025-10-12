#include "sabot_sql/function/table/system_functions.hpp"
#include "sabot_sql/storage/external_file_cache.hpp"

namespace sabot_sql {

struct SabotSQLExternalFileCacheData : public GlobalTableFunctionState {
	SabotSQLExternalFileCacheData() : offset(0) {
	}

	vector<CachedFileInformation> entries;
	idx_t offset;
};

static unique_ptr<FunctionData> SabotSQLExternalFileCacheBind(ClientContext &context, TableFunctionBindInput &input,
                                                            vector<LogicalType> &return_types, vector<string> &names) {
	names.emplace_back("path");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("nr_bytes");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("location");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("loaded");
	return_types.emplace_back(LogicalType::BOOLEAN);

	return nullptr;
}

unique_ptr<GlobalTableFunctionState> SabotSQLExternalFileCacheInit(ClientContext &context,
                                                                 TableFunctionInitInput &input) {
	auto result = make_uniq<SabotSQLExternalFileCacheData>();
	result->entries = ExternalFileCache::Get(context).GetCachedFileInformation();
	return std::move(result);
}

void SabotSQLExternalFileCacheFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.global_state->Cast<SabotSQLExternalFileCacheData>();
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
		// path, VARCHAR
		output.SetValue(col++, count, entry.path);
		// nr_bytes, BIGINT
		output.SetValue(col++, count, Value::BIGINT(NumericCast<int64_t>(entry.nr_bytes)));
		// location, BIGINT
		output.SetValue(col++, count, Value::BIGINT(NumericCast<int64_t>(entry.location)));
		// loaded, BOOLEAN
		output.SetValue(col++, count, entry.loaded);
		count++;
	}
	output.SetCardinality(count);
}

void SabotSQLExternalFileCacheFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(TableFunction("sabot_sql_external_file_cache", {}, SabotSQLExternalFileCacheFunction,
	                              SabotSQLExternalFileCacheBind, SabotSQLExternalFileCacheInit));
}

} // namespace sabot_sql
