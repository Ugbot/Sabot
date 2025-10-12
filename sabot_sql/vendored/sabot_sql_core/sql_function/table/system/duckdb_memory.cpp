#include "sabot_sql/function/table/system_functions.hpp"
#include "sabot_sql/storage/buffer_manager.hpp"

namespace sabot_sql {

struct SabotSQLMemoryData : public GlobalTableFunctionState {
	SabotSQLMemoryData() : offset(0) {
	}

	vector<MemoryInformation> entries;
	idx_t offset;
};

static unique_ptr<FunctionData> SabotSQLMemoryBind(ClientContext &context, TableFunctionBindInput &input,
                                                 vector<LogicalType> &return_types, vector<string> &names) {
	names.emplace_back("tag");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("memory_usage_bytes");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("temporary_storage_bytes");
	return_types.emplace_back(LogicalType::BIGINT);

	return nullptr;
}

unique_ptr<GlobalTableFunctionState> SabotSQLMemoryInit(ClientContext &context, TableFunctionInitInput &input) {
	auto result = make_uniq<SabotSQLMemoryData>();
	result->entries = BufferManager::GetBufferManager(context).GetMemoryUsageInfo();
	return std::move(result);
}

int64_t ClampReportedMemory(idx_t memory_usage) {
	if (memory_usage > static_cast<idx_t>(NumericLimits<int64_t>::Maximum())) {
		return 0;
	}
	return UnsafeNumericCast<int64_t>(memory_usage);
}

void SabotSQLMemoryFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.global_state->Cast<SabotSQLMemoryData>();
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
		// tag, VARCHAR
		output.SetValue(col++, count, EnumUtil::ToString(entry.tag));
		// memory_usage_bytes, BIGINT
		output.SetValue(col++, count, Value::BIGINT(ClampReportedMemory(entry.size)));
		// temporary_storage_bytes, BIGINT
		output.SetValue(col++, count, Value::BIGINT(ClampReportedMemory(entry.evicted_data)));
		count++;
	}
	output.SetCardinality(count);
}

void SabotSQLMemoryFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(TableFunction("sabot_sql_memory", {}, SabotSQLMemoryFunction, SabotSQLMemoryBind, SabotSQLMemoryInit));
}

} // namespace sabot_sql
