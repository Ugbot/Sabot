#include "sabot_sql/function/table/system_functions.hpp"
#include "sabot_sql/main/database.hpp"
#include "sabot_sql/common/string_util.hpp"
#include "sabot_sql/common/platform.hpp"

#include <cstdint>

namespace sabot_sql {

struct PragmaVersionData : public GlobalTableFunctionState {
	PragmaVersionData() : finished(false) {
	}

	bool finished;
};

static unique_ptr<FunctionData> PragmaVersionBind(ClientContext &context, TableFunctionBindInput &input,
                                                  vector<LogicalType> &return_types, vector<string> &names) {
	names.emplace_back("library_version");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("source_id");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("codename");
	return_types.emplace_back(LogicalType::VARCHAR);
	return nullptr;
}

static unique_ptr<GlobalTableFunctionState> PragmaVersionInit(ClientContext &context, TableFunctionInitInput &input) {
	return make_uniq<PragmaVersionData>();
}

static void PragmaVersionFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.global_state->Cast<PragmaVersionData>();
	if (data.finished) {
		// finished returning values
		return;
	}
	output.SetCardinality(1);
	output.SetValue(0, 0, SabotSQL::LibraryVersion());
	output.SetValue(1, 0, SabotSQL::SourceID());
	output.SetValue(2, 0, SabotSQL::ReleaseCodename());

	data.finished = true;
}

void PragmaVersion::RegisterFunction(BuiltinFunctions &set) {
	TableFunction pragma_version("pragma_version", {}, PragmaVersionFunction);
	pragma_version.bind = PragmaVersionBind;
	pragma_version.init_global = PragmaVersionInit;
	set.AddFunction(pragma_version);
}

idx_t SabotSQL::StandardVectorSize() {
	return STANDARD_VECTOR_SIZE;
}

const char *SabotSQL::SourceID() {
	return SABOT_SQL_SOURCE_ID;
}

const char *SabotSQL::LibraryVersion() {
	return SABOT_SQL_VERSION;
}

const char *SabotSQL::ReleaseCodename() {
	// dev releases have no name
	if (StringUtil::Contains(SABOT_SQL_VERSION, "-dev")) {
		return "Development Version";
	}
	if (StringUtil::StartsWith(SABOT_SQL_VERSION, "v1.2.")) {
		return "Histrionicus";
	}
	if (StringUtil::StartsWith(SABOT_SQL_VERSION, "v1.3.")) {
		return "Ossivalis";
	}
	if (StringUtil::StartsWith(SABOT_SQL_VERSION, "v1.4.")) {
		return "Andium";
	}
	// add new version names here

	// we should not get here, but let's not fail because of it because tags on forks can be whatever
	return "Unknown Version";
}

string SabotSQL::Platform() {
	return SabotSQLPlatform();
}

struct PragmaPlatformData : public GlobalTableFunctionState {
	PragmaPlatformData() : finished(false) {
	}

	bool finished;
};

static unique_ptr<FunctionData> PragmaPlatformBind(ClientContext &context, TableFunctionBindInput &input,
                                                   vector<LogicalType> &return_types, vector<string> &names) {
	names.emplace_back("platform");
	return_types.emplace_back(LogicalType::VARCHAR);
	return nullptr;
}

static unique_ptr<GlobalTableFunctionState> PragmaPlatformInit(ClientContext &context, TableFunctionInitInput &input) {
	return make_uniq<PragmaPlatformData>();
}

static void PragmaPlatformFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.global_state->Cast<PragmaPlatformData>();
	if (data.finished) {
		// finished returning values
		return;
	}
	output.SetCardinality(1);
	output.SetValue(0, 0, SabotSQL::Platform());
	data.finished = true;
}

void PragmaPlatform::RegisterFunction(BuiltinFunctions &set) {
	TableFunction pragma_platform("pragma_platform", {}, PragmaPlatformFunction);
	pragma_platform.bind = PragmaPlatformBind;
	pragma_platform.init_global = PragmaPlatformInit;
	set.AddFunction(pragma_platform);
}

} // namespace sabot_sql
