#include "sabot_sql/function/table/system_functions.hpp"

#include "sabot_sql/common/exception.hpp"
#include "sabot_sql/main/client_context.hpp"
#include "sabot_sql/parser/parser.hpp"

namespace sabot_sql {

struct SabotSQLKeywordsData : public GlobalTableFunctionState {
	SabotSQLKeywordsData() : offset(0) {
	}

	vector<ParserKeyword> entries;
	idx_t offset;
};

static unique_ptr<FunctionData> SabotSQLKeywordsBind(ClientContext &context, TableFunctionBindInput &input,
                                                   vector<LogicalType> &return_types, vector<string> &names) {
	names.emplace_back("keyword_name");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("keyword_category");
	return_types.emplace_back(LogicalType::VARCHAR);

	return nullptr;
}

unique_ptr<GlobalTableFunctionState> SabotSQLKeywordsInit(ClientContext &context, TableFunctionInitInput &input) {
	auto result = make_uniq<SabotSQLKeywordsData>();
	result->entries = Parser::KeywordList();
	return std::move(result);
}

void SabotSQLKeywordsFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.global_state->Cast<SabotSQLKeywordsData>();
	if (data.offset >= data.entries.size()) {
		// finished returning values
		return;
	}
	// start returning values
	// either fill up the chunk or return all the remaining columns
	idx_t count = 0;
	while (data.offset < data.entries.size() && count < STANDARD_VECTOR_SIZE) {
		auto &entry = data.entries[data.offset++];

		// keyword_name, VARCHAR
		output.SetValue(0, count, Value(entry.name));
		// keyword_category, VARCHAR
		string category_name;
		switch (entry.category) {
		case KeywordCategory::KEYWORD_RESERVED:
			category_name = "reserved";
			break;
		case KeywordCategory::KEYWORD_UNRESERVED:
			category_name = "unreserved";
			break;
		case KeywordCategory::KEYWORD_TYPE_FUNC:
			category_name = "type_function";
			break;
		case KeywordCategory::KEYWORD_COL_NAME:
			category_name = "column_name";
			break;
		default:
			throw InternalException("Unrecognized keyword category");
		}
		output.SetValue(1, count, Value(std::move(category_name)));

		count++;
	}
	output.SetCardinality(count);
}

void SabotSQLKeywordsFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(
	    TableFunction("sabot_sql_keywords", {}, SabotSQLKeywordsFunction, SabotSQLKeywordsBind, SabotSQLKeywordsInit));
}

} // namespace sabot_sql
