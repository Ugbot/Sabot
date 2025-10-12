#include "sabot_sql/function/table/system_functions.hpp"

#include "sabot_sql/catalog/catalog.hpp"
#include "sabot_sql/catalog/catalog_entry/table_catalog_entry.hpp"
#include "sabot_sql/catalog/catalog_entry/view_catalog_entry.hpp"
#include "sabot_sql/parser/qualified_name.hpp"
#include "sabot_sql/parser/constraints/not_null_constraint.hpp"
#include "sabot_sql/parser/constraints/unique_constraint.hpp"
#include "sabot_sql/planner/expression/bound_parameter_expression.hpp"
#include "sabot_sql/planner/binder.hpp"

#include "sabot_sql/common/exception.hpp"
#include "sabot_sql/common/limits.hpp"

#include <algorithm>

namespace sabot_sql {

struct SabotSQLTableSampleFunctionData : public TableFunctionData {
	explicit SabotSQLTableSampleFunctionData(CatalogEntry &entry_p) : entry(entry_p) {
	}
	CatalogEntry &entry;
};

struct SabotSQLTableSampleOperatorData : public GlobalTableFunctionState {
	SabotSQLTableSampleOperatorData() : sample_offset(0) {
		sample = nullptr;
	}
	idx_t sample_offset;
	unique_ptr<BlockingSample> sample;
};

static unique_ptr<FunctionData> SabotSQLTableSampleBind(ClientContext &context, TableFunctionBindInput &input,
                                                      vector<LogicalType> &return_types, vector<string> &names) {

	// look up the table name in the catalog
	auto qname = QualifiedName::Parse(input.inputs[0].GetValue<string>());
	Binder::BindSchemaOrCatalog(context, qname.catalog, qname.schema);

	auto &entry = Catalog::GetEntry<TableCatalogEntry>(context, qname.catalog, qname.schema, qname.name);
	if (entry.type != CatalogType::TABLE_ENTRY) {
		throw NotImplementedException("Invalid Catalog type passed to table_sample()");
	}
	auto &table_entry = entry.Cast<TableCatalogEntry>();
	auto types = table_entry.GetTypes();
	for (auto &type : types) {
		return_types.push_back(type);
	}
	for (idx_t i = 0; i < types.size(); i++) {
		auto logical_index = LogicalIndex(i);
		auto &col = table_entry.GetColumn(logical_index);
		names.push_back(col.GetName());
	}

	return make_uniq<SabotSQLTableSampleFunctionData>(entry);
}

unique_ptr<GlobalTableFunctionState> SabotSQLTableSampleInit(ClientContext &context, TableFunctionInitInput &input) {
	return make_uniq<SabotSQLTableSampleOperatorData>();
}

static void SabotSQLTableSampleTable(ClientContext &context, SabotSQLTableSampleOperatorData &data,
                                   TableCatalogEntry &table, DataChunk &output) {
	// if table has statistics.
	// copy the sample of statistics into the output chunk
	if (!data.sample) {
		data.sample = table.GetSample();
	}
	if (data.sample) {
		auto sample_chunk = data.sample->GetChunk();
		if (sample_chunk) {
			sample_chunk->Copy(output, 0);
			data.sample_offset += sample_chunk->size();
		}
	}
}

static void SabotSQLTableSampleFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &bind_data = data_p.bind_data->Cast<SabotSQLTableSampleFunctionData>();
	auto &state = data_p.global_state->Cast<SabotSQLTableSampleOperatorData>();
	switch (bind_data.entry.type) {
	case CatalogType::TABLE_ENTRY:
		SabotSQLTableSampleTable(context, state, bind_data.entry.Cast<TableCatalogEntry>(), output);
		break;
	default:
		throw NotImplementedException("Unimplemented catalog type for pragma_table_sample");
	}
}

void SabotSQLTableSample::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(TableFunction("sabot_sql_table_sample", {LogicalType::VARCHAR}, SabotSQLTableSampleFunction,
	                              SabotSQLTableSampleBind, SabotSQLTableSampleInit));
}

} // namespace sabot_sql
