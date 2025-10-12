//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/main/materialized_query_result.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/common/types/column/column_data_collection.hpp"
#include "sabot_sql/common/winapi.hpp"
#include "sabot_sql/main/query_result.hpp"

namespace sabot_sql {

class ClientContext;

class MaterializedQueryResult : public QueryResult {
public:
	static constexpr const QueryResultType TYPE = QueryResultType::MATERIALIZED_RESULT;

public:
	friend class ClientContext;
	//! Creates a successful query result with the specified names and types
	SABOT_SQL_API MaterializedQueryResult(StatementType statement_type, StatementProperties properties,
	                                   vector<string> names, unique_ptr<ColumnDataCollection> collection,
	                                   ClientProperties client_properties);
	//! Creates an unsuccessful query result with error condition
	SABOT_SQL_API explicit MaterializedQueryResult(ErrorData error);

public:
	//! Fetches a DataChunk from the query result.
	//! This will consume the result (i.e. the result can only be scanned once with this function)
	SABOT_SQL_API unique_ptr<DataChunk> Fetch() override;
	SABOT_SQL_API unique_ptr<DataChunk> FetchRaw() override;
	//! Converts the QueryResult to a string
	SABOT_SQL_API string ToString() override;
	SABOT_SQL_API string ToBox(ClientContext &context, const BoxRendererConfig &config) override;

	//! Gets the (index) value of the (column index) column.
	//! Note: this is very slow. Scanning over the underlying collection is much faster.
	SABOT_SQL_API Value GetValue(idx_t column, idx_t index);

	template <class T>
	T GetValue(idx_t column, idx_t index) {
		auto value = GetValue(column, index);
		return (T)value.GetValue<int64_t>();
	}

	SABOT_SQL_API idx_t RowCount() const;

	//! Returns a reference to the underlying column data collection
	ColumnDataCollection &Collection();

	//! Takes ownership of the collection, 'collection' is null after this operation
	unique_ptr<ColumnDataCollection> TakeCollection();

private:
	unique_ptr<ColumnDataCollection> collection;
	//! Row collection, only created if GetValue is called
	unique_ptr<ColumnDataRowCollection> row_collection;
	//! Scan state for Fetch calls
	ColumnDataScanState scan_state;
	bool scan_initialized;
};

} // namespace sabot_sql
