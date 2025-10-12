//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql_python/arrow/arrow_query_result.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/common/winapi.hpp"
#include "sabot_sql/common/arrow/arrow_wrapper.hpp"
#include "sabot_sql/main/query_result.hpp"
#include "sabot_sql/common/error_data.hpp"
#include "sabot_sql/common/unique_ptr.hpp"

namespace sabot_sql {

class ClientContext;

class ArrowQueryResult : public QueryResult {
public:
	static constexpr QueryResultType TYPE = QueryResultType::ARROW_RESULT;

public:
	friend class ClientContext;
	//! Creates a successful query result with the specified names and types
	SABOT_SQL_API ArrowQueryResult(StatementType statement_type, StatementProperties properties, vector<string> names_p,
	                            vector<LogicalType> types_p, ClientProperties client_properties, idx_t batch_size);
	//! Creates an unsuccessful query result with error condition
	SABOT_SQL_API explicit ArrowQueryResult(ErrorData error);

public:
	//! Fetches a DataChunk from the query result.
	//! This will consume the result (i.e. the result can only be scanned once with this function)
	SABOT_SQL_API unique_ptr<DataChunk> Fetch() override;
	SABOT_SQL_API unique_ptr<DataChunk> FetchRaw() override;
	//! Converts the QueryResult to a string
	SABOT_SQL_API string ToString() override;

public:
	vector<unique_ptr<ArrowArrayWrapper>> ConsumeArrays();
	vector<unique_ptr<ArrowArrayWrapper>> &Arrays();
	void SetArrowData(vector<unique_ptr<ArrowArrayWrapper>> arrays);
	idx_t BatchSize() const;

private:
	vector<unique_ptr<ArrowArrayWrapper>> arrays;
	idx_t batch_size;
};

} // namespace sabot_sql
