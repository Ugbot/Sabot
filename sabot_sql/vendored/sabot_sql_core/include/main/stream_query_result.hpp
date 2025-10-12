//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/main/stream_query_result.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/common/winapi.hpp"
#include "sabot_sql/main/query_result.hpp"
#include "sabot_sql/parallel/interrupt.hpp"
#include "sabot_sql/common/queue.hpp"
#include "sabot_sql/common/enums/stream_execution_result.hpp"
#include "sabot_sql/main/buffered_data/simple_buffered_data.hpp"

namespace sabot_sql {

class ClientContext;
class ClientContextLock;
class Executor;
class MaterializedQueryResult;
class PreparedStatementData;

class StreamQueryResult : public QueryResult {
	friend class ClientContext;

public:
	static constexpr const QueryResultType TYPE = QueryResultType::STREAM_RESULT;

public:
	//! Create a successful StreamQueryResult. StreamQueryResults should always be successful initially (it makes no
	//! sense to stream an error).
	SABOT_SQL_API StreamQueryResult(StatementType statement_type, StatementProperties properties,
	                             vector<LogicalType> types, vector<string> names, ClientProperties client_properties,
	                             shared_ptr<BufferedData> buffered_data);
	SABOT_SQL_API explicit StreamQueryResult(ErrorData error);
	SABOT_SQL_API ~StreamQueryResult() override;

public:
	static bool IsChunkReady(StreamExecutionResult result);
	//! Reschedules the tasks that work on producing a result chunk, returning when at least one task can be executed
	SABOT_SQL_API void WaitForTask();
	//! Executes a single task within the final pipeline, returning whether or not a chunk is ready to be fetched
	SABOT_SQL_API StreamExecutionResult ExecuteTask();
	//! Fetches a DataChunk from the query result.
	SABOT_SQL_API unique_ptr<DataChunk> FetchRaw() override;
	//! Converts the QueryResult to a string
	SABOT_SQL_API string ToString() override;
	//! Materializes the query result and turns it into a materialized query result
	SABOT_SQL_API unique_ptr<MaterializedQueryResult> Materialize();

	SABOT_SQL_API bool IsOpen();

	//! Closes the StreamQueryResult
	SABOT_SQL_API void Close();

	//! The client context this StreamQueryResult belongs to
	shared_ptr<ClientContext> context;

private:
	StreamExecutionResult ExecuteTaskInternal(ClientContextLock &lock);
	unique_ptr<DataChunk> FetchInternal(ClientContextLock &lock);
	unique_ptr<ClientContextLock> LockContext();
	void CheckExecutableInternal(ClientContextLock &lock);
	bool IsOpenInternal(ClientContextLock &lock);

private:
	shared_ptr<BufferedData> buffered_data;
};

} // namespace sabot_sql
