//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/main/pending_query_result.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/main/query_result.hpp"
#include "sabot_sql/common/enums/pending_execution_result.hpp"
#include "sabot_sql/execution/executor.hpp"

namespace sabot_sql {
class ClientContext;
class ClientContextLock;
class PreparedStatementData;

class PendingQueryResult : public BaseQueryResult {
	friend class ClientContext;

public:
	static constexpr const QueryResultType TYPE = QueryResultType::PENDING_RESULT;

public:
	SABOT_SQL_API PendingQueryResult(shared_ptr<ClientContext> context, PreparedStatementData &statement,
	                              vector<LogicalType> types, bool allow_stream_result);
	SABOT_SQL_API explicit PendingQueryResult(ErrorData error_message);
	SABOT_SQL_API ~PendingQueryResult() override;
	SABOT_SQL_API bool AllowStreamResult() const;
	PendingQueryResult(const PendingQueryResult &) = delete;
	PendingQueryResult &operator=(const PendingQueryResult &) = delete;

public:
	//! Executes a single task within the query, returning whether or not the query is ready.
	//! If this returns RESULT_READY, the Execute function can be called to obtain a pointer to the result.
	//! If this returns RESULT_NOT_READY, the ExecuteTask function should be called again.
	//! If this returns EXECUTION_ERROR, an error occurred during execution.
	//! If this returns NO_TASKS_AVAILABLE, this means currently no meaningful work can be done by the current executor,
	//!	    but tasks may become available in the future.
	//! The error message can be obtained by calling GetError() on the PendingQueryResult.
	SABOT_SQL_API PendingExecutionResult ExecuteTask();
	SABOT_SQL_API PendingExecutionResult CheckPulse();
	//! Halt execution of the thread until a Task is ready to be executed (use with caution)
	void WaitForTask();

	//! Returns the result of the query as an actual query result.
	//! This returns (mostly) instantly if ExecuteTask has been called until RESULT_READY was returned.
	SABOT_SQL_API unique_ptr<QueryResult> Execute();

	SABOT_SQL_API void Close();

	//! Function to determine whether execution is considered finished
	SABOT_SQL_API static bool IsResultReady(PendingExecutionResult result);
	SABOT_SQL_API static bool IsExecutionFinished(PendingExecutionResult result);

private:
	shared_ptr<ClientContext> context;
	bool allow_stream_result;

private:
	void CheckExecutableInternal(ClientContextLock &lock);

	PendingExecutionResult ExecuteTaskInternal(ClientContextLock &lock);
	unique_ptr<QueryResult> ExecuteInternal(ClientContextLock &lock);
	unique_ptr<ClientContextLock> LockContext();
};

} // namespace sabot_sql
