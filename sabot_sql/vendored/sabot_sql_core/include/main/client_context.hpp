//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/main/client_context.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "sabot_sql/catalog/catalog_set.hpp"
#include "sabot_sql/common/atomic.hpp"
#include "sabot_sql/common/deque.hpp"
#include "sabot_sql/common/enums/pending_execution_result.hpp"
#include "sabot_sql/common/enums/prepared_statement_mode.hpp"
#include "sabot_sql/common/error_data.hpp"
#include "sabot_sql/common/pair.hpp"
#include "sabot_sql/common/unordered_set.hpp"
#include "sabot_sql/common/winapi.hpp"
#include "sabot_sql/main/client_config.hpp"
#include "sabot_sql/main/client_context_state.hpp"
#include "sabot_sql/main/client_properties.hpp"
#include "sabot_sql/main/external_dependencies.hpp"
#include "sabot_sql/main/pending_query_result.hpp"
#include "sabot_sql/main/prepared_statement.hpp"
#include "sabot_sql/main/stream_query_result.hpp"
#include "sabot_sql/main/table_description.hpp"
#include "sabot_sql/planner/expression/bound_parameter_data.hpp"
#include "sabot_sql/transaction/transaction_context.hpp"

namespace sabot_sql {

class Appender;
class Catalog;
class CatalogSearchPath;
class ColumnDataCollection;
class DatabaseInstance;
class FileOpener;
class LogicalOperator;
class PreparedStatementData;
class Relation;
class BufferedFileWriter;
class QueryProfiler;
class ClientContextLock;
struct CreateScalarFunctionInfo;
class ScalarFunctionCatalogEntry;
struct ActiveQueryContext;
struct ParserOptions;
class SimpleBufferedData;
class BufferedData;
struct ClientData;
class ClientContextState;
class RegisteredStateManager;

struct PendingQueryParameters {
	//! Prepared statement parameters (if any)
	optional_ptr<case_insensitive_map_t<BoundParameterData>> parameters;
	//! Whether a stream result should be allowed
	bool allow_stream_result = false;
};

//! The ClientContext holds information relevant to the current client session
//! during execution
class ClientContext : public enable_shared_from_this<ClientContext> {
	friend class PendingQueryResult;  // LockContext
	friend class BufferedData;        // ExecuteTaskInternal
	friend class SimpleBufferedData;  // ExecuteTaskInternal
	friend class BatchedBufferedData; // ExecuteTaskInternal
	friend class StreamQueryResult;   // LockContext
	friend class ConnectionManager;

public:
	SABOT_SQL_API explicit ClientContext(shared_ptr<DatabaseInstance> db);
	SABOT_SQL_API ~ClientContext();

	//! The database that this client is connected to
	shared_ptr<DatabaseInstance> db;
	//! Whether or not the query is interrupted
	atomic<bool> interrupted;
	//! Set of optional states (e.g. Caches) that can be held by the ClientContext
	unique_ptr<RegisteredStateManager> registered_state;
	//! The logger to be used by this ClientContext
	shared_ptr<Logger> logger;
	//! The client configuration
	ClientConfig config;
	//! The set of client-specific data
	unique_ptr<ClientData> client_data;
	//! Data for the currently running transaction
	TransactionContext transaction;

public:
	MetaTransaction &ActiveTransaction() {
		return transaction.ActiveTransaction();
	}

	//! Interrupt execution of a query
	SABOT_SQL_API void Interrupt();
	SABOT_SQL_API void CancelTransaction();

	//! Enable query profiling
	SABOT_SQL_API void EnableProfiling();
	//! Disable query profiling
	SABOT_SQL_API void DisableProfiling();

	//! Issue a query, returning a QueryResult. The QueryResult can be either a StreamQueryResult or a
	//! MaterializedQueryResult. The StreamQueryResult will only be returned in the case of a successful SELECT
	//! statement.
	SABOT_SQL_API unique_ptr<QueryResult> Query(const string &query, bool allow_stream_result);
	SABOT_SQL_API unique_ptr<QueryResult> Query(unique_ptr<SQLStatement> statement, bool allow_stream_result);

	//! Issues a query to the database and returns a Pending Query Result. Note that "query" may only contain
	//! a single statement.
	SABOT_SQL_API unique_ptr<PendingQueryResult> PendingQuery(const string &query, bool allow_stream_result);
	//! Issues a query to the database and returns a Pending Query Result
	SABOT_SQL_API unique_ptr<PendingQueryResult> PendingQuery(unique_ptr<SQLStatement> statement,
	                                                       bool allow_stream_result);

	//! Create a pending query with a list of parameters
	SABOT_SQL_API unique_ptr<PendingQueryResult> PendingQuery(unique_ptr<SQLStatement> statement,
	                                                       case_insensitive_map_t<BoundParameterData> &values,
	                                                       bool allow_stream_result);
	SABOT_SQL_API unique_ptr<PendingQueryResult>
	PendingQuery(const string &query, case_insensitive_map_t<BoundParameterData> &values, bool allow_stream_result);

	//! Destroy the client context
	SABOT_SQL_API void Destroy();

	//! Get the table info of a specific table, or nullptr if it cannot be found.
	SABOT_SQL_API unique_ptr<TableDescription> TableInfo(const string &database_name, const string &schema_name,
	                                                  const string &table_name);
	//! Get the table info of a specific table, or nullptr if it cannot be found. Uses INVALID_CATALOG.
	SABOT_SQL_API unique_ptr<TableDescription> TableInfo(const string &schema_name, const string &table_name);
	//! Execute a query with the given collection "attached" to the query using a CTE
	SABOT_SQL_API void Append(ColumnDataCollection &collection, const string &query, const vector<string> &column_names,
	                       const string &collection_name);
	//! Appends a DataChunk and its default columns to the specified table.
	SABOT_SQL_API void Append(TableDescription &description, ColumnDataCollection &collection,
	                       optional_ptr<const vector<LogicalIndex>> column_ids = nullptr);

	//! Try to bind a relation in the current client context; either throws an exception or fills the result_columns
	//! list with the set of returned columns
	SABOT_SQL_API void TryBindRelation(Relation &relation, vector<ColumnDefinition> &result_columns);

	//! Internal function for try bind relation. It does not require a client-context lock.
	SABOT_SQL_API void InternalTryBindRelation(Relation &relation, vector<ColumnDefinition> &result_columns);

	//! Execute a relation
	SABOT_SQL_API unique_ptr<PendingQueryResult> PendingQuery(const shared_ptr<Relation> &relation,
	                                                       bool allow_stream_result);
	SABOT_SQL_API unique_ptr<QueryResult> Execute(const shared_ptr<Relation> &relation);

	//! Prepare a query
	SABOT_SQL_API unique_ptr<PreparedStatement> Prepare(const string &query);
	//! Directly prepare a SQL statement
	SABOT_SQL_API unique_ptr<PreparedStatement> Prepare(unique_ptr<SQLStatement> statement);

	//! Create a pending query result from a prepared statement with the given name and set of parameters
	//! It is possible that the prepared statement will be re-bound. This will generally happen if the catalog is
	//! modified in between the prepared statement being bound and the prepared statement being run.
	SABOT_SQL_API unique_ptr<PendingQueryResult> PendingQuery(const string &query,
	                                                       shared_ptr<PreparedStatementData> &prepared,
	                                                       const PendingQueryParameters &parameters);

	//! Execute a prepared statement with the given name and set of parameters
	//! It is possible that the prepared statement will be re-bound. This will generally happen if the catalog is
	//! modified in between the prepared statement being bound and the prepared statement being run.
	SABOT_SQL_API unique_ptr<QueryResult> Execute(const string &query, shared_ptr<PreparedStatementData> &prepared,
	                                           case_insensitive_map_t<BoundParameterData> &values,
	                                           bool allow_stream_result = true);
	SABOT_SQL_API unique_ptr<QueryResult> Execute(const string &query, shared_ptr<PreparedStatementData> &prepared,
	                                           const PendingQueryParameters &parameters);

	//! Gets current percentage of the query's progress, returns 0 in case the progress bar is disabled.
	SABOT_SQL_API QueryProgress GetQueryProgress();

	//! Register function in the temporary schema
	SABOT_SQL_API void RegisterFunction(CreateFunctionInfo &info);

	//! Parse statements from a query
	SABOT_SQL_API vector<unique_ptr<SQLStatement>> ParseStatements(const string &query);

	//! Extract the logical plan of a query
	SABOT_SQL_API unique_ptr<LogicalOperator> ExtractPlan(const string &query);
	SABOT_SQL_API void HandlePragmaStatements(vector<unique_ptr<SQLStatement>> &statements);

	//! Runs a function with a valid transaction context, potentially starting a transaction if the context is in auto
	//! commit mode.
	SABOT_SQL_API void RunFunctionInTransaction(const std::function<void(void)> &fun,
	                                         bool requires_valid_transaction = true);
	//! Same as RunFunctionInTransaction, but does not obtain a lock on the client context or check for validation
	SABOT_SQL_API void RunFunctionInTransactionInternal(ClientContextLock &lock, const std::function<void(void)> &fun,
	                                                 bool requires_valid_transaction = true);

	//! Equivalent to CURRENT_SETTING(key) SQL function.
	SABOT_SQL_API SettingLookupResult TryGetCurrentSetting(const string &key, Value &result) const;

	//! Returns the parser options for this client context
	SABOT_SQL_API ParserOptions GetParserOptions() const;

	//! Whether or not the given result object (streaming query result or pending query result) is active
	SABOT_SQL_API bool IsActiveResult(ClientContextLock &lock, BaseQueryResult &result);

	//! Returns the current executor
	Executor &GetExecutor();

	//! Return the current logger
	Logger &GetLogger() const;

	//! Returns the current query string (if any)
	const string &GetCurrentQuery();

	connection_t GetConnectionId() const;

	//! Fetch the set of tables names of the query.
	//! Returns the fully qualified, escaped table names, if qualified is set to true,
	//! else returns the not qualified, not escaped table names.
	SABOT_SQL_API unordered_set<string> GetTableNames(const string &query, const bool qualified = false);

	SABOT_SQL_API ClientProperties GetClientProperties();

	//! Returns true if execution of the current query is finished
	SABOT_SQL_API bool ExecutionIsFinished();

	//! Process an error for display to the user
	SABOT_SQL_API void ProcessError(ErrorData &error, const string &query) const;

private:
	//! Parse statements and resolve pragmas from a query
	vector<unique_ptr<SQLStatement>> ParseStatements(ClientContextLock &lock, const string &query);
	//! Issues a query to the database and returns a Pending Query Result
	unique_ptr<PendingQueryResult> PendingQueryInternal(ClientContextLock &lock, unique_ptr<SQLStatement> statement,
	                                                    const PendingQueryParameters &parameters, bool verify = true);
	unique_ptr<QueryResult> ExecutePendingQueryInternal(ClientContextLock &lock, PendingQueryResult &query);

	//! Parse statements from a query
	vector<unique_ptr<SQLStatement>> ParseStatementsInternal(ClientContextLock &lock, const string &query);
	//! Perform aggressive query verification of a SELECT statement. Only called when query_verification_enabled is
	//! true.
	ErrorData VerifyQuery(ClientContextLock &lock, const string &query, unique_ptr<SQLStatement> statement,
	                      optional_ptr<case_insensitive_map_t<BoundParameterData>> values = nullptr);

	void InitialCleanup(ClientContextLock &lock);
	//! Internal clean up, does not lock. Caller must hold the context_lock.
	void CleanupInternal(ClientContextLock &lock, BaseQueryResult *result = nullptr,
	                     bool invalidate_transaction = false);
	unique_ptr<PendingQueryResult> PendingStatementOrPreparedStatement(ClientContextLock &lock, const string &query,
	                                                                   unique_ptr<SQLStatement> statement,
	                                                                   shared_ptr<PreparedStatementData> &prepared,
	                                                                   const PendingQueryParameters &parameters);
	unique_ptr<PendingQueryResult> PendingPreparedStatement(ClientContextLock &lock, const string &query,
	                                                        shared_ptr<PreparedStatementData> statement_p,
	                                                        const PendingQueryParameters &parameters);
	unique_ptr<PendingQueryResult> PendingPreparedStatementInternal(ClientContextLock &lock,
	                                                                shared_ptr<PreparedStatementData> statement_data_p,
	                                                                const PendingQueryParameters &parameters);
	void CheckIfPreparedStatementIsExecutable(PreparedStatementData &statement);

	//! Internally prepare a SQL statement. Caller must hold the context_lock.
	shared_ptr<PreparedStatementData>
	CreatePreparedStatement(ClientContextLock &lock, const string &query, unique_ptr<SQLStatement> statement,
	                        optional_ptr<case_insensitive_map_t<BoundParameterData>> values = nullptr,
	                        PreparedStatementMode mode = PreparedStatementMode::PREPARE_ONLY);
	unique_ptr<PendingQueryResult> PendingStatementInternal(ClientContextLock &lock, const string &query,
	                                                        unique_ptr<SQLStatement> statement,
	                                                        const PendingQueryParameters &parameters);
	unique_ptr<QueryResult> RunStatementInternal(ClientContextLock &lock, const string &query,
	                                             unique_ptr<SQLStatement> statement, bool allow_stream_result,
	                                             optional_ptr<case_insensitive_map_t<BoundParameterData>> params,
	                                             bool verify = true);
	unique_ptr<PreparedStatement> PrepareInternal(ClientContextLock &lock, unique_ptr<SQLStatement> statement);
	void LogQueryInternal(ClientContextLock &lock, const string &query);

	unique_ptr<QueryResult> FetchResultInternal(ClientContextLock &lock, PendingQueryResult &pending);

	unique_ptr<ClientContextLock> LockContext();

	void BeginQueryInternal(ClientContextLock &lock, const string &query);
	ErrorData EndQueryInternal(ClientContextLock &lock, bool success, bool invalidate_transaction,
	                           optional_ptr<ErrorData> previous_error);

	//! Wait until a task is available to execute
	void WaitForTask(ClientContextLock &lock, BaseQueryResult &result);
	PendingExecutionResult ExecuteTaskInternal(ClientContextLock &lock, BaseQueryResult &result, bool dry_run = false);

	unique_ptr<PendingQueryResult> PendingStatementOrPreparedStatementInternal(
	    ClientContextLock &lock, const string &query, unique_ptr<SQLStatement> statement,
	    shared_ptr<PreparedStatementData> &prepared, const PendingQueryParameters &parameters);

	unique_ptr<PendingQueryResult> PendingQueryPreparedInternal(ClientContextLock &lock, const string &query,
	                                                            shared_ptr<PreparedStatementData> &prepared,
	                                                            const PendingQueryParameters &parameters);

	unique_ptr<PendingQueryResult> PendingQueryInternal(ClientContextLock &, const shared_ptr<Relation> &relation,
	                                                    bool allow_stream_result);

	void RebindPreparedStatement(ClientContextLock &lock, const string &query,
	                             shared_ptr<PreparedStatementData> &prepared, const PendingQueryParameters &parameters);

	template <class T>
	unique_ptr<T> ErrorResult(ErrorData error, const string &query = string());

	shared_ptr<PreparedStatementData>
	CreatePreparedStatementInternal(ClientContextLock &lock, const string &query, unique_ptr<SQLStatement> statement,
	                                optional_ptr<case_insensitive_map_t<BoundParameterData>> values);

	SettingLookupResult TryGetCurrentSettingInternal(const string &key, Value &result) const;

private:
	//! Lock on using the ClientContext in parallel
	mutex context_lock;
	//! The currently active query context
	unique_ptr<ActiveQueryContext> active_query;
	//! The current query progress
	QueryProgress query_progress;
	//! The connection corresponding to this client context
	connection_t connection_id;
};

class ClientContextLock {
public:
	explicit ClientContextLock(mutex &context_lock) : client_guard(context_lock) {
	}

	~ClientContextLock() {
	}

private:
	lock_guard<mutex> client_guard;
};

//! The QueryContext wraps an optional client context.
//! It makes query-related information available to operations.
class QueryContext {
public:
	QueryContext() : context(nullptr) {
	}
	QueryContext(optional_ptr<ClientContext> context) : context(context) { // NOLINT: allow implicit construction
	}
	QueryContext(ClientContext &context) : context(&context) { // NOLINT: allow implicit construction
	}
	QueryContext(weak_ptr<ClientContext> context) // NOLINT: allow implicit construction
	    : owning_context(context.lock()), context(owning_context.get()) {
	}

public:
	bool Valid() const {
		return context != nullptr;
	}
	optional_ptr<ClientContext> GetClientContext() const {
		return context;
	}

private:
	shared_ptr<ClientContext> owning_context;
	optional_ptr<ClientContext> context;
};

} // namespace sabot_sql
