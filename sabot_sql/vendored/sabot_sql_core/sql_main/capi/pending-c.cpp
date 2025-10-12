#include "sabot_sql/main/capi/capi_internal.hpp"
#include "sabot_sql/main/query_result.hpp"
#include "sabot_sql/main/pending_query_result.hpp"
#include "sabot_sql/common/error_data.hpp"
#include "sabot_sql/common/case_insensitive_map.hpp"
#include "sabot_sql/common/optional_ptr.hpp"

using sabot_sql::case_insensitive_map_t;
using sabot_sql::make_uniq;
using sabot_sql::optional_ptr;
using sabot_sql::PendingExecutionResult;
using sabot_sql::PendingQueryResult;
using sabot_sql::PendingStatementWrapper;
using sabot_sql::PreparedStatementWrapper;
using sabot_sql::Value;

sabot_sql_state sabot_sql_pending_prepared_internal(sabot_sql_prepared_statement prepared_statement,
                                              sabot_sql_pending_result *out_result, bool allow_streaming) {
	if (!prepared_statement || !out_result) {
		return SabotSQLError;
	}
	auto wrapper = reinterpret_cast<PreparedStatementWrapper *>(prepared_statement);
	auto result = new PendingStatementWrapper();
	result->allow_streaming = allow_streaming;

	try {
		result->statement = wrapper->statement->PendingQuery(wrapper->values, allow_streaming);
	} catch (std::exception &ex) {
		result->statement = make_uniq<PendingQueryResult>(sabot_sql::ErrorData(ex));
	}
	sabot_sql_state return_value = !result->statement->HasError() ? SabotSQLSuccess : SabotSQLError;
	*out_result = reinterpret_cast<sabot_sql_pending_result>(result);

	return return_value;
}

sabot_sql_state sabot_sql_pending_prepared(sabot_sql_prepared_statement prepared_statement, sabot_sql_pending_result *out_result) {
	return sabot_sql_pending_prepared_internal(prepared_statement, out_result, false);
}

sabot_sql_state sabot_sql_pending_prepared_streaming(sabot_sql_prepared_statement prepared_statement,
                                               sabot_sql_pending_result *out_result) {
	return sabot_sql_pending_prepared_internal(prepared_statement, out_result, true);
}

void sabot_sql_destroy_pending(sabot_sql_pending_result *pending_result) {
	if (!pending_result || !*pending_result) {
		return;
	}
	auto wrapper = reinterpret_cast<PendingStatementWrapper *>(*pending_result);
	if (wrapper->statement) {
		wrapper->statement->Close();
	}
	delete wrapper;
	*pending_result = nullptr;
}

const char *sabot_sql_pending_error(sabot_sql_pending_result pending_result) {
	if (!pending_result) {
		return nullptr;
	}
	auto wrapper = reinterpret_cast<PendingStatementWrapper *>(pending_result);
	if (!wrapper->statement) {
		return nullptr;
	}
	return wrapper->statement->GetError().c_str();
}

sabot_sql_pending_state sabot_sql_pending_execute_check_state(sabot_sql_pending_result pending_result) {
	if (!pending_result) {
		return SABOT_SQL_PENDING_ERROR;
	}
	auto wrapper = reinterpret_cast<PendingStatementWrapper *>(pending_result);
	if (!wrapper->statement) {
		return SABOT_SQL_PENDING_ERROR;
	}
	if (wrapper->statement->HasError()) {
		return SABOT_SQL_PENDING_ERROR;
	}
	PendingExecutionResult return_value;
	try {
		return_value = wrapper->statement->CheckPulse();
	} catch (std::exception &ex) {
		wrapper->statement->SetError(sabot_sql::ErrorData(ex));
		return SABOT_SQL_PENDING_ERROR;
	}
	switch (return_value) {
	case PendingExecutionResult::BLOCKED:
	case PendingExecutionResult::RESULT_READY:
		return SABOT_SQL_PENDING_RESULT_READY;
	case PendingExecutionResult::NO_TASKS_AVAILABLE:
		return SABOT_SQL_PENDING_NO_TASKS_AVAILABLE;
	case PendingExecutionResult::RESULT_NOT_READY:
		return SABOT_SQL_PENDING_RESULT_NOT_READY;
	default:
		return SABOT_SQL_PENDING_ERROR;
	}
}

sabot_sql_pending_state sabot_sql_pending_execute_task(sabot_sql_pending_result pending_result) {
	if (!pending_result) {
		return SABOT_SQL_PENDING_ERROR;
	}
	auto wrapper = reinterpret_cast<PendingStatementWrapper *>(pending_result);
	if (!wrapper->statement) {
		return SABOT_SQL_PENDING_ERROR;
	}
	if (wrapper->statement->HasError()) {
		return SABOT_SQL_PENDING_ERROR;
	}
	PendingExecutionResult return_value;
	try {
		return_value = wrapper->statement->ExecuteTask();
	} catch (std::exception &ex) {
		wrapper->statement->SetError(sabot_sql::ErrorData(ex));
		return SABOT_SQL_PENDING_ERROR;
	}
	switch (return_value) {
	case PendingExecutionResult::EXECUTION_FINISHED:
	case PendingExecutionResult::RESULT_READY:
		return SABOT_SQL_PENDING_RESULT_READY;
	case PendingExecutionResult::BLOCKED:
	case PendingExecutionResult::NO_TASKS_AVAILABLE:
		return SABOT_SQL_PENDING_NO_TASKS_AVAILABLE;
	case PendingExecutionResult::RESULT_NOT_READY:
		return SABOT_SQL_PENDING_RESULT_NOT_READY;
	default:
		return SABOT_SQL_PENDING_ERROR;
	}
}

bool sabot_sql_pending_execution_is_finished(sabot_sql_pending_state pending_state) {
	switch (pending_state) {
	case SABOT_SQL_PENDING_RESULT_READY:
		return PendingQueryResult::IsResultReady(PendingExecutionResult::RESULT_READY);
	case SABOT_SQL_PENDING_NO_TASKS_AVAILABLE:
		return PendingQueryResult::IsResultReady(PendingExecutionResult::NO_TASKS_AVAILABLE);
	case SABOT_SQL_PENDING_RESULT_NOT_READY:
		return PendingQueryResult::IsResultReady(PendingExecutionResult::RESULT_NOT_READY);
	case SABOT_SQL_PENDING_ERROR:
		return PendingQueryResult::IsResultReady(PendingExecutionResult::EXECUTION_ERROR);
	default:
		return PendingQueryResult::IsResultReady(PendingExecutionResult::EXECUTION_ERROR);
	}
}

sabot_sql_state sabot_sql_execute_pending(sabot_sql_pending_result pending_result, sabot_sql_result *out_result) {
	if (!pending_result || !out_result) {
		return SabotSQLError;
	}
	memset(out_result, 0, sizeof(sabot_sql_result));
	auto wrapper = reinterpret_cast<PendingStatementWrapper *>(pending_result);
	if (!wrapper->statement) {
		return SabotSQLError;
	}

	sabot_sql::unique_ptr<sabot_sql::QueryResult> result;
	try {
		result = wrapper->statement->Execute();
	} catch (std::exception &ex) {
		sabot_sql::ErrorData error(ex);
		result = sabot_sql::make_uniq<sabot_sql::MaterializedQueryResult>(std::move(error));
	}

	wrapper->statement.reset();
	return SabotSQLTranslateResult(std::move(result), out_result);
}
