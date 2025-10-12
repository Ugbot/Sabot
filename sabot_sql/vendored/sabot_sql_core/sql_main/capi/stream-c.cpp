#include "sabot_sql/main/capi/capi_internal.hpp"
#include "sabot_sql/common/types/timestamp.hpp"
#include "sabot_sql/common/allocator.hpp"

sabot_sql_data_chunk sabot_sql_stream_fetch_chunk(sabot_sql_result result) {
	if (!result.internal_data) {
		return nullptr;
	}
	auto &result_data = *((sabot_sql::SabotSQLResultData *)result.internal_data);
	if (result_data.result->type != sabot_sql::QueryResultType::STREAM_RESULT) {
		// We can only fetch from a StreamQueryResult
		return nullptr;
	}
	return sabot_sql_fetch_chunk(result);
}

sabot_sql_data_chunk sabot_sql_fetch_chunk(sabot_sql_result result) {
	if (!result.internal_data) {
		return nullptr;
	}
	auto &result_data = *((sabot_sql::SabotSQLResultData *)result.internal_data);
	if (result_data.result_set_type == sabot_sql::CAPIResultSetType::CAPI_RESULT_TYPE_DEPRECATED) {
		return nullptr;
	}
	result_data.result_set_type = sabot_sql::CAPIResultSetType::CAPI_RESULT_TYPE_STREAMING;
	auto &result_instance = (sabot_sql::QueryResult &)*result_data.result;
	// FetchRaw ? Do we care about flattening them?
	try {
		auto chunk = result_instance.Fetch();
		return reinterpret_cast<sabot_sql_data_chunk>(chunk.release());
	} catch (std::exception &e) {
		return nullptr;
	}
}
