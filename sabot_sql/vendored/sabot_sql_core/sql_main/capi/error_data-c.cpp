#include "sabot_sql/main/capi/capi_internal.hpp"

using sabot_sql::ErrorData;
using sabot_sql::ErrorDataWrapper;

sabot_sql_error_data sabot_sql_create_error_data(sabot_sql_error_type type, const char *message) {
	auto wrapper = new ErrorDataWrapper();
	wrapper->error_data = ErrorData(sabot_sql::ErrorTypeFromC(type), message);
	return reinterpret_cast<sabot_sql_error_data>(wrapper);
}

void sabot_sql_destroy_error_data(sabot_sql_error_data *error_data) {
	if (!error_data || !*error_data) {
		return;
	}
	auto wrapper = reinterpret_cast<ErrorDataWrapper *>(*error_data);
	delete wrapper;
	*error_data = nullptr;
}

sabot_sql_error_type sabot_sql_error_data_error_type(sabot_sql_error_data error_data) {
	if (!error_data) {
		return SABOT_SQL_ERROR_INVALID_TYPE;
	}

	auto *wrapper = reinterpret_cast<ErrorDataWrapper *>(error_data);
	return sabot_sql::ErrorTypeToC(wrapper->error_data.Type());
}

const char *sabot_sql_error_data_message(sabot_sql_error_data error_data) {
	if (!error_data) {
		return nullptr;
	}

	auto *wrapper = reinterpret_cast<ErrorDataWrapper *>(error_data);
	return wrapper->error_data.RawMessage().c_str();
}

bool sabot_sql_error_data_has_error(sabot_sql_error_data error_data) {
	if (!error_data) {
		return false;
	}

	auto *wrapper = reinterpret_cast<ErrorDataWrapper *>(error_data);
	return wrapper->error_data.HasError();
}
