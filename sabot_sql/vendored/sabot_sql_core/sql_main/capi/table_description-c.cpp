#include "sabot_sql/main/capi/capi_internal.hpp"
#include "sabot_sql/common/string_util.hpp"

using sabot_sql::Connection;
using sabot_sql::ErrorData;
using sabot_sql::TableDescription;
using sabot_sql::TableDescriptionWrapper;

sabot_sql_state sabot_sql_table_description_create(sabot_sql_connection connection, const char *schema, const char *table,
                                             sabot_sql_table_description *out) {
	return sabot_sql_table_description_create_ext(connection, INVALID_CATALOG, schema, table, out);
}

sabot_sql_state sabot_sql_table_description_create_ext(sabot_sql_connection connection, const char *catalog, const char *schema,
                                                 const char *table, sabot_sql_table_description *out) {
	Connection *conn = reinterpret_cast<Connection *>(connection);

	if (!out) {
		return SabotSQLError;
	}
	auto wrapper = new TableDescriptionWrapper();
	*out = reinterpret_cast<sabot_sql_table_description>(wrapper);

	if (!connection || !table) {
		return SabotSQLError;
	}
	if (catalog == nullptr) {
		catalog = INVALID_CATALOG;
	}
	if (schema == nullptr) {
		schema = DEFAULT_SCHEMA;
	}

	try {
		wrapper->description = conn->TableInfo(catalog, schema, table);
	} catch (std::exception &ex) {
		ErrorData error(ex);
		wrapper->error = error.RawMessage();
		return SabotSQLError;
	} catch (...) { // LCOV_EXCL_START
		wrapper->error = "Unknown Connection::TableInfo error";
		return SabotSQLError;
	} // LCOV_EXCL_STOP
	if (!wrapper->description) {
		wrapper->error = "No table with that schema+name could be located";
		return SabotSQLError;
	}
	return SabotSQLSuccess;
}

void sabot_sql_table_description_destroy(sabot_sql_table_description *table) {
	if (!table || !*table) {
		return;
	}
	auto wrapper = reinterpret_cast<TableDescriptionWrapper *>(*table);
	delete wrapper;
	*table = nullptr;
}

const char *sabot_sql_table_description_error(sabot_sql_table_description table) {
	if (!table) {
		return nullptr;
	}
	auto wrapper = reinterpret_cast<TableDescriptionWrapper *>(table);
	if (wrapper->error.empty()) {
		return nullptr;
	}
	return wrapper->error.c_str();
}

sabot_sql_state GetTableDescription(TableDescriptionWrapper *wrapper, idx_t index) {
	if (!wrapper) {
		return SabotSQLError;
	}
	auto &table = wrapper->description;
	if (index >= table->columns.size()) {
		wrapper->error = sabot_sql::StringUtil::Format("Column index %d is out of range, table only has %d columns", index,
		                                            table->columns.size());
		return SabotSQLError;
	}
	return SabotSQLSuccess;
}

sabot_sql_state sabot_sql_column_has_default(sabot_sql_table_description table_description, idx_t index, bool *out) {
	auto wrapper = reinterpret_cast<TableDescriptionWrapper *>(table_description);
	if (GetTableDescription(wrapper, index) == SabotSQLError) {
		return SabotSQLError;
	}
	if (!out) {
		wrapper->error = "Please provide a valid (non-null) 'out' variable";
		return SabotSQLError;
	}

	auto &table = wrapper->description;
	auto &column = table->columns[index];
	*out = column.HasDefaultValue();
	return SabotSQLSuccess;
}

char *sabot_sql_table_description_get_column_name(sabot_sql_table_description table_description, idx_t index) {
	auto wrapper = reinterpret_cast<TableDescriptionWrapper *>(table_description);
	if (GetTableDescription(wrapper, index) == SabotSQLError) {
		return nullptr;
	}

	auto &table = wrapper->description;
	auto &column = table->columns[index];

	auto name = column.GetName();
	auto result = reinterpret_cast<char *>(malloc(sizeof(char) * (name.size() + 1)));
	memcpy(result, name.c_str(), name.size());
	result[name.size()] = '\0';

	return result;
}
