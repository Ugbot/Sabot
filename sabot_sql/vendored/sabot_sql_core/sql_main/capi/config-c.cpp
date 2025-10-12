#include "sabot_sql/main/capi/capi_internal.hpp"
#include "sabot_sql/main/config.hpp"
#include "sabot_sql/common/types/value.hpp"
#include "sabot_sql/main/extension_helper.hpp"

using sabot_sql::DBConfig;
using sabot_sql::Value;

// config
sabot_sql_state sabot_sql_create_config(sabot_sql_config *out_config) {
	if (!out_config) {
		return SabotSQLError;
	}
	try {
		*out_config = nullptr;
		auto config = new DBConfig();
		*out_config = reinterpret_cast<sabot_sql_config>(config);
		config->SetOptionByName("sabot_sql_api", "capi");
	} catch (...) { // LCOV_EXCL_START
		return SabotSQLError;
	} // LCOV_EXCL_STOP
	return SabotSQLSuccess;
}

size_t sabot_sql_config_count() {
	return DBConfig::GetOptionCount() + DBConfig::GetAliasCount() +
	       sabot_sql::ExtensionHelper::ArraySize(sabot_sql::EXTENSION_SETTINGS);
}

sabot_sql_state sabot_sql_get_config_flag(size_t index, const char **out_name, const char **out_description) {
	auto option = DBConfig::GetOptionByIndex(index);
	if (option) {
		if (out_name) {
			*out_name = option->name;
		}
		if (out_description) {
			*out_description = option->description;
		}
		return SabotSQLSuccess;
	}
	// alias
	index -= DBConfig::GetOptionCount();
	auto alias = DBConfig::GetAliasByIndex(index);
	if (alias) {
		if (out_name) {
			*out_name = alias->alias;
		}
		option = DBConfig::GetOptionByIndex(alias->option_index);
		if (out_description) {
			*out_description = option->description;
		}
		return SabotSQLSuccess;
	}
	index -= DBConfig::GetAliasCount();

	// extension index
	auto entry = sabot_sql::ExtensionHelper::GetArrayEntry(sabot_sql::EXTENSION_SETTINGS, index);
	if (!entry) {
		return SabotSQLError;
	}
	if (out_name) {
		*out_name = entry->name;
	}
	if (out_description) {
		*out_description = entry->extension;
	}
	return SabotSQLSuccess;
}

sabot_sql_state sabot_sql_set_config(sabot_sql_config config, const char *name, const char *option) {
	if (!config || !name || !option) {
		return SabotSQLError;
	}

	try {
		auto db_config = (DBConfig *)config;
		db_config->SetOptionByName(name, Value(option));
	} catch (...) {
		return SabotSQLError;
	}
	return SabotSQLSuccess;
}

void sabot_sql_destroy_config(sabot_sql_config *config) {
	if (!config) {
		return;
	}
	if (*config) {
		auto db_config = (DBConfig *)*config;
		delete db_config;
		*config = nullptr;
	}
}
