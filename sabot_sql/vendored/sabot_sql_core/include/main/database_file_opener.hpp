//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/main/database_file_opener.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/common/file_opener.hpp"
#include "sabot_sql/common/opener_file_system.hpp"
#include "sabot_sql/main/config.hpp"
#include "sabot_sql/main/database.hpp"
#include "sabot_sql/logging/log_manager.hpp"

namespace sabot_sql {
class DatabaseInstance;

class DatabaseFileOpener : public FileOpener {
public:
	explicit DatabaseFileOpener(DatabaseInstance &db_p) : db(db_p) {
	}

	Logger &GetLogger() const override {
		return Logger::Get(db);
	}

	SettingLookupResult TryGetCurrentSetting(const string &key, Value &result) override {
		return db.TryGetCurrentSetting(key, result);
	}

	optional_ptr<ClientContext> TryGetClientContext() override {
		return nullptr;
	}

	optional_ptr<DatabaseInstance> TryGetDatabase() override {
		return &db;
	}
	shared_ptr<HTTPUtil> &GetHTTPUtil() override {
		return TryGetDatabase()->config.http_util;
	}

private:
	DatabaseInstance &db;
};

class DatabaseFileSystem : public OpenerFileSystem {
public:
	explicit DatabaseFileSystem(DatabaseInstance &db_p) : db(db_p), database_opener(db_p) {
	}

	FileSystem &GetFileSystem() const override {
		auto &config = DBConfig::GetConfig(db);
		return *config.file_system;
	}
	optional_ptr<FileOpener> GetOpener() const override {
		return &database_opener;
	}

private:
	DatabaseInstance &db;
	mutable DatabaseFileOpener database_opener;
};

} // namespace sabot_sql
