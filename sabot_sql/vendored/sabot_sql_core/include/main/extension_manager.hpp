//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/main/extension_manager.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/common/common.hpp"
#include "sabot_sql/main/extension_install_info.hpp"

namespace sabot_sql {
class ErrorData;

class ExtensionInfo {
public:
	ExtensionInfo();

	mutex lock;
	atomic<bool> is_loaded;
	unique_ptr<ExtensionInstallInfo> install_info;
	unique_ptr<ExtensionLoadedInfo> load_info;
};

class ExtensionActiveLoad {
public:
	ExtensionActiveLoad(DatabaseInstance &db, ExtensionInfo &info, string extension_name);

	DatabaseInstance &db;
	unique_lock<mutex> load_lock;
	ExtensionInfo &info;
	string extension_name;

public:
	void FinishLoad(ExtensionInstallInfo &install_info);
	void LoadFail(const ErrorData &error);
};

class ExtensionManager {
public:
	explicit ExtensionManager(DatabaseInstance &db);

	SABOT_SQL_API bool ExtensionIsLoaded(const string &name);
	SABOT_SQL_API vector<string> GetExtensions();
	SABOT_SQL_API optional_ptr<ExtensionInfo> GetExtensionInfo(const string &name);
	SABOT_SQL_API unique_ptr<ExtensionActiveLoad> BeginLoad(const string &extension);

	SABOT_SQL_API static ExtensionManager &Get(DatabaseInstance &db);
	SABOT_SQL_API static ExtensionManager &Get(ClientContext &context);

private:
	DatabaseInstance &db;
	mutex lock;
	unordered_map<string, unique_ptr<ExtensionInfo>> loaded_extensions_info;
};

} // namespace sabot_sql
