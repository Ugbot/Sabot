//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/main/database.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/common/winapi.hpp"
#include "sabot_sql/main/capi/extension_api.hpp"
#include "sabot_sql/main/config.hpp"
#include "sabot_sql/main/extension.hpp"
#include "sabot_sql/main/valid_checker.hpp"
#include "sabot_sql/main/extension/extension_loader.hpp"
#include "sabot_sql/main/extension_manager.hpp"

namespace sabot_sql {
class BufferManager;
class DatabaseManager;
class StorageManager;
class Catalog;
class TransactionManager;
class ConnectionManager;
class ExtensionManager;
class FileSystem;
class TaskScheduler;
class ObjectCache;
struct AttachInfo;
struct AttachOptions;
class DatabaseFileSystem;
struct DatabaseCacheEntry;
class LogManager;
class ExternalFileCache;

class DatabaseInstance : public enable_shared_from_this<DatabaseInstance> {
	friend class SabotSQL;

public:
	SABOT_SQL_API DatabaseInstance();
	SABOT_SQL_API ~DatabaseInstance();

	DBConfig config;

public:
	BufferPool &GetBufferPool() const;
	SABOT_SQL_API SecretManager &GetSecretManager();
	SABOT_SQL_API BufferManager &GetBufferManager();
	SABOT_SQL_API const BufferManager &GetBufferManager() const;
	SABOT_SQL_API DatabaseManager &GetDatabaseManager();
	SABOT_SQL_API FileSystem &GetFileSystem();
	SABOT_SQL_API ExternalFileCache &GetExternalFileCache();
	SABOT_SQL_API TaskScheduler &GetScheduler();
	SABOT_SQL_API ObjectCache &GetObjectCache();
	SABOT_SQL_API ConnectionManager &GetConnectionManager();
	SABOT_SQL_API ExtensionManager &GetExtensionManager();
	SABOT_SQL_API ValidChecker &GetValidChecker();
	SABOT_SQL_API LogManager &GetLogManager() const;

	SABOT_SQL_API const sabot_sql_ext_api_v1 GetExtensionAPIV1();

	idx_t NumberOfThreads();

	SABOT_SQL_API static DatabaseInstance &GetDatabase(ClientContext &context);
	SABOT_SQL_API static const DatabaseInstance &GetDatabase(const ClientContext &context);

	SABOT_SQL_API bool ExtensionIsLoaded(const string &name);

	SABOT_SQL_API SettingLookupResult TryGetCurrentSetting(const string &key, Value &result) const;

	SABOT_SQL_API shared_ptr<EncryptionUtil> GetEncryptionUtil() const;

	shared_ptr<AttachedDatabase> CreateAttachedDatabase(ClientContext &context, AttachInfo &info,
	                                                    AttachOptions &options);

private:
	void Initialize(const char *path, DBConfig *config);
	void LoadExtensionSettings();
	void CreateMainDatabase();

	void Configure(DBConfig &config, const char *path);

private:
	shared_ptr<BufferManager> buffer_manager;
	unique_ptr<DatabaseManager> db_manager;
	unique_ptr<TaskScheduler> scheduler;
	unique_ptr<ObjectCache> object_cache;
	unique_ptr<ConnectionManager> connection_manager;
	unique_ptr<ExtensionManager> extension_manager;
	ValidChecker db_validity;
	unique_ptr<DatabaseFileSystem> db_file_system;
	shared_ptr<LogManager> log_manager;
	unique_ptr<ExternalFileCache> external_file_cache;

	sabot_sql_ext_api_v1 (*create_api_v1)();
};

//! The database object. This object holds the catalog and all the
//! database-specific meta information.
class SabotSQL {
public:
	SABOT_SQL_API explicit SabotSQL(const char *path = nullptr, DBConfig *config = nullptr);
	SABOT_SQL_API explicit SabotSQL(const string &path, DBConfig *config = nullptr);
	SABOT_SQL_API explicit SabotSQL(DatabaseInstance &instance);

	SABOT_SQL_API ~SabotSQL();

	//! Reference to the actual database instance
	shared_ptr<DatabaseInstance> instance;

public:
	// Load a statically loaded extension by its class
	template <class T>
	void LoadStaticExtension() {
		T extension;
		auto &manager = ExtensionManager::Get(*instance);
		auto load_info = manager.BeginLoad(extension.Name());
		if (!load_info) {
			// already loaded - return
			return;
		}

		// Instantiate a new loader
		ExtensionLoader loader(*load_info);

		// Call the Load method of the extension
		extension.Load(loader);

		// Finalize the loading process
		loader.FinalizeLoad();

		ExtensionInstallInfo install_info;
		install_info.mode = ExtensionInstallMode::STATICALLY_LINKED;
		install_info.version = extension.Version();
		load_info->FinishLoad(install_info);
	}

	SABOT_SQL_API FileSystem &GetFileSystem();

	SABOT_SQL_API idx_t NumberOfThreads();
	SABOT_SQL_API static const char *SourceID();
	SABOT_SQL_API static const char *LibraryVersion();
	SABOT_SQL_API static const char *ReleaseCodename();
	SABOT_SQL_API static idx_t StandardVectorSize();
	SABOT_SQL_API static string Platform();
	SABOT_SQL_API bool ExtensionIsLoaded(const string &name);
};

} // namespace sabot_sql
