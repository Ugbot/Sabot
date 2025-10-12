//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/main/db_instance_cache.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/main/connection_manager.hpp"
#include "sabot_sql/main/database.hpp"
#include "sabot_sql/common/unordered_map.hpp"
#include <functional>

namespace sabot_sql {
class DBInstanceCache;
class DatabaseFilePathManager;

struct DatabaseCacheEntry {
	DatabaseCacheEntry();
	explicit DatabaseCacheEntry(const shared_ptr<SabotSQL> &database);
	~DatabaseCacheEntry();

	weak_ptr<SabotSQL> database;
	mutex update_database_mutex;
};

class DBInstanceCache {
public:
	DBInstanceCache();
	~DBInstanceCache();

	//! Gets a DB Instance from the cache if already exists (Fails if the configurations do not match)
	shared_ptr<SabotSQL> GetInstance(const string &database, const DBConfig &config_dict);

	//! Creates and caches a new DB Instance (Fails if a cached instance already exists)
	shared_ptr<SabotSQL> CreateInstance(const string &database, DBConfig &config_dict, bool cache_instance = true,
	                                  const std::function<void(SabotSQL &)> &on_create = nullptr);

	//! Either returns an existing entry, or creates and caches a new DB Instance
	shared_ptr<SabotSQL> GetOrCreateInstance(const string &database, DBConfig &config_dict, bool cache_instance,
	                                       const std::function<void(SabotSQL &)> &on_create = nullptr);

private:
	shared_ptr<DatabaseFilePathManager> path_manager;
	//! A map with the cached instances <absolute_path/instance>
	unordered_map<string, weak_ptr<DatabaseCacheEntry>> db_instances;

	//! Lock to alter cache
	mutex cache_lock;

private:
	shared_ptr<SabotSQL> GetInstanceInternal(const string &database, const DBConfig &config,
	                                       std::unique_lock<std::mutex> &db_instances_lock);
	shared_ptr<SabotSQL> CreateInstanceInternal(const string &database, DBConfig &config_dict, bool cache_instance,
	                                          std::unique_lock<std::mutex> db_instances_lock,
	                                          const std::function<void(SabotSQL &)> &on_create);
};
} // namespace sabot_sql
