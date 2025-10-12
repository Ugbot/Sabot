//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/storage/object_cache.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/common/common.hpp"
#include "sabot_sql/common/string.hpp"
#include "sabot_sql/common/unordered_map.hpp"
#include "sabot_sql/common/mutex.hpp"
#include "sabot_sql/main/client_context.hpp"
#include "sabot_sql/main/database.hpp"

namespace sabot_sql {

//! ObjectCache is the base class for objects caches in SabotSQL
class ObjectCacheEntry {
public:
	virtual ~ObjectCacheEntry() {
	}

	virtual string GetObjectType() = 0;
};

class ObjectCache {
public:
	shared_ptr<ObjectCacheEntry> GetObject(const string &key) {
		lock_guard<mutex> glock(lock);
		auto entry = cache.find(key);
		if (entry == cache.end()) {
			return nullptr;
		}
		return entry->second;
	}

	template <class T>
	shared_ptr<T> Get(const string &key) {
		shared_ptr<ObjectCacheEntry> object = GetObject(key);
		if (!object || object->GetObjectType() != T::ObjectType()) {
			return nullptr;
		}
		return shared_ptr_cast<ObjectCacheEntry, T>(object);
	}

	template <class T, class... ARGS>
	shared_ptr<T> GetOrCreate(const string &key, ARGS &&... args) {
		lock_guard<mutex> glock(lock);

		auto entry = cache.find(key);
		if (entry == cache.end()) {
			auto value = make_shared_ptr<T>(args...);
			cache[key] = value;
			return value;
		}
		auto object = entry->second;
		if (!object || object->GetObjectType() != T::ObjectType()) {
			return nullptr;
		}
		return shared_ptr_cast<ObjectCacheEntry, T>(object);
	}

	void Put(string key, shared_ptr<ObjectCacheEntry> value) {
		lock_guard<mutex> glock(lock);
		cache.insert(make_pair(std::move(key), std::move(value)));
	}

	void Delete(const string &key) {
		lock_guard<mutex> glock(lock);
		cache.erase(key);
	}

	SABOT_SQL_API static ObjectCache &GetObjectCache(ClientContext &context);

private:
	//! Object Cache
	unordered_map<string, shared_ptr<ObjectCacheEntry>> cache;
	mutex lock;
};

} // namespace sabot_sql
