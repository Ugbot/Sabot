//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/main/valid_checker.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/common/constants.hpp"
#include "sabot_sql/common/atomic.hpp"
#include "sabot_sql/common/mutex.hpp"

namespace sabot_sql {
class DatabaseInstance;
class MetaTransaction;

class ValidChecker {
public:
	explicit ValidChecker(DatabaseInstance &db);

	SABOT_SQL_API static ValidChecker &Get(DatabaseInstance &db);
	SABOT_SQL_API static ValidChecker &Get(MetaTransaction &transaction);

	SABOT_SQL_API void Invalidate(string error);
	SABOT_SQL_API bool IsInvalidated();
	SABOT_SQL_API string InvalidatedMessage();

	template <class T>
	static bool IsInvalidated(T &o) {
		return Get(o).IsInvalidated();
	}
	template <class T>
	static void Invalidate(T &o, string error) {
		Get(o).Invalidate(std::move(error));
	}

	template <class T>
	static string InvalidatedMessage(T &o) {
		return Get(o).InvalidatedMessage();
	}

private:
	mutex invalidate_lock;
	//! Set to true when encountering a fatal exception.
	atomic<bool> is_invalidated;
	//! The message invalidating the database instance.
	string invalidated_msg;
	//! The database instance.
	DatabaseInstance &db;
};

} // namespace sabot_sql
