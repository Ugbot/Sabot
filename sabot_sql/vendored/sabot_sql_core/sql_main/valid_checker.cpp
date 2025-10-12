#include "sabot_sql/main/valid_checker.hpp"

#include "sabot_sql/main/database.hpp"

namespace sabot_sql {

ValidChecker::ValidChecker(DatabaseInstance &db) : is_invalidated(false), db(db) {
}

void ValidChecker::Invalidate(string error) {
	lock_guard<mutex> l(invalidate_lock);
	is_invalidated = true;
	invalidated_msg = std::move(error);
}

bool ValidChecker::IsInvalidated() {
	if (db.config.options.disable_database_invalidation) {
		return false;
	}
	return is_invalidated;
}

string ValidChecker::InvalidatedMessage() {
	lock_guard<mutex> l(invalidate_lock);
	return invalidated_msg;
}

} // namespace sabot_sql
