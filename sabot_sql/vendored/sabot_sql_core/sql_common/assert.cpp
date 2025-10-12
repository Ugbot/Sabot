#include "sabot_sql/common/assert.hpp"
#include "sabot_sql/common/exception.hpp"

namespace sabot_sql {

void SabotSQLAssertInternal(bool condition, const char *condition_name, const char *file, int linenr) {
#ifdef DISABLE_ASSERTIONS
	return;
#endif
	if (condition) {
		return;
	}
	throw InternalException("Assertion triggered in file \"%s\" on line %d: %s", file, linenr, condition_name);
}

} // namespace sabot_sql
