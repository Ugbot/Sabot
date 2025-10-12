//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/common/assert.hpp
//
//
//===----------------------------------------------------------------------===//

#include "sabot_sql/common/winapi.hpp"

#pragma once

// clang-format off
#if ( \
    /* Not a debug build */ \
    !defined(DEBUG) && \
    /* FORCE_ASSERT is not set (enables assertions even on release mode when set to true) */ \
    !defined(SABOT_SQL_FORCE_ASSERT) && \
    /* The project is not compiled for Microsoft Visual Studio */ \
    !defined(__MVS__) \
)
// clang-format on

//! On most builds, NDEBUG is defined, turning the assert call into a NO-OP
//! Only the 'else' condition is supposed to check the assertions
#include <assert.h>
#define D_ASSERT assert
namespace sabot_sql {
SABOT_SQL_API void SabotSQLAssertInternal(bool condition, const char *condition_name, const char *file, int linenr);
}

#else
namespace sabot_sql {
SABOT_SQL_API void SabotSQLAssertInternal(bool condition, const char *condition_name, const char *file, int linenr);
}

#define D_ASSERT(condition) sabot_sql::SabotSQLAssertInternal(bool(condition), #condition, __FILE__, __LINE__)
#define D_ASSERT_IS_ENABLED

#endif
