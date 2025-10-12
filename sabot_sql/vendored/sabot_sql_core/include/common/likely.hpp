//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/common/likely.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#if __GNUC__
#define SABOT_SQL_BUILTIN_EXPECT(cond, expected_value) (__builtin_expect(cond, expected_value))
#else
#define SABOT_SQL_BUILTIN_EXPECT(cond, expected_value) (cond)
#endif

#define SABOT_SQL_LIKELY(...)   SABOT_SQL_BUILTIN_EXPECT((__VA_ARGS__), 1)
#define SABOT_SQL_UNLIKELY(...) SABOT_SQL_BUILTIN_EXPECT((__VA_ARGS__), 0)
