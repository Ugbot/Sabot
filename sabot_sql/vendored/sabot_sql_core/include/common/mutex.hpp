//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/common/mutex.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#ifdef __MVS__
#include <time.h>
#endif
#include <mutex>

namespace sabot_sql {
using std::lock_guard;
using std::mutex;
using std::unique_lock;
} // namespace sabot_sql
