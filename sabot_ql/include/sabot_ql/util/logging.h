#pragma once

#include <iostream>
#include <cstdlib>

namespace sabot_ql {

// Runtime logging control via environment variable
inline bool IsDebugLoggingEnabled() {
    static bool initialized = false;
    static bool enabled = false;

    if (!initialized) {
        const char* env = std::getenv("SABOT_DEBUG");
        enabled = (env != nullptr && std::string(env) == "1");
        initialized = true;
    }

    return enabled;
}

} // namespace sabot_ql

// Compile-time logging macros
// Usage:
//   SABOT_LOG_PLANNER("Planning BGP with " << triple_count << " triples");
//   SABOT_LOG_EXECUTOR("Executing query");
//
// Control:
//   Compile-time: cmake -DSABOT_ENABLE_DEBUG_LOGGING=ON
//   Runtime: export SABOT_DEBUG=1

#ifdef SABOT_ENABLE_DEBUG_LOGGING

// When debug logging is enabled at compile-time, check runtime flag
#define SABOT_LOG(category, message) \
    do { \
        if (::sabot_ql::IsDebugLoggingEnabled()) { \
            std::cout << "[" << category << "] " << message << "\n" << std::flush; \
        } \
    } while (0)

#else

// When debug logging is disabled at compile-time, no-op (zero overhead)
#define SABOT_LOG(category, message) \
    do { } while (0)

#endif

// Category-specific logging macros
#define SABOT_LOG_PLANNER(message)  SABOT_LOG("PLANNER", message)
#define SABOT_LOG_EXECUTOR(message) SABOT_LOG("EXECUTOR", message)
#define SABOT_LOG_OPERATOR(message) SABOT_LOG("OPERATOR", message)
#define SABOT_LOG_SCAN(message)     SABOT_LOG("SCAN", message)
#define SABOT_LOG_RENAME(message)   SABOT_LOG("RENAME", message)
#define SABOT_LOG_PROJECT(message)  SABOT_LOG("PROJECT", message)
