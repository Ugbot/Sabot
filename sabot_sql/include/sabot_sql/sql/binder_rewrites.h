#pragma once

#include <string>
#include <vector>
#include <regex>

namespace sabot_sql {
namespace sql {

struct RewriteInfo {
    bool has_flink_constructs = false;
    bool has_questdb_constructs = false;
    bool has_asof_join = false;
    bool has_windows = false;
    // Hints extracted during rewriting
    std::string window_interval; // e.g., "1h", "5m"
    std::vector<std::string> join_key_columns; // lhs.key==rhs.key
    std::string join_timestamp_column; // ts column used in ASOF condition
};

/**
 * Apply binder-level rewrites for extended SQL constructs into a base dialect
 * that the rest of the planning stack can recognize.
 */
std::string ApplyBinderRewrites(const std::string& sql, RewriteInfo& info);

} // namespace sql
} // namespace sabot_sql


