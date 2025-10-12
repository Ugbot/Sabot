#include "sabot_sql/sql/binder_rewrites.h"
#include <algorithm>

namespace sabot_sql {
namespace sql {

static inline std::string to_upper(std::string s) {
    std::transform(s.begin(), s.end(), s.begin(), [](unsigned char c){ return std::toupper(c); });
    return s;
}

std::string ApplyBinderRewrites(const std::string& sql, RewriteInfo& info) {
    std::string out = sql;
    const std::string upper = to_upper(sql);

    // Detect constructs
    info.has_flink_constructs = upper.find("TUMBLE(") != std::string::npos ||
                                upper.find("HOP(") != std::string::npos ||
                                upper.find("SESSION(") != std::string::npos ||
                                upper.find(" OVER (") != std::string::npos ||
                                upper.find("CURRENT_TIMESTAMP") != std::string::npos;

    info.has_questdb_constructs = upper.find("SAMPLE BY ") != std::string::npos ||
                                  upper.find("LATEST BY ") != std::string::npos ||
                                  upper.find("ASOF JOIN") != std::string::npos;

    info.has_asof_join = upper.find("ASOF JOIN") != std::string::npos;

    // Flink rewrites (minimal safe subset)
    if (info.has_flink_constructs) {
        // CURRENT_TIMESTAMP -> NOW()
        out = std::regex_replace(out, std::regex("CURRENT_TIMESTAMP", std::regex::icase), "NOW()");
    }

    // QuestDB rewrites (conservative)
    if (upper.find("SAMPLE BY ") != std::string::npos) {
        info.has_windows = true;
        // Extract interval and store as hint; also rewrite
        std::smatch m;
        if (std::regex_search(out, m, std::regex(R"(SAMPLE\s+BY\s+([^\s,\)]+))", std::regex::icase))) {
            if (m.size() > 1) {
                info.window_interval = m[1].str();
            }
        }
        out = std::regex_replace(out,
            std::regex(R"(SAMPLE\s+BY\s+([^\s,\)]+))", std::regex::icase),
            "GROUP BY DATE_TRUNC('$1', timestamp)");
    }

    if (upper.find("LATEST BY ") != std::string::npos) {
        // LATEST BY key -> ORDER BY key DESC LIMIT 1 (simplified)
        out = std::regex_replace(out,
            std::regex(R"(LATEST\s+BY\s+([^\s,\)]+))", std::regex::icase),
            "ORDER BY $1 DESC LIMIT 1");
    }

    if (info.has_asof_join) {
        // Try to extract simple ON clause hints: key equality and time inequality
        // Pattern: ON lhs.key = rhs.key AND lhs.ts <= rhs.ts  (order-insensitive around spaces)
        std::smatch on_match;
        // Capture four identifiers around '=' and '<=' operators
        const std::regex on_regex(
            R"(ON\s+([A-Za-z0-9_\.]+)\s*=\s*([A-Za-z0-9_\.]+)\s+AND\s+([A-Za-z0-9_\.]+)\s*<=\s*([A-Za-z0-9_\.]+))", std::regex::icase);
        if (std::regex_search(out, on_match, on_regex)) {
            if (on_match.size() >= 5) {
                // Join keys
                info.join_key_columns.clear();
                info.join_key_columns.push_back(on_match[1].str());
                info.join_key_columns.push_back(on_match[2].str());
                // Timestamp columns (choose the left expr as the probe ts by default)
                info.join_timestamp_column = on_match[3].str();
            }
        }
        // Normalize ASOF JOIN to LEFT JOIN so downstream parsing succeeds;
        // execution layer will use info.has_asof_join to select time-aware pipeline.
        out = std::regex_replace(out,
            std::regex(R"(ASOF\s+JOIN)", std::regex::icase),
            "LEFT JOIN");
    }

    return out;
}

} // namespace sql
} // namespace sabot_sql


