#pragma once

namespace sabot_sql {
namespace sql {

enum class JoinType {
    INNER,
    LEFT,
    RIGHT,
    FULL,
    SEMI,
    ANTI,
    ASOF
};

enum class WindowFunctionType {
    TUMBLE,
    HOP,
    SESSION,
    GENERIC
};

} // namespace sql
} // namespace sabot_sql


