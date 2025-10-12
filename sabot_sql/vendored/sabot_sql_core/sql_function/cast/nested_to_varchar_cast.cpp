#include "sabot_sql/function/cast/nested_to_varchar_cast.hpp"

namespace sabot_sql {

const bool NestedToVarcharCast::LOOKUP_TABLE[256] = {
    false, false, false, false, false, false, false, false, false, false, false, false, false, false, false,
    false, false, false, false, false, false, false, false, false, false, false, false, false, false, false,
    false, false, false, false,
    true, // "
    false, false, false, false,
    true, // '
    true, // (
    true, // )
    false, false,
    true, // ,
    false, false, false, false, false, false, false, false, false, false, false, false, false,
    true, // :
    false, false,
    true, // =
    false, false, false, false, false, false, false, false, false, false, false, false, false, false, false,
    false, false, false, false, false, false, false, false, false, false, false, false, false, false,
    true, // [
    false,
    true, // ]
    false, false, false, false, false, false, false, false, false, false, false, false, false, false, false,
    false, false, false, false, false, false, false, false, false, false, false, false, false, false,
    true, // {
    false,
    true, // }
    false, false, false, false, false, false, false, false, false, false, false, false, false, false, false,
    false, false, false, false, false, false, false, false, false, false, false, false, false, false, false,
    false, false, false, false, false, false, false, false, false, false, false, false, false, false, false,
    false, false, false, false, false, false, false, false, false, false, false, false, false, false, false,
    false, false, false, false, false, false, false, false, false, false, false, false, false, false, false,
    false, false, false, false, false, false, false, false, false, false, false, false, false, false, false,
    false, false, false, false, false, false, false, false, false, false, false, false, false, false, false,
    false, false, false, false, false, false, false, false, false, false, false, false, false, false, false,
    false, false, false, false, false, false, false, false, false, false};

} // namespace sabot_sql
