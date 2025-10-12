//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/common/enums/merge_action_type.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/common/constants.hpp"

namespace sabot_sql {

enum class MergeActionType : uint8_t { MERGE_UPDATE, MERGE_DELETE, MERGE_INSERT, MERGE_DO_NOTHING, MERGE_ERROR };

enum class MergeActionCondition : uint8_t { WHEN_MATCHED, WHEN_NOT_MATCHED_BY_SOURCE, WHEN_NOT_MATCHED_BY_TARGET };

} // namespace sabot_sql
