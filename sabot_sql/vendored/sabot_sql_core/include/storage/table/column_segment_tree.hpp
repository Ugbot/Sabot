//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/storage/table/column_segment_tree.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/storage/table/segment_tree.hpp"
#include "sabot_sql/storage/table/column_segment.hpp"

namespace sabot_sql {

class ColumnSegmentTree : public SegmentTree<ColumnSegment> {};

} // namespace sabot_sql
