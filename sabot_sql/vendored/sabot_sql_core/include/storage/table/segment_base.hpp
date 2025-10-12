//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/storage/table/segment_base.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/common/constants.hpp"
#include "sabot_sql/common/atomic.hpp"

namespace sabot_sql {

template <class T>
class SegmentBase {
public:
	SegmentBase(idx_t start, idx_t count) : start(start), count(count), next(nullptr) {
	}
	T *Next() {
#ifndef SABOT_SQL_R_BUILD
		return next.load();
#else
		return next;
#endif
	}

	//! The start row id of this chunk
	idx_t start;
	//! The amount of entries in this storage chunk
	atomic<idx_t> count;
	//! The next segment after this one
#ifndef SABOT_SQL_R_BUILD
	atomic<T *> next;
#else
	T *next;
#endif
	//! The index within the segment tree
	idx_t index;
};

} // namespace sabot_sql
