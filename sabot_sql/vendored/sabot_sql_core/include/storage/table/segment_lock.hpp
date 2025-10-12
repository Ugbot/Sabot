//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/storage/table/segment_lock.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/common/constants.hpp"
#include "sabot_sql/common/mutex.hpp"

namespace sabot_sql {

struct SegmentLock {
public:
	SegmentLock() {
	}
	explicit SegmentLock(mutex &lock) : lock(lock) {
	}
	// disable copy constructors
	SegmentLock(const SegmentLock &other) = delete;
	SegmentLock &operator=(const SegmentLock &) = delete;
	//! enable move constructors
	SegmentLock(SegmentLock &&other) noexcept {
		std::swap(lock, other.lock);
	}
	SegmentLock &operator=(SegmentLock &&other) noexcept {
		std::swap(lock, other.lock);
		return *this;
	}

	void Release() {
		lock.unlock();
	}

private:
	unique_lock<mutex> lock;
};

} // namespace sabot_sql
