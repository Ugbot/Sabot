//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/common/types/string_heap.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/common/common.hpp"
#include "sabot_sql/common/types/value.hpp"
#include "sabot_sql/storage/arena_allocator.hpp"

namespace sabot_sql {
//! A string heap is the owner of a set of strings, strings can be inserted into
//! it On every insert, a pointer to the inserted string is returned The
//! returned pointer will remain valid until the StringHeap is destroyed
class StringHeap {
public:
	SABOT_SQL_API explicit StringHeap(Allocator &allocator = Allocator::DefaultAllocator());

	SABOT_SQL_API void Destroy();
	SABOT_SQL_API void Move(StringHeap &other);

	//! Add a string to the string heap, returns a pointer to the string
	SABOT_SQL_API string_t AddString(const char *data, idx_t len);
	//! Add a string to the string heap, returns a pointer to the string
	SABOT_SQL_API string_t AddString(const char *data);
	//! Add a string to the string heap, returns a pointer to the string
	SABOT_SQL_API string_t AddString(const string &data);
	//! Add a string to the string heap, returns a pointer to the string
	SABOT_SQL_API string_t AddString(const string_t &data);
	//! Add a blob to the string heap; blobs can be non-valid UTF8
	SABOT_SQL_API string_t AddBlob(const string_t &data);
	//! Add a blob to the string heap; blobs can be non-valid UTF8
	SABOT_SQL_API string_t AddBlob(const char *data, idx_t len);
	//! Allocates space for an empty string of size "len" on the heap
	SABOT_SQL_API string_t EmptyString(idx_t len);

	//! Size of strings
	SABOT_SQL_API idx_t SizeInBytes() const;
	//! Total allocation size (cached)
	SABOT_SQL_API idx_t AllocationSize() const;

	SABOT_SQL_API ArenaAllocator &GetAllocator() {
		return allocator;
	}

private:
	ArenaAllocator allocator;
};

} // namespace sabot_sql
