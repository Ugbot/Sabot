//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/common/types/vector_cache.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/common/types.hpp"
#include "sabot_sql/common/types/vector_buffer.hpp"
#include "sabot_sql/common/vector.hpp"

namespace sabot_sql {
class Allocator;
class Vector;

//! The VectorCache holds cached vector data.
//! It enables re-using the same memory for different vectors.
class VectorCache {
public:
	//! Instantiate an empty vector cache.
	SABOT_SQL_API VectorCache();
	//! Instantiate a vector cache with the given type and capacity.
	SABOT_SQL_API VectorCache(Allocator &allocator, const LogicalType &type, const idx_t capacity = STANDARD_VECTOR_SIZE);

public:
	buffer_ptr<VectorBuffer> buffer;

public:
	void ResetFromCache(Vector &result) const;
	const LogicalType &GetType() const;
};

} // namespace sabot_sql
