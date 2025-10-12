//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/common/types/data_chunk.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/common/allocator.hpp"
#include "sabot_sql/common/arrow/arrow_wrapper.hpp"
#include "sabot_sql/common/common.hpp"
#include "sabot_sql/common/types/vector.hpp"
#include "sabot_sql/common/winapi.hpp"

namespace sabot_sql {
class Allocator;
class ClientContext;
class ExecutionContext;
class VectorCache;
class Serializer;
class Deserializer;

//!  A Data Chunk represents a set of vectors.
/*!
    The data chunk class is the intermediate representation used by the
   execution engine of SabotSQL. It effectively represents a subset of a relation.
   It holds a set of vectors that all have the same length.

    DataChunk is initialized using the DataChunk::Initialize function by
   providing it with a vector of TypeIds for the Vector members. By default,
   this function will also allocate a chunk of memory in the DataChunk for the
   vectors and all the vectors will be referencing vectors to the data owned by
   the chunk. The reason for this behavior is that the underlying vectors can
   become referencing vectors to other chunks as well (i.e. in the case an
   operator does not alter the data, such as a Filter operator which only adds a
   selection vector).

    In addition to holding the data of the vectors, the DataChunk also owns the
   selection vector that underlying vectors can point to.
*/
class DataChunk {
public:
	//! Creates an empty DataChunk
	SABOT_SQL_API DataChunk();
	SABOT_SQL_API ~DataChunk();

	//! The vectors owned by the DataChunk.
	vector<Vector> data;

public:
	inline idx_t size() const { // NOLINT
		return count;
	}
	inline idx_t ColumnCount() const {
		return data.size();
	}
	inline void SetCardinality(idx_t count_p) {
		D_ASSERT(count_p <= capacity);
		this->count = count_p;
	}
	inline void SetCardinality(const DataChunk &other) {
		SetCardinality(other.size());
	}
	inline idx_t GetCapacity() const {
		return capacity;
	}
	inline void SetCapacity(idx_t capacity_p) {
		this->capacity = capacity_p;
	}
	inline void SetCapacity(const DataChunk &other) {
		SetCapacity(other.capacity);
	}

	SABOT_SQL_API Value GetValue(idx_t col_idx, idx_t index) const;
	SABOT_SQL_API void SetValue(idx_t col_idx, idx_t index, const Value &val);

	idx_t GetAllocationSize() const;

	//! Returns true if all vectors in the DataChunk are constant
	SABOT_SQL_API bool AllConstant() const;

	//! Set the DataChunk to reference another data chunk
	SABOT_SQL_API void Reference(DataChunk &chunk);
	//! Set the DataChunk to own the data of data chunk, destroying the other chunk in the process
	SABOT_SQL_API void Move(DataChunk &chunk);

	//! Initializes a DataChunk with the given types and without any vector data allocation.
	SABOT_SQL_API void InitializeEmpty(const vector<LogicalType> &types);

	//! Initializes a DataChunk with the given types. Then, if the corresponding boolean in the initialize-vector is
	//! true, it initializes the vector for that data type.
	SABOT_SQL_API void Initialize(ClientContext &context, const vector<LogicalType> &types,
	                           idx_t capacity = STANDARD_VECTOR_SIZE);
	SABOT_SQL_API void Initialize(Allocator &allocator, const vector<LogicalType> &types,
	                           idx_t capacity = STANDARD_VECTOR_SIZE);
	SABOT_SQL_API void Initialize(ClientContext &context, const vector<LogicalType> &types, const vector<bool> &initialize,
	                           idx_t capacity = STANDARD_VECTOR_SIZE);
	SABOT_SQL_API void Initialize(Allocator &allocator, const vector<LogicalType> &types, const vector<bool> &initialize,
	                           idx_t capacity = STANDARD_VECTOR_SIZE);

	//! Append the other DataChunk to this one. The column count and types of
	//! the two DataChunks have to match exactly. Throws an exception if there
	//! is not enough space in the chunk and resize is not allowed.
	SABOT_SQL_API void Append(const DataChunk &other, bool resize = false, SelectionVector *sel = nullptr,
	                       idx_t count = 0);

	//! Destroy all data and columns owned by this DataChunk
	SABOT_SQL_API void Destroy();

	//! Copies the data from this chunk to another chunk.
	SABOT_SQL_API void Copy(DataChunk &other, idx_t offset = 0) const;
	SABOT_SQL_API void Copy(DataChunk &other, const SelectionVector &sel, const idx_t source_count,
	                     const idx_t offset = 0) const;

	//! Splits the DataChunk in two
	SABOT_SQL_API void Split(DataChunk &other, idx_t split_idx);

	//! Fuses a DataChunk onto the right of this one, and destroys the other. Inverse of Split.
	SABOT_SQL_API void Fuse(DataChunk &other);

	//! Makes this DataChunk reference the specified columns in the other DataChunk
	SABOT_SQL_API void ReferenceColumns(DataChunk &other, const vector<column_t> &column_ids);

	//! Turn all the vectors from the chunk into flat vectors
	SABOT_SQL_API void Flatten();

	// FIXME: this is SABOT_SQL_API, might need conversion back to regular unique ptr?
	SABOT_SQL_API unsafe_unique_array<UnifiedVectorFormat> ToUnifiedFormat();

	SABOT_SQL_API void Slice(const SelectionVector &sel_vector, idx_t count);

	//! Slice all Vectors from other.data[i] to data[i + 'col_offset']
	//! Turning all Vectors into Dictionary Vectors, using 'sel'
	SABOT_SQL_API void Slice(const DataChunk &other, const SelectionVector &sel, idx_t count, idx_t col_offset = 0);

	//! Slice a DataChunk from "offset" to "offset + count"
	SABOT_SQL_API void Slice(idx_t offset, idx_t count);

	//! Resets the DataChunk to its state right after the DataChunk::Initialize
	//! function was called. This sets the count to 0, the capacity to initial_capacity and resets each member
	//! Vector to point back to the data owned by this DataChunk.
	SABOT_SQL_API void Reset();

	SABOT_SQL_API void Serialize(Serializer &serializer, bool compressed_serialization = true) const;
	SABOT_SQL_API void Deserialize(Deserializer &source);

	//! Hashes the DataChunk to the target vector
	SABOT_SQL_API void Hash(Vector &result);
	//! Hashes specific vectors of the DataChunk to the target vector
	SABOT_SQL_API void Hash(vector<idx_t> &column_ids, Vector &result);

	//! Returns a list of types of the vectors of this data chunk
	SABOT_SQL_API vector<LogicalType> GetTypes() const;

	//! Converts this DataChunk to a printable string representation
	SABOT_SQL_API string ToString() const;
	SABOT_SQL_API void Print() const;

	DataChunk(const DataChunk &) = delete;

	//! Verify that the DataChunk is in a consistent, not corrupt state. DEBUG
	//! FUNCTION ONLY!
	SABOT_SQL_API void Verify();

private:
	//! The amount of tuples stored in the data chunk
	idx_t count;
	//! The amount of tuples that can be stored in the data chunk
	idx_t capacity;
	//! The initial capacity of this chunk set during ::Initialize, used when resetting
	idx_t initial_capacity;
	//! Vector caches, used to store data when ::Initialize is called
	vector<VectorCache> vector_caches;
};
} // namespace sabot_sql
