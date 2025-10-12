//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/common/types/column/column_data_collection.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/common/pair.hpp"
#include "sabot_sql/common/types/column/column_data_collection_iterators.hpp"

namespace sabot_sql {
class BufferManager;
class BlockHandle;
class ClientContext;
struct ColumnDataCopyFunction;
class ColumnDataAllocator;
class ColumnDataCollection;
class ColumnDataCollectionSegment;
class ColumnDataRowCollection;

//! The ColumnDataCollection represents a set of (buffer-managed) data stored in columnar format
//! It is efficient to read and scan
class ColumnDataCollection {
public:
	//! Constructs an in-memory column data collection from an allocator
	SABOT_SQL_API ColumnDataCollection(Allocator &allocator, vector<LogicalType> types);
	//! Constructs an empty (but valid) in-memory column data collection from an allocator
	SABOT_SQL_API explicit ColumnDataCollection(Allocator &allocator);
	//! Constructs a buffer-managed column data collection
	SABOT_SQL_API ColumnDataCollection(BufferManager &buffer_manager, vector<LogicalType> types);
	//! Constructs either an in-memory or a buffer-managed column data collection
	SABOT_SQL_API ColumnDataCollection(ClientContext &context, vector<LogicalType> types,
	                                ColumnDataAllocatorType type = ColumnDataAllocatorType::BUFFER_MANAGER_ALLOCATOR);
	//! Creates a column data collection that inherits the blocks to write to. This allows blocks to be shared
	//! between multiple column data collections and prevents wasting space.
	//! Note that after one CDC inherits blocks from another, the other
	//! cannot be written to anymore (i.e. we take ownership of the half-written blocks).
	SABOT_SQL_API ColumnDataCollection(ColumnDataCollection &parent);
	SABOT_SQL_API ColumnDataCollection(shared_ptr<ColumnDataAllocator> allocator, vector<LogicalType> types);
	SABOT_SQL_API ~ColumnDataCollection();

public:
	//! The types of columns in the ColumnDataCollection
	vector<LogicalType> &Types() {
		return types;
	}
	const vector<LogicalType> &Types() const {
		return types;
	}

	//! The amount of rows in the ColumnDataCollection
	const idx_t &Count() const {
		return count;
	}

	//! The amount of columns in the ColumnDataCollection
	idx_t ColumnCount() const {
		return types.size();
	}

	//! The size (in bytes) of this ColumnDataCollection
	idx_t SizeInBytes() const;
	//! The allocation size (in bytes) of this ColumnDataCollection - this property is cached
	idx_t AllocationSize() const;
	//! Sets the partition index of this ColumnDataCollection
	void SetPartitionIndex(idx_t index);

	//! Get the allocator
	SABOT_SQL_API Allocator &GetAllocator() const;

	//! Initializes an Append state - useful for optimizing many appends made to the same column data collection
	SABOT_SQL_API void InitializeAppend(ColumnDataAppendState &state);
	//! Append a DataChunk to this ColumnDataCollection using the specified append state
	SABOT_SQL_API void Append(ColumnDataAppendState &state, DataChunk &new_chunk);

	//! Initializes a chunk with the correct types that can be used to call Scan
	SABOT_SQL_API void InitializeScanChunk(DataChunk &chunk) const;
	//! Initializes a chunk with the correct types for a given scan state
	SABOT_SQL_API void InitializeScanChunk(ColumnDataScanState &state, DataChunk &chunk) const;
	//! Initializes a Scan state for scanning all columns
	SABOT_SQL_API void
	InitializeScan(ColumnDataScanState &state,
	               ColumnDataScanProperties properties = ColumnDataScanProperties::ALLOW_ZERO_COPY) const;
	//! Initializes a Scan state for scanning a subset of the columns
	SABOT_SQL_API void
	InitializeScan(ColumnDataScanState &state, vector<column_t> column_ids,
	               ColumnDataScanProperties properties = ColumnDataScanProperties::ALLOW_ZERO_COPY) const;
	//! Initialize a parallel scan over the column data collection over all columns
	SABOT_SQL_API void
	InitializeScan(ColumnDataParallelScanState &state,
	               ColumnDataScanProperties properties = ColumnDataScanProperties::ALLOW_ZERO_COPY) const;
	//! Initialize a parallel scan over the column data collection over a subset of the columns
	SABOT_SQL_API void
	InitializeScan(ColumnDataParallelScanState &state, vector<column_t> column_ids,
	               ColumnDataScanProperties properties = ColumnDataScanProperties::ALLOW_ZERO_COPY) const;
	//! Scans a DataChunk from the ColumnDataCollection
	SABOT_SQL_API bool Scan(ColumnDataScanState &state, DataChunk &result) const;
	//! Scans a DataChunk from the ColumnDataCollection
	SABOT_SQL_API bool Scan(ColumnDataParallelScanState &state, ColumnDataLocalScanState &lstate, DataChunk &result) const;

	//! Append a DataChunk directly to this ColumnDataCollection - calls InitializeAppend and Append internally
	SABOT_SQL_API void Append(DataChunk &new_chunk);

	//! Appends the other ColumnDataCollection to this, destroying the other data collection
	SABOT_SQL_API void Combine(ColumnDataCollection &other);

	SABOT_SQL_API void Verify();

	SABOT_SQL_API string ToString() const;
	SABOT_SQL_API void Print() const;

	SABOT_SQL_API void Reset();

	//! Returns the number of data chunks present in the ColumnDataCollection
	SABOT_SQL_API idx_t ChunkCount() const;
	//! Fetch an individual chunk from the ColumnDataCollection
	SABOT_SQL_API void FetchChunk(idx_t chunk_idx, DataChunk &result) const;

	//! Constructs a class that can be iterated over to fetch individual chunks
	//! Iterating over this is syntactic sugar over just calling Scan
	SABOT_SQL_API ColumnDataChunkIterationHelper Chunks() const;
	//! Constructs a class that can be iterated over to fetch individual chunks
	//! Only the column indexes specified in the column_ids list are scanned
	SABOT_SQL_API ColumnDataChunkIterationHelper Chunks(vector<column_t> column_ids) const;

	//! Constructs a class that can be iterated over to fetch individual rows
	//! Note that row iteration is slow, and the `.Chunks()` method should be used instead
	SABOT_SQL_API ColumnDataRowIterationHelper Rows() const;

	//! Returns a materialized set of all of the rows in the column data collection
	//! Note that usage of this is slow - avoid using this unless the amount of rows is small, or if you do not care
	//! about performance
	SABOT_SQL_API ColumnDataRowCollection GetRows() const;

	//! Compare two column data collections to another. If they are equal according to result equality rules,
	//! return true. That means null values are equal, and approx equality is used for floating point values.
	//! If they are not equal, return false and fill in the error message.
	static bool ResultEquals(const ColumnDataCollection &left, const ColumnDataCollection &right, string &error_message,
	                         bool ordered = false);

	//! Obtains the next scan index to scan from
	bool NextScanIndex(ColumnDataScanState &state, idx_t &chunk_index, idx_t &segment_index, idx_t &row_index) const;
	//! Obtains the previous scan index to scan from
	bool PrevScanIndex(ColumnDataScanState &state, idx_t &chunk_index, idx_t &segment_index, idx_t &row_index) const;
	//! Scans at the indices (obtained from NextScanIndex)
	void ScanAtIndex(ColumnDataParallelScanState &state, ColumnDataLocalScanState &lstate, DataChunk &result,
	                 idx_t chunk_index, idx_t segment_index, idx_t row_index) const;

	//! Seeks to the chunk _containing_ the row. Returns false if it is past the end.
	//! Note that the returned chunk will likely not be aligned to the given row
	//! but the scan state will provide the actual range
	bool Seek(idx_t row_idx, ColumnDataScanState &state, DataChunk &result) const;

	//! Initialize the column data collection
	void Initialize(vector<LogicalType> types);

	//! Get references to the string heaps in this ColumnDataCollection
	vector<shared_ptr<StringHeap>> GetHeapReferences();
	//! Get the allocator type of this ColumnDataCollection
	ColumnDataAllocatorType GetAllocatorType() const;

	//! Get a vector of the segments in this ColumnDataCollection
	const vector<unique_ptr<ColumnDataCollectionSegment>> &GetSegments() const;

	void Serialize(Serializer &serializer) const;
	static unique_ptr<ColumnDataCollection> Deserialize(Deserializer &deserializer);

private:
	//! Creates a new segment within the ColumnDataCollection
	void CreateSegment();

	static ColumnDataCopyFunction GetCopyFunction(const LogicalType &type);

private:
	//! The Column Data Allocator
	buffer_ptr<ColumnDataAllocator> allocator;
	//! The types of the stored entries
	vector<LogicalType> types;
	//! The number of entries stored in the column data collection
	idx_t count;
	//! The data segments of the column data collection
	vector<unique_ptr<ColumnDataCollectionSegment>> segments;
	//! The set of copy functions
	vector<ColumnDataCopyFunction> copy_functions;
	//! When the column data collection is marked as finished - new tuples can no longer be appended to it
	bool finished_append;
	//! Partition index (optional, if partitioned)
	optional_idx partition_index;
};

//! The ColumnDataRowCollection represents a set of materialized rows, as obtained from the ColumnDataCollection
class ColumnDataRowCollection {
public:
	SABOT_SQL_API explicit ColumnDataRowCollection(const ColumnDataCollection &collection);

public:
	SABOT_SQL_API Value GetValue(idx_t column, idx_t index) const;

public:
	// container API
	bool empty() const {     // NOLINT: match stl API
		return rows.empty(); // NOLINT
	}
	idx_t size() const { // NOLINT: match stl API
		return rows.size();
	}

	SABOT_SQL_API ColumnDataRow &operator[](idx_t i);
	SABOT_SQL_API const ColumnDataRow &operator[](idx_t i) const;

	vector<ColumnDataRow>::iterator begin() { // NOLINT: match stl API
		return rows.begin();
	}
	vector<ColumnDataRow>::iterator end() { // NOLINT: match stl API
		return rows.end();
	}
	vector<ColumnDataRow>::const_iterator cbegin() const { // NOLINT: match stl API
		return rows.cbegin();
	}
	vector<ColumnDataRow>::const_iterator cend() const { // NOLINT: match stl API
		return rows.cend();
	}
	vector<ColumnDataRow>::const_iterator begin() const { // NOLINT: match stl API
		return rows.begin();
	}
	vector<ColumnDataRow>::const_iterator end() const { // NOLINT: match stl API
		return rows.end();
	}

private:
	vector<ColumnDataRow> rows;
	vector<unique_ptr<DataChunk>> chunks;
	ColumnDataScanState scan_state;
};

} // namespace sabot_sql
