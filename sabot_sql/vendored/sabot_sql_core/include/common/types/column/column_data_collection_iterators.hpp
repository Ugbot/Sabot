//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/common/types/column/column_data_collection_iterators.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/common/types/column/column_data_scan_states.hpp"

namespace sabot_sql {
class ColumnDataCollection;

class ColumnDataChunkIterationHelper {
public:
	SABOT_SQL_API ColumnDataChunkIterationHelper(const ColumnDataCollection &collection, vector<column_t> column_ids);

private:
	const ColumnDataCollection &collection;
	vector<column_t> column_ids;

private:
	class ColumnDataChunkIterator;

	class ColumnDataChunkIterator {
	public:
		SABOT_SQL_API explicit ColumnDataChunkIterator(const ColumnDataCollection *collection_p,
		                                            vector<column_t> column_ids);

		const ColumnDataCollection *collection;
		ColumnDataScanState scan_state;
		shared_ptr<DataChunk> scan_chunk;
		idx_t row_index;

	public:
		SABOT_SQL_API void Next();

		SABOT_SQL_API ColumnDataChunkIterator &operator++();
		SABOT_SQL_API bool operator!=(const ColumnDataChunkIterator &other) const;
		SABOT_SQL_API DataChunk &operator*() const;
	};

public:
	ColumnDataChunkIterator begin() { // NOLINT: match stl API
		return ColumnDataChunkIterator(&collection, column_ids);
	}
	ColumnDataChunkIterator end() { // NOLINT: match stl API
		return ColumnDataChunkIterator(nullptr, vector<column_t>());
	}
};

class ColumnDataRowIterationHelper {
public:
	SABOT_SQL_API explicit ColumnDataRowIterationHelper(const ColumnDataCollection &collection);

private:
	const ColumnDataCollection &collection;

private:
	class ColumnDataRowIterator;

	class ColumnDataRowIterator {
	public:
		SABOT_SQL_API explicit ColumnDataRowIterator(const ColumnDataCollection *collection_p);

		const ColumnDataCollection *collection;
		ColumnDataScanState scan_state;
		shared_ptr<DataChunk> scan_chunk;
		ColumnDataRow current_row;

	public:
		void Next();

		SABOT_SQL_API ColumnDataRowIterator &operator++();
		SABOT_SQL_API bool operator!=(const ColumnDataRowIterator &other) const;
		SABOT_SQL_API const ColumnDataRow &operator*() const;
	};

public:
	SABOT_SQL_API ColumnDataRowIterator begin(); // NOLINT: match stl API
	SABOT_SQL_API ColumnDataRowIterator end();   // NOLINT: match stl API
};

} // namespace sabot_sql
