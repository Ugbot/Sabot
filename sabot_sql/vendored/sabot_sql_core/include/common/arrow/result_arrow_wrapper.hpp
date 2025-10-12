//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/common/arrow/result_arrow_wrapper.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/main/query_result.hpp"
#include "sabot_sql/common/arrow/arrow_wrapper.hpp"
#include "sabot_sql/main/chunk_scan_state.hpp"
#include "sabot_sql/function/table/arrow/arrow_duck_schema.hpp"

namespace sabot_sql {
class ResultArrowArrayStreamWrapper {
public:
	explicit ResultArrowArrayStreamWrapper(unique_ptr<QueryResult> result, idx_t batch_size);

public:
	ArrowArrayStream stream;
	unique_ptr<QueryResult> result;
	ErrorData last_error;
	idx_t batch_size;
	vector<LogicalType> column_types;
	vector<string> column_names;
	unique_ptr<ChunkScanState> scan_state;
	unordered_map<idx_t, const shared_ptr<ArrowTypeExtensionData>> extension_types;

private:
	static int MyStreamGetSchema(struct ArrowArrayStream *stream, struct ArrowSchema *out);
	static int MyStreamGetNext(struct ArrowArrayStream *stream, struct ArrowArray *out);
	static void MyStreamRelease(struct ArrowArrayStream *stream);
	static const char *MyStreamGetLastError(struct ArrowArrayStream *stream);
};
} // namespace sabot_sql
