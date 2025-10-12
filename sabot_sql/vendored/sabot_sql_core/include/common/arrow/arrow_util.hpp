//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/common/arrow/arrow_util.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once
#include "sabot_sql/common/arrow/arrow.hpp"
#include "sabot_sql/main/chunk_scan_state.hpp"
#include "sabot_sql/main/client_properties.hpp"
#include "sabot_sql/common/helper.hpp"
#include "sabot_sql/common/error_data.hpp"

namespace sabot_sql {

class QueryResult;
class DataChunk;
class ArrowTypeExtensionData;

class ArrowUtil {
public:
	static bool TryFetchChunk(ChunkScanState &scan_state, ClientProperties options, idx_t chunk_size, ArrowArray *out,
	                          idx_t &result_count, ErrorData &error,
	                          unordered_map<idx_t, const shared_ptr<ArrowTypeExtensionData>> extension_type_cast);
	static idx_t FetchChunk(ChunkScanState &scan_state, ClientProperties options, idx_t chunk_size, ArrowArray *out,
	                        const unordered_map<idx_t, const shared_ptr<ArrowTypeExtensionData>> &extension_type_cast);

private:
	static bool TryFetchNext(QueryResult &result, unique_ptr<DataChunk> &out, ErrorData &error);
};

} // namespace sabot_sql
