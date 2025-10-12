//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/storage/compression/chimp/chimp_compress.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/storage/compression/chimp/chimp.hpp"
#include "sabot_sql/function/compression_function.hpp"
#include "sabot_sql/storage/compression/chimp/chimp_analyze.hpp"

#include "sabot_sql/common/helper.hpp"
#include "sabot_sql/common/limits.hpp"
#include "sabot_sql/common/types/null_value.hpp"
#include "sabot_sql/function/compression/compression.hpp"
#include "sabot_sql/main/config.hpp"
#include "sabot_sql/storage/buffer_manager.hpp"

#include "sabot_sql/storage/table/column_data_checkpointer.hpp"
#include "sabot_sql/storage/table/column_segment.hpp"
#include "sabot_sql/common/operator/subtract.hpp"

#include <functional>

namespace sabot_sql {

template <class T>
struct ChimpCompressionState : public CompressionState {};

// Compression Functions

template <class T>
unique_ptr<CompressionState> ChimpInitCompression(ColumnDataCheckpointData &checkpoint_data,
                                                  unique_ptr<AnalyzeState> state) {
	throw InternalException("Chimp has been deprecated, can no longer be used to compress data");
	return nullptr;
}

template <class T>
void ChimpCompress(CompressionState &state_p, Vector &scan_vector, idx_t count) {
	throw InternalException("Chimp has been deprecated, can no longer be used to compress data");
}

template <class T>
void ChimpFinalizeCompress(CompressionState &state_p) {
	throw InternalException("Chimp has been deprecated, can no longer be used to compress data");
}

} // namespace sabot_sql
