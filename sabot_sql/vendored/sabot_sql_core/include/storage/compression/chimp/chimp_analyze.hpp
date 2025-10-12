//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/storage/compression/chimp/chimp_analyze.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/storage/compression/chimp/chimp.hpp"
#include "sabot_sql/function/compression_function.hpp"

namespace sabot_sql {

struct EmptyChimpWriter;

template <class T>
struct ChimpAnalyzeState : public AnalyzeState {};

template <class T>
unique_ptr<AnalyzeState> ChimpInitAnalyze(ColumnData &col_data, PhysicalType type) {
	// This compression type is deprecated
	return nullptr;
}

template <class T>
bool ChimpAnalyze(AnalyzeState &state, Vector &input, idx_t count) {
	throw InternalException("Chimp has been deprecated, can no longer be used to compress data");
	return false;
}

template <class T>
idx_t ChimpFinalAnalyze(AnalyzeState &state) {
	throw InternalException("Chimp has been deprecated, can no longer be used to compress data");
}

} // namespace sabot_sql
