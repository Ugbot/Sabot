//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/storage/compression/patas/patas_fetch.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/storage/compression/patas/patas.hpp"
#include "sabot_sql/storage/compression/patas/patas_scan.hpp"

#include "sabot_sql/common/limits.hpp"
#include "sabot_sql/common/types/null_value.hpp"
#include "sabot_sql/function/compression/compression.hpp"
#include "sabot_sql/function/compression_function.hpp"
#include "sabot_sql/main/config.hpp"
#include "sabot_sql/storage/buffer_manager.hpp"

#include "sabot_sql/storage/table/column_data_checkpointer.hpp"
#include "sabot_sql/storage/table/column_segment.hpp"
#include "sabot_sql/common/operator/subtract.hpp"

namespace sabot_sql {

template <class T>
void PatasFetchRow(ColumnSegment &segment, ColumnFetchState &state, row_t row_id, Vector &result, idx_t result_idx) {
	using EXACT_TYPE = typename FloatingToExact<T>::TYPE;

	PatasScanState<T> scan_state(segment);
	scan_state.Skip(segment, UnsafeNumericCast<idx_t>(row_id));
	auto result_data = FlatVector::GetDataUnsafe<EXACT_TYPE>(result);
	result_data[result_idx] = (EXACT_TYPE)0;

	if (scan_state.GroupFinished() && scan_state.total_value_count < scan_state.count) {
		scan_state.LoadGroup(scan_state.group_state.values);
	}
	scan_state.group_state.Scan((uint8_t *)(result_data + result_idx), 1);
	scan_state.total_value_count++;
}

} // namespace sabot_sql
