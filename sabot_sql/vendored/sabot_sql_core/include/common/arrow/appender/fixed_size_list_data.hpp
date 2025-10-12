//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/common/arrow/appender/fixed_size_list_data.hpp
//
//
//===----------------------------------------------------------------------===/

#pragma once

#include "sabot_sql/common/arrow/appender/append_data.hpp"

namespace sabot_sql {

struct ArrowFixedSizeListData {
public:
	static void Initialize(ArrowAppendData &result, const LogicalType &type, idx_t capacity);
	static void Append(ArrowAppendData &append_data, Vector &input, idx_t from, idx_t to, idx_t input_size);
	static void Finalize(ArrowAppendData &append_data, const LogicalType &type, ArrowArray *result);
};

} // namespace sabot_sql
