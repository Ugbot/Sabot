#include "sabot_sql/common/arrow/arrow_appender.hpp"
#include "sabot_sql/common/arrow/appender/null_data.hpp"

namespace sabot_sql {

void ArrowNullData::Initialize(ArrowAppendData &result, const LogicalType &type, idx_t capacity) {
	// nop
}

void ArrowNullData::Append(ArrowAppendData &append_data, Vector &input, idx_t from, idx_t to, idx_t input_size) {
	idx_t size = to - from;
	append_data.row_count += size;
}

void ArrowNullData::Finalize(ArrowAppendData &append_data, const LogicalType &type, ArrowArray *result) {
	result->n_buffers = 0;
}

} // namespace sabot_sql
