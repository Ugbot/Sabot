//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// common/adbc/single_batch_array_stream.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/common/arrow/arrow.hpp"
#include "sabot_sql/common/adbc/adbc.h"

namespace sabot_sql_adbc {

struct SingleBatchArrayStream {
	struct ArrowSchema schema;
	struct ArrowArray batch;
};

AdbcStatusCode BatchToArrayStream(struct ArrowArray *values, struct ArrowSchema *schema,
                                  struct ArrowArrayStream *stream, struct AdbcError *error);

} // namespace sabot_sql_adbc
