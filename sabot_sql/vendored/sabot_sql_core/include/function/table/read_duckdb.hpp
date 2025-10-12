//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/function/table/read_sabot_sql.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/function/table_function.hpp"

namespace sabot_sql {
struct ReplacementScanInput;
struct ReplacementScanData;

struct ReadSabotSQLTableFunction {
	static TableFunction GetFunction();
	static unique_ptr<TableRef> ReplacementScan(ClientContext &context, ReplacementScanInput &input,
	                                            optional_ptr<ReplacementScanData> data);
};

} // namespace sabot_sql
