//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/parser/parsed_data/create_table_function_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/parser/parsed_data/create_function_info.hpp"
#include "sabot_sql/function/function_set.hpp"

namespace sabot_sql {

struct CreateTableFunctionInfo : public CreateFunctionInfo {
	SABOT_SQL_API explicit CreateTableFunctionInfo(TableFunction function);
	SABOT_SQL_API explicit CreateTableFunctionInfo(TableFunctionSet set);

	//! The table functions
	TableFunctionSet functions;

public:
	SABOT_SQL_API unique_ptr<CreateInfo> Copy() const override;
	SABOT_SQL_API unique_ptr<AlterInfo> GetAlterInfo() const override;
};

} // namespace sabot_sql
