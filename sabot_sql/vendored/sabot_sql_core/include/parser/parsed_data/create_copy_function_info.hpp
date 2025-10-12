//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/parser/parsed_data/create_copy_function_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/parser/parsed_data/create_info.hpp"
#include "sabot_sql/function/copy_function.hpp"

namespace sabot_sql {

struct CreateCopyFunctionInfo : public CreateInfo {
	SABOT_SQL_API explicit CreateCopyFunctionInfo(CopyFunction function);

	//! Function name
	string name;
	//! The table function
	CopyFunction function;

public:
	unique_ptr<CreateInfo> Copy() const override;
};

} // namespace sabot_sql
