//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/parser/parsed_data/create_pragma_function_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/parser/parsed_data/create_function_info.hpp"
#include "sabot_sql/function/pragma_function.hpp"
#include "sabot_sql/function/function_set.hpp"

namespace sabot_sql {

struct CreatePragmaFunctionInfo : public CreateFunctionInfo {
	SABOT_SQL_API explicit CreatePragmaFunctionInfo(PragmaFunction function);
	SABOT_SQL_API CreatePragmaFunctionInfo(string name, PragmaFunctionSet functions);

	PragmaFunctionSet functions;

public:
	SABOT_SQL_API unique_ptr<CreateInfo> Copy() const override;
};

} // namespace sabot_sql
