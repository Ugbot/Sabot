//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/parser/parsed_data/create_scalar_function_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/parser/parsed_data/create_function_info.hpp"
#include "sabot_sql/function/scalar_function.hpp"
#include "sabot_sql/function/function_set.hpp"

namespace sabot_sql {

struct CreateScalarFunctionInfo : public CreateFunctionInfo {
	SABOT_SQL_API explicit CreateScalarFunctionInfo(ScalarFunction function);
	SABOT_SQL_API explicit CreateScalarFunctionInfo(ScalarFunctionSet set);

	ScalarFunctionSet functions;

public:
	SABOT_SQL_API unique_ptr<CreateInfo> Copy() const override;
	SABOT_SQL_API unique_ptr<AlterInfo> GetAlterInfo() const override;
};

} // namespace sabot_sql
