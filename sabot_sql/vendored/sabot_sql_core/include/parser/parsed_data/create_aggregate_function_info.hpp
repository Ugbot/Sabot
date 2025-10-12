//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/parser/parsed_data/create_aggregate_function_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/parser/parsed_data/create_function_info.hpp"
#include "sabot_sql/function/function_set.hpp"

namespace sabot_sql {

struct CreateAggregateFunctionInfo : public CreateFunctionInfo {
	explicit CreateAggregateFunctionInfo(AggregateFunction function);
	explicit CreateAggregateFunctionInfo(AggregateFunctionSet set);

	AggregateFunctionSet functions;

public:
	unique_ptr<CreateInfo> Copy() const override;
};

} // namespace sabot_sql
