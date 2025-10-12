//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/function/scalar/generic_common.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/function/scalar_function.hpp"
#include "sabot_sql/function/function_set.hpp"
#include "sabot_sql/function/built_in_functions.hpp"
#include "sabot_sql/common/serializer/serializer.hpp"
#include "sabot_sql/common/serializer/deserializer.hpp"

namespace sabot_sql {
class BoundFunctionExpression;

struct ConstantOrNull {
	static unique_ptr<FunctionData> Bind(Value value);
	static bool IsConstantOrNull(BoundFunctionExpression &expr, const Value &val);
};

struct ExportAggregateFunctionBindData : public FunctionData {
	unique_ptr<BoundAggregateExpression> aggregate;
	explicit ExportAggregateFunctionBindData(unique_ptr<Expression> aggregate_p);
	unique_ptr<FunctionData> Copy() const override;
	bool Equals(const FunctionData &other_p) const override;
};

struct ExportAggregateFunction {
	static unique_ptr<BoundAggregateExpression> Bind(unique_ptr<BoundAggregateExpression> child_aggregate);
};

} // namespace sabot_sql
