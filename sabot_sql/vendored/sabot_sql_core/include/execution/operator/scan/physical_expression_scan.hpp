//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/execution/operator/scan/physical_expression_scan.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/execution/physical_operator.hpp"
#include "sabot_sql/planner/expression.hpp"

namespace sabot_sql {

//! The PhysicalExpressionScan scans a set of expressions
class PhysicalExpressionScan : public PhysicalOperator {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::EXPRESSION_SCAN;

public:
	PhysicalExpressionScan(PhysicalPlan &physical_plan, vector<LogicalType> types,
	                       vector<vector<unique_ptr<Expression>>> expressions, idx_t estimated_cardinality)
	    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXPRESSION_SCAN, std::move(types),
	                       estimated_cardinality),
	      expressions(std::move(expressions)) {
	}

	//! The set of expressions to scan
	vector<vector<unique_ptr<Expression>>> expressions;

public:
	unique_ptr<OperatorState> GetOperatorState(ExecutionContext &context) const override;
	OperatorResultType Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
	                           GlobalOperatorState &gstate, OperatorState &state) const override;

	bool ParallelOperator() const override {
		return true;
	}

public:
	bool IsFoldable() const;
	void EvaluateExpression(ClientContext &context, idx_t expression_idx, optional_ptr<DataChunk> child_chunk,
	                        DataChunk &result, optional_ptr<DataChunk> temp_chunk_ptr = nullptr) const;

private:
	void EvaluateExpressionInternal(ClientContext &context, idx_t expression_idx, optional_ptr<DataChunk> child_chunk,
	                                DataChunk &result, DataChunk &temp_chunk) const;
};

} // namespace sabot_sql
