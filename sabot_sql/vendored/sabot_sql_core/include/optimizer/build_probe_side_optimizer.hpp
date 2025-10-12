//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/optimizer/build_side_probe_side_optimizer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/common/unordered_set.hpp"
#include "sabot_sql/common/vector.hpp"
#include "sabot_sql/planner/logical_operator.hpp"

namespace sabot_sql {

struct BuildSize {
	double left_side;
	double right_side;

	// Initialize with 1 so the build side is just the cardinality if types aren't
	// known.
	BuildSize() : left_side(1), right_side(1) {
	}
};

class BuildProbeSideOptimizer : LogicalOperatorVisitor {
private:
	static constexpr idx_t COLUMN_COUNT_PENALTY = 2;
	static constexpr double PREFER_RIGHT_DEEP_PENALTY = 0.15;

public:
	explicit BuildProbeSideOptimizer(ClientContext &context, LogicalOperator &op);
	void VisitOperator(LogicalOperator &op) override;
	void VisitExpression(unique_ptr<Expression> *expression) override {};

private:
	bool TryFlipJoinChildren(LogicalOperator &op) const;
	static idx_t ChildHasJoins(LogicalOperator &op);

	static BuildSize GetBuildSizes(const LogicalOperator &op, idx_t lhs_cardinality, idx_t rhs_cardinality);
	static double GetBuildSize(vector<LogicalType> types, idx_t cardinality);

private:
	ClientContext &context;
	vector<ColumnBinding> preferred_on_probe_side;
};

} // namespace sabot_sql
