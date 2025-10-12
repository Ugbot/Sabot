//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/planner/operator/logical_pivot.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/common/unordered_map.hpp"
#include "sabot_sql/common/unordered_set.hpp"
#include "sabot_sql/planner/logical_operator.hpp"
#include "sabot_sql/parser/tableref/pivotref.hpp"
#include "sabot_sql/planner/tableref/bound_pivotref.hpp"

namespace sabot_sql {

class LogicalPivot : public LogicalOperator {
public:
	static constexpr const LogicalOperatorType TYPE = LogicalOperatorType::LOGICAL_PIVOT;

public:
	LogicalPivot(idx_t pivot_idx, unique_ptr<LogicalOperator> plan, BoundPivotInfo info);

	idx_t pivot_index;
	//! The bound pivot info
	BoundPivotInfo bound_pivot;

public:
	vector<ColumnBinding> GetColumnBindings() override;

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<LogicalOperator> Deserialize(Deserializer &deserializer);
	vector<idx_t> GetTableIndex() const override;
	string GetName() const override;

protected:
	void ResolveTypes() override;

private:
	LogicalPivot();
};
} // namespace sabot_sql
