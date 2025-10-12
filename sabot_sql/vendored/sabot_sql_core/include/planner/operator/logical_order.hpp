//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/planner/operator/logical_order.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/planner/bound_query_node.hpp"
#include "sabot_sql/planner/expression/bound_reference_expression.hpp"
#include "sabot_sql/planner/logical_operator.hpp"
#include "sabot_sql/storage/statistics/base_statistics.hpp"

namespace sabot_sql {

//! LogicalOrder represents an ORDER BY clause, sorting the data
class LogicalOrder : public LogicalOperator {
public:
	static constexpr const LogicalOperatorType TYPE = LogicalOperatorType::LOGICAL_ORDER_BY;

public:
	explicit LogicalOrder(vector<BoundOrderByNode> orders);

	vector<BoundOrderByNode> orders;
	vector<idx_t> projection_map;

public:
	vector<ColumnBinding> GetColumnBindings() override;

	bool HasProjectionMap() const override {
		return !projection_map.empty();
	}

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<LogicalOperator> Deserialize(Deserializer &deserializer);

	InsertionOrderPreservingMap<string> ParamsToString() const override;

protected:
	void ResolveTypes() override;
};
} // namespace sabot_sql
