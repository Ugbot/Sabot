//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/execution/operator/set/physical_union.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/execution/physical_operator.hpp"

namespace sabot_sql {

class PhysicalUnion : public PhysicalOperator {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::UNION;

public:
	PhysicalUnion(PhysicalPlan &physical_plan, vector<LogicalType> types,
	              const ArenaLinkedList<reference<PhysicalOperator>> &children_p, idx_t estimated_cardinality,
	              bool allow_out_of_order);

	bool allow_out_of_order;

public:
	void BuildPipelines(Pipeline &current, MetaPipeline &meta_pipeline) override;
	vector<const_reference<PhysicalOperator>> GetSources() const override;
};

} // namespace sabot_sql
