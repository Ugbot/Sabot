//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/planner/subquery/rewrite_cte_scan.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/planner/binder.hpp"
#include "sabot_sql/planner/column_binding_map.hpp"
#include "sabot_sql/planner/logical_operator.hpp"

namespace sabot_sql {

//! Helper class to rewrite correlated cte scans within a single LogicalOperator
class RewriteCTEScan : public LogicalOperatorVisitor {
public:
	RewriteCTEScan(idx_t table_index, const CorrelatedColumns &correlated_columns);

	void VisitOperator(LogicalOperator &op) override;

private:
	idx_t table_index;
	const CorrelatedColumns &correlated_columns;
};

} // namespace sabot_sql
