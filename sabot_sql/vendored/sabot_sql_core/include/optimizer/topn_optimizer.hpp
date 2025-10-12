//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/optimizer/topn_optimizer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/main/client_context.hpp"
#include "sabot_sql/common/constants.hpp"

namespace sabot_sql {
class LogicalOperator;
class LogicalTopN;
class Optimizer;

class TopN {
public:
	explicit TopN(ClientContext &context);

	//! Optimize ORDER BY + LIMIT to TopN
	unique_ptr<LogicalOperator> Optimize(unique_ptr<LogicalOperator> op);
	//! Whether we can perform the optimization on this operator
	static bool CanOptimize(LogicalOperator &op, optional_ptr<ClientContext> context = nullptr);

private:
	void PushdownDynamicFilters(LogicalTopN &op);

private:
	ClientContext &context;
};

} // namespace sabot_sql
