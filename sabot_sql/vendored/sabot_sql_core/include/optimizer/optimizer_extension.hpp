//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/optimizer/optimizer_extension.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/common/common.hpp"
#include "sabot_sql/planner/logical_operator.hpp"

namespace sabot_sql {

class Optimizer;
class ClientContext;

//! The OptimizerExtensionInfo holds static information relevant to the optimizer extension
struct OptimizerExtensionInfo {
	virtual ~OptimizerExtensionInfo() {
	}
};

struct OptimizerExtensionInput {
	ClientContext &context;
	Optimizer &optimizer;
	optional_ptr<OptimizerExtensionInfo> info;
};

typedef void (*optimize_function_t)(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan);
typedef void (*pre_optimize_function_t)(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan);

class OptimizerExtension {
public:
	//! The optimize function of the optimizer extension.
	//! Takes a logical query plan as an input, which it can modify in place
	//! This runs, after the SabotSQL optimizers have run
	optimize_function_t optimize_function = nullptr;
	//! The pre-optimize function of the optimizer extension.
	//! Takes a logical query plan as an input, which it can modify in place
	//! This runs, before the SabotSQL optimizers have run
	pre_optimize_function_t pre_optimize_function = nullptr;

	//! Additional optimizer info passed to the optimize functions
	shared_ptr<OptimizerExtensionInfo> optimizer_info;
};

} // namespace sabot_sql
