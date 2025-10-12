//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/planner/operator_extension.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/common/common.hpp"
#include "sabot_sql/execution/physical_plan_generator.hpp"
#include "sabot_sql/planner/binder.hpp"

namespace sabot_sql {

//! The OperatorExtensionInfo holds static information relevant to the operator extension
struct OperatorExtensionInfo {
	virtual ~OperatorExtensionInfo() {
	}
};

typedef BoundStatement (*bind_function_t)(ClientContext &context, Binder &binder, OperatorExtensionInfo *info,
                                          SQLStatement &statement);

// forward declaration to avoid circular reference
struct LogicalExtensionOperator;

class OperatorExtension {
public:
	bind_function_t Bind; // NOLINT: backwards compatibility

	//! Additional info passed to the CreatePlan & Bind functions
	shared_ptr<OperatorExtensionInfo> operator_info;

	virtual std::string GetName() = 0;
	virtual unique_ptr<LogicalExtensionOperator> Deserialize(Deserializer &deserializer) = 0;

	virtual ~OperatorExtension() {
	}
};

} // namespace sabot_sql
