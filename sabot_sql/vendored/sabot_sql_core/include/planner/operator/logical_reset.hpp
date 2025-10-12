//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/planner/operator/logical_reset.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/common/enums/set_scope.hpp"
#include "sabot_sql/parser/parsed_data/copy_info.hpp"
#include "sabot_sql/planner/logical_operator.hpp"
#include "sabot_sql/function/copy_function.hpp"

namespace sabot_sql {

class LogicalReset : public LogicalOperator {
public:
	static constexpr const LogicalOperatorType TYPE = LogicalOperatorType::LOGICAL_RESET;

public:
	LogicalReset(std::string name_p, SetScope scope_p)
	    : LogicalOperator(LogicalOperatorType::LOGICAL_RESET), name(std::move(name_p)), scope(scope_p) {
	}

	std::string name;
	SetScope scope;

public:
	void Serialize(Serializer &serializer) const override;
	static unique_ptr<LogicalOperator> Deserialize(Deserializer &deserializer);

	idx_t EstimateCardinality(ClientContext &context) override;

protected:
	void ResolveTypes() override {
		types.emplace_back(LogicalType::BOOLEAN);
	}
};

} // namespace sabot_sql
