//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/planner/operator/logical_cross_product.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/planner/operator/logical_unconditional_join.hpp"

namespace sabot_sql {

//! LogicalCrossProduct represents a cross product between two relations
class LogicalCrossProduct : public LogicalUnconditionalJoin {
	LogicalCrossProduct() : LogicalUnconditionalJoin(LogicalOperatorType::LOGICAL_CROSS_PRODUCT) {};

public:
	static constexpr const LogicalOperatorType TYPE = LogicalOperatorType::LOGICAL_CROSS_PRODUCT;

public:
	LogicalCrossProduct(unique_ptr<LogicalOperator> left, unique_ptr<LogicalOperator> right);

public:
	static unique_ptr<LogicalOperator> Create(unique_ptr<LogicalOperator> left, unique_ptr<LogicalOperator> right);

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<LogicalOperator> Deserialize(Deserializer &deserializer);
};
} // namespace sabot_sql
