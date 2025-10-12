//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/execution/operator/schema/physical_create_schema.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/execution/physical_operator.hpp"
#include "sabot_sql/parser/parsed_data/create_schema_info.hpp"

namespace sabot_sql {

//! PhysicalCreateSchema represents a CREATE SCHEMA command
class PhysicalCreateSchema : public PhysicalOperator {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::CREATE_SCHEMA;

public:
	explicit PhysicalCreateSchema(PhysicalPlan &physical_plan, unique_ptr<CreateSchemaInfo> info,
	                              idx_t estimated_cardinality)
	    : PhysicalOperator(physical_plan, PhysicalOperatorType::CREATE_SCHEMA, {LogicalType::BIGINT},
	                       estimated_cardinality),
	      info(std::move(info)) {
	}

	unique_ptr<CreateSchemaInfo> info;

public:
	// Source interface
	SourceResultType GetData(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &input) const override;

	bool IsSource() const override {
		return true;
	}
};

} // namespace sabot_sql
