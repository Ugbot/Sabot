//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/execution/operator/helper/physical_transaction.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/execution/physical_operator.hpp"
#include "sabot_sql/parser/parsed_data/transaction_info.hpp"

namespace sabot_sql {

//! PhysicalTransaction represents a transaction operator (e.g. BEGIN or COMMIT)
class PhysicalTransaction : public PhysicalOperator {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::TRANSACTION;

public:
	explicit PhysicalTransaction(PhysicalPlan &physical_plan, unique_ptr<TransactionInfo> info,
	                             idx_t estimated_cardinality)
	    : PhysicalOperator(physical_plan, PhysicalOperatorType::TRANSACTION, {LogicalType::BOOLEAN},
	                       estimated_cardinality),
	      info(std::move(info)) {
	}

	unique_ptr<TransactionInfo> info;

public:
	SourceResultType GetData(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &input) const override;

	bool IsSource() const override {
		return true;
	}
};

} // namespace sabot_sql
