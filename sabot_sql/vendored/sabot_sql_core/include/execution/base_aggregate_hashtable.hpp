//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/execution/base_aggregate_hashtable.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/common/common.hpp"
#include "sabot_sql/common/types/row/tuple_data_layout.hpp"
#include "sabot_sql/common/types/vector.hpp"
#include "sabot_sql/execution/operator/aggregate/aggregate_object.hpp"

namespace sabot_sql {
class BufferManager;

class BaseAggregateHashTable {
public:
	BaseAggregateHashTable(ClientContext &context, Allocator &allocator, const vector<AggregateObject> &aggregates,
	                       vector<LogicalType> payload_types);
	virtual ~BaseAggregateHashTable() {
	}

protected:
	Allocator &allocator;
	BufferManager &buffer_manager;
	//! A helper for managing offsets into the data buffers
	shared_ptr<TupleDataLayout> layout_ptr;
	//! The types of the payload columns stored in the hashtable
	vector<LogicalType> payload_types;
	//! Intermediate structures and data for aggregate filters
	AggregateFilterDataSet filter_set;
};

} // namespace sabot_sql
