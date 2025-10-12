//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/function/scalar/sequence_functions.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/catalog/catalog_entry/sequence_catalog_entry.hpp"
#include "sabot_sql/function/scalar_function.hpp"
#include "sabot_sql/function/function_set.hpp"
#include "sabot_sql/function/built_in_functions.hpp"

namespace sabot_sql {

struct NextvalBindData : public FunctionData {
	explicit NextvalBindData(SequenceCatalogEntry &sequence) : sequence(sequence), create_info(sequence.GetInfo()) {
	}

	//! The sequence to use for the nextval computation; only if the sequence is a constant
	SequenceCatalogEntry &sequence;

	//! The CreateInfo for the above sequence, if it exists
	unique_ptr<CreateInfo> create_info;

	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<NextvalBindData>(sequence);
	}

	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<NextvalBindData>();
		return RefersToSameObject(sequence, other.sequence);
	}
};

} // namespace sabot_sql
