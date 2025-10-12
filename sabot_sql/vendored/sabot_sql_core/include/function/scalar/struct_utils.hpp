//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/function/scalar/struct_utils.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/function/scalar_function.hpp"
#include "sabot_sql/function/function_set.hpp"
#include "sabot_sql/function/built_in_functions.hpp"

namespace sabot_sql {

struct StructExtractBindData : public FunctionData {
	explicit StructExtractBindData(idx_t index) : index(index) {
	}

	idx_t index;

public:
	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<StructExtractBindData>(index);
	}
	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<StructExtractBindData>();
		return index == other.index;
	}
};

} // namespace sabot_sql
