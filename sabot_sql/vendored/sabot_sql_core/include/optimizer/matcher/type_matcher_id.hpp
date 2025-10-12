//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/optimizer/matcher/type_matcher_id.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/common/types.hpp"
#include "sabot_sql/optimizer/matcher/type_matcher.hpp"
namespace sabot_sql {

//! The TypeMatcherId class contains a set of matchers that can be used to pattern match TypeIds for Rules
class TypeMatcherId : public TypeMatcher {
public:
	explicit TypeMatcherId(LogicalTypeId type_id_p) : type_id(type_id_p) {
	}

	bool Match(const LogicalType &type) override {
		return type.id() == this->type_id;
	}

private:
	LogicalTypeId type_id;
};

} // namespace sabot_sql
