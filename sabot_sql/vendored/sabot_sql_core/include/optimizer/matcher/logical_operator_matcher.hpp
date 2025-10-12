//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/optimizer/matcher/logical_operator_matcher.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/common/enums/logical_operator_type.hpp"

namespace sabot_sql {

//! The LogicalOperatorMatcher class contains a set of matchers that can be used to match LogicalOperators
class LogicalOperatorMatcher {
public:
	virtual ~LogicalOperatorMatcher() {
	}

	virtual bool Match(LogicalOperatorType type) = 0;
};

//! The SpecificLogicalTypeMatcher class matches only a single specified LogicalOperatorType
class SpecificLogicalTypeMatcher : public LogicalOperatorMatcher {
public:
	explicit SpecificLogicalTypeMatcher(LogicalOperatorType type) : type(type) {
	}

	bool Match(LogicalOperatorType type) override {
		return type == this->type;
	}

private:
	LogicalOperatorType type;
};

} // namespace sabot_sql
