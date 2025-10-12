#include "sabot_sql/parser/base_expression.hpp"

#include "sabot_sql/main/config.hpp"
#include "sabot_sql/common/printer.hpp"

namespace sabot_sql {

void BaseExpression::Print() const {
	Printer::Print(ToString());
}

string BaseExpression::GetName() const {
#ifdef DEBUG
	if (DBConfigOptions::debug_print_bindings) {
		return ToString();
	}
#endif
	return !alias.empty() ? alias : ToString();
}

bool BaseExpression::Equals(const BaseExpression &other) const {
	if (expression_class != other.expression_class || type != other.type) {
		return false;
	}
	return true;
}

void BaseExpression::Verify() const {
}

} // namespace sabot_sql
