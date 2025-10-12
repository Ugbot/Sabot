#include "sabot_sql/parser/constraint.hpp"

#include "sabot_sql/common/printer.hpp"
#include "sabot_sql/parser/constraints/list.hpp"

namespace sabot_sql {

Constraint::Constraint(ConstraintType type) : type(type) {
}

Constraint::~Constraint() {
}

void Constraint::Print() const {
	Printer::Print(ToString());
}

} // namespace sabot_sql
