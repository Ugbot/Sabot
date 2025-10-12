#include "sabot_sql/parser/statement/detach_statement.hpp"

namespace sabot_sql {

DetachStatement::DetachStatement() : SQLStatement(StatementType::DETACH_STATEMENT) {
}

DetachStatement::DetachStatement(const DetachStatement &other) : SQLStatement(other), info(other.info->Copy()) {
}

unique_ptr<SQLStatement> DetachStatement::Copy() const {
	return unique_ptr<DetachStatement>(new DetachStatement(*this));
}

string DetachStatement::ToString() const {
	return info->ToString();
}

} // namespace sabot_sql
