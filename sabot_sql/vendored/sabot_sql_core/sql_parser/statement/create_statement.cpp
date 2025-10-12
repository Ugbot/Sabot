#include "sabot_sql/parser/statement/create_statement.hpp"

namespace sabot_sql {

CreateStatement::CreateStatement() : SQLStatement(StatementType::CREATE_STATEMENT) {
}

CreateStatement::CreateStatement(const CreateStatement &other) : SQLStatement(other), info(other.info->Copy()) {
}

unique_ptr<SQLStatement> CreateStatement::Copy() const {
	return unique_ptr<CreateStatement>(new CreateStatement(*this));
}

string CreateStatement::ToString() const {
	return info->ToString();
}

} // namespace sabot_sql
