#include "sabot_sql/parser/statement/copy_statement.hpp"

namespace sabot_sql {

CopyStatement::CopyStatement() : SQLStatement(StatementType::COPY_STATEMENT), info(make_uniq<CopyInfo>()) {
}

CopyStatement::CopyStatement(const CopyStatement &other) : SQLStatement(other), info(other.info->Copy()) {
}

string CopyStatement::ToString() const {
	return info->ToString();
}

unique_ptr<SQLStatement> CopyStatement::Copy() const {
	return unique_ptr<CopyStatement>(new CopyStatement(*this));
}

} // namespace sabot_sql
