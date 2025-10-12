#include "sabot_sql/parser/statement/pragma_statement.hpp"

namespace sabot_sql {

PragmaStatement::PragmaStatement() : SQLStatement(StatementType::PRAGMA_STATEMENT), info(make_uniq<PragmaInfo>()) {
}

PragmaStatement::PragmaStatement(const PragmaStatement &other) : SQLStatement(other), info(other.info->Copy()) {
}

unique_ptr<SQLStatement> PragmaStatement::Copy() const {
	return unique_ptr<PragmaStatement>(new PragmaStatement(*this));
}

string PragmaStatement::ToString() const {
	return info->ToString();
}

} // namespace sabot_sql
