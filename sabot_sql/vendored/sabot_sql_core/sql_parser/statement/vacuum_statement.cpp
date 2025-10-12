#include "sabot_sql/parser/statement/vacuum_statement.hpp"

namespace sabot_sql {

VacuumStatement::VacuumStatement(const VacuumOptions &options)
    : SQLStatement(StatementType::VACUUM_STATEMENT), info(make_uniq<VacuumInfo>(options)) {
}

VacuumStatement::VacuumStatement(const VacuumStatement &other) : SQLStatement(other), info(other.info->Copy()) {
}

unique_ptr<SQLStatement> VacuumStatement::Copy() const {
	return unique_ptr<VacuumStatement>(new VacuumStatement(*this));
}

string VacuumStatement::ToString() const {
	return info->ToString();
}

} // namespace sabot_sql
