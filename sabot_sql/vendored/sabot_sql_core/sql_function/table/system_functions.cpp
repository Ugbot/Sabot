#include "sabot_sql/function/table/system_functions.hpp"
#include "sabot_sql/parser/parsed_data/create_view_info.hpp"
#include "sabot_sql/parser/query_node/select_node.hpp"
#include "sabot_sql/parser/expression/star_expression.hpp"
#include "sabot_sql/parser/tableref/table_function_ref.hpp"
#include "sabot_sql/parser/expression/function_expression.hpp"
#include "sabot_sql/catalog/catalog.hpp"

namespace sabot_sql {

void BuiltinFunctions::RegisterSQLiteFunctions() {
	PragmaVersion::RegisterFunction(*this);
	PragmaPlatform::RegisterFunction(*this);
	PragmaCollations::RegisterFunction(*this);
	PragmaTableInfo::RegisterFunction(*this);
	PragmaStorageInfo::RegisterFunction(*this);
	PragmaMetadataInfo::RegisterFunction(*this);
	PragmaDatabaseSize::RegisterFunction(*this);
	PragmaUserAgent::RegisterFunction(*this);

	SabotSQLConnectionCountFun::RegisterFunction(*this);
	SabotSQLApproxDatabaseCountFun::RegisterFunction(*this);
	SabotSQLColumnsFun::RegisterFunction(*this);
	SabotSQLConstraintsFun::RegisterFunction(*this);
	SabotSQLDatabasesFun::RegisterFunction(*this);
	SabotSQLFunctionsFun::RegisterFunction(*this);
	SabotSQLKeywordsFun::RegisterFunction(*this);
	SabotSQLPreparedStatementsFun::RegisterFunction(*this);
	SabotSQLLogFun::RegisterFunction(*this);
	SabotSQLLogContextFun::RegisterFunction(*this);
	SabotSQLIndexesFun::RegisterFunction(*this);
	SabotSQLSchemasFun::RegisterFunction(*this);
	SabotSQLDependenciesFun::RegisterFunction(*this);
	SabotSQLExtensionsFun::RegisterFunction(*this);
	SabotSQLMemoryFun::RegisterFunction(*this);
	SabotSQLExternalFileCacheFun::RegisterFunction(*this);
	SabotSQLOptimizersFun::RegisterFunction(*this);
	SabotSQLSecretsFun::RegisterFunction(*this);
	SabotSQLWhichSecretFun::RegisterFunction(*this);
	SabotSQLSecretTypesFun::RegisterFunction(*this);
	SabotSQLSequencesFun::RegisterFunction(*this);
	SabotSQLSettingsFun::RegisterFunction(*this);
	SabotSQLTablesFun::RegisterFunction(*this);
	SabotSQLTableSample::RegisterFunction(*this);
	SabotSQLTemporaryFilesFun::RegisterFunction(*this);
	SabotSQLTypesFun::RegisterFunction(*this);
	SabotSQLVariablesFun::RegisterFunction(*this);
	SabotSQLViewsFun::RegisterFunction(*this);
	EnableLoggingFun::RegisterFunction(*this);
	TestAllTypesFun::RegisterFunction(*this);
	TestVectorTypesFun::RegisterFunction(*this);
}

} // namespace sabot_sql
