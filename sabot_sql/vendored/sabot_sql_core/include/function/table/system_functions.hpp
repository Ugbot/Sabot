//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/function/table/system_functions.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/function/table_function.hpp"
#include "sabot_sql/function/built_in_functions.hpp"

namespace sabot_sql {

struct PragmaCollations {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct PragmaTableInfo {
	static void GetColumnInfo(TableCatalogEntry &table, const ColumnDefinition &column, DataChunk &output, idx_t index);

	static void RegisterFunction(BuiltinFunctions &set);
};

struct PragmaStorageInfo {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct PragmaMetadataInfo {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct PragmaVersion {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct PragmaPlatform {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct PragmaDatabaseSize {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct SabotSQLSchemasFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct SabotSQLConnectionCountFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct SabotSQLApproxDatabaseCountFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct SabotSQLColumnsFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct SabotSQLConstraintsFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct SabotSQLSecretsFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct SabotSQLWhichSecretFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct SabotSQLDatabasesFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct SabotSQLDependenciesFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct SabotSQLExtensionsFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct SabotSQLPreparedStatementsFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct SabotSQLFunctionsFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct SabotSQLKeywordsFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct SabotSQLLogFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct SabotSQLLogContextFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct SabotSQLIndexesFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct SabotSQLMemoryFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct SabotSQLExternalFileCacheFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct SabotSQLOptimizersFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct SabotSQLSecretTypesFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct SabotSQLSequencesFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct SabotSQLSettingsFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct SabotSQLTablesFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct SabotSQLTableSample {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct SabotSQLTemporaryFilesFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct SabotSQLTypesFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct SabotSQLVariablesFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct SabotSQLViewsFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct EnableLoggingFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct TestType {
	TestType(LogicalType type_p, string name_p)
	    : type(std::move(type_p)), name(std::move(name_p)), min_value(Value::MinimumValue(type)),
	      max_value(Value::MaximumValue(type)) {
	}
	TestType(LogicalType type_p, string name_p, Value min, Value max)
	    : type(std::move(type_p)), name(std::move(name_p)), min_value(std::move(min)), max_value(std::move(max)) {
	}

	LogicalType type;
	string name;
	Value min_value;
	Value max_value;
};

struct TestAllTypesFun {
	static void RegisterFunction(BuiltinFunctions &set);
	static vector<TestType> GetTestTypes(bool large_enum = false, bool large_bignum = false);
};

struct TestVectorTypesFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct PragmaUserAgent {
	static void RegisterFunction(BuiltinFunctions &set);
};

} // namespace sabot_sql
