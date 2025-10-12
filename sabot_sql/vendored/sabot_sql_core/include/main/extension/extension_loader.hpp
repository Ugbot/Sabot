//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/main/extension/extension_loader.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/common/constants.hpp"
#include "sabot_sql/function/cast/cast_function_set.hpp"
#include "sabot_sql/function/function_set.hpp"
#include "sabot_sql/main/secret/secret.hpp"
#include "sabot_sql/parser/parsed_data/create_type_info.hpp"
#include "sabot_sql/main/extension_install_info.hpp"
#include "sabot_sql/main/extension_manager.hpp"

namespace sabot_sql {

class DatabaseInstance;
struct CreateMacroInfo;
struct CreateCollationInfo;
struct CreateAggregateFunctionInfo;
struct CreateScalarFunctionInfo;
struct CreateTableFunctionInfo;

class ExtensionLoader {
	friend class SabotSQL;
	friend class ExtensionHelper;

public:
	explicit ExtensionLoader(ExtensionActiveLoad &load_info);
	ExtensionLoader(DatabaseInstance &db, const string &extension_name);

	//! Returns the DatabaseInstance associated with this extension loader
	SABOT_SQL_API DatabaseInstance &GetDatabaseInstance();

public:
	//! Set the description of the extension
	SABOT_SQL_API void SetDescription(const string &description);

public:
	//! Register a new scalar function - merge overloads if the function already exists
	SABOT_SQL_API void RegisterFunction(ScalarFunction function);
	SABOT_SQL_API void RegisterFunction(ScalarFunctionSet function);
	SABOT_SQL_API void RegisterFunction(CreateScalarFunctionInfo info);

	//! Register a new aggregate function - merge overloads if the function already exists
	SABOT_SQL_API void RegisterFunction(AggregateFunction function);
	SABOT_SQL_API void RegisterFunction(AggregateFunctionSet function);
	SABOT_SQL_API void RegisterFunction(CreateAggregateFunctionInfo info);

	//! Register a new table function - merge overloads if the function already exists
	SABOT_SQL_API void RegisterFunction(TableFunction function);
	SABOT_SQL_API void RegisterFunction(TableFunctionSet function);
	SABOT_SQL_API void RegisterFunction(CreateTableFunctionInfo info);

	//! Register a new pragma function - throw an exception if the function already exists
	SABOT_SQL_API void RegisterFunction(PragmaFunction function);

	//! Register a new pragma function set - throw an exception if the function already exists
	SABOT_SQL_API void RegisterFunction(PragmaFunctionSet function);

	//! Register a CreateSecretFunction
	SABOT_SQL_API void RegisterFunction(CreateSecretFunction function);

	//! Register a new copy function - throw an exception if the function already exists
	SABOT_SQL_API void RegisterFunction(CopyFunction function);
	//! Register a new macro function - throw an exception if the function already exists
	SABOT_SQL_API void RegisterFunction(CreateMacroInfo &info);

	//! Register a new collation
	SABOT_SQL_API void RegisterCollation(CreateCollationInfo &info);

	//! Returns a reference to the function in the catalog - throws an exception if it does not exist
	SABOT_SQL_API ScalarFunctionCatalogEntry &GetFunction(const string &name);
	SABOT_SQL_API TableFunctionCatalogEntry &GetTableFunction(const string &name);
	SABOT_SQL_API optional_ptr<CatalogEntry> TryGetFunction(const string &name);
	SABOT_SQL_API optional_ptr<CatalogEntry> TryGetTableFunction(const string &name);

	//! Add a function overload
	SABOT_SQL_API void AddFunctionOverload(ScalarFunction function);
	SABOT_SQL_API void AddFunctionOverload(ScalarFunctionSet function);
	SABOT_SQL_API void AddFunctionOverload(TableFunctionSet function);

	//! Registers a new type
	SABOT_SQL_API void RegisterType(string type_name, LogicalType type,
	                             bind_logical_type_function_t bind_function = nullptr);

	//! Registers a new secret type
	SABOT_SQL_API void RegisterSecretType(SecretType secret_type);

	//! Registers a cast between two types
	SABOT_SQL_API void RegisterCastFunction(const LogicalType &source, const LogicalType &target,
	                                     bind_cast_function_t function, int64_t implicit_cast_cost = -1);
	SABOT_SQL_API void RegisterCastFunction(const LogicalType &source, const LogicalType &target, BoundCastInfo function,
	                                     int64_t implicit_cast_cost = -1);

private:
	void FinalizeLoad();

private:
	DatabaseInstance &db;
	string extension_name;
	string extension_description;
	optional_ptr<ExtensionInfo> extension_info;
};

} // namespace sabot_sql

//! Helper macro to define the entrypoint for a C++ extension
//! Usage:
//!
//!		SABOT_SQL_CPP_EXTENSION_ENTRY(my_extension, loader) {
//!			loader.RegisterFunction(...);
//!		}
//!
#define SABOT_SQL_CPP_EXTENSION_ENTRY(EXTENSION_NAME, LOADER_NAME)                                                        \
	SABOT_SQL_EXTENSION_API void EXTENSION_NAME##_sabot_sql_cpp_init(sabot_sql::ExtensionLoader &LOADER_NAME)
