//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/function/function.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/common/named_parameter_map.hpp"
#include "sabot_sql/common/types/data_chunk.hpp"
#include "sabot_sql/common/unordered_set.hpp"
#include "sabot_sql/main/external_dependencies.hpp"
#include "sabot_sql/parser/column_definition.hpp"
#include "sabot_sql/common/enums/function_errors.hpp"

namespace sabot_sql {
class CatalogEntry;
class Catalog;
class ClientContext;
class Expression;
class ExpressionExecutor;
class Transaction;

class AggregateFunction;
class AggregateFunctionSet;
class CopyFunction;
class PragmaFunction;
class PragmaFunctionSet;
class ScalarFunctionSet;
class ScalarFunction;
class TableFunctionSet;
class TableFunction;
class SimpleFunction;

struct PragmaInfo;

//! The default null handling is NULL in, NULL out
enum class FunctionNullHandling : uint8_t { DEFAULT_NULL_HANDLING = 0, SPECIAL_HANDLING = 1 };
//! The stability of the function, used by the optimizer
//! CONSISTENT              -> this function always returns the same result when given the same input, no variance
//! CONSISTENT_WITHIN_QUERY -> this function returns the same result WITHIN the same query/transaction
//!                            but the result might change across queries (e.g. NOW(), CURRENT_TIME)
//! VOLATILE                -> the result of this function might change per row (e.g. RANDOM())
enum class FunctionStability : uint8_t { CONSISTENT = 0, VOLATILE = 1, CONSISTENT_WITHIN_QUERY = 2 };

//! How to handle collations
//! PROPAGATE_COLLATIONS        -> this function combines collation from its inputs and emits them again (default)
//! PUSH_COMBINABLE_COLLATIONS  -> combinable collations are executed for the input arguments
//! IGNORE_COLLATIONS           -> collations are completely ignored by the function
enum class FunctionCollationHandling : uint8_t {
	PROPAGATE_COLLATIONS = 0,
	PUSH_COMBINABLE_COLLATIONS = 1,
	IGNORE_COLLATIONS = 2
};

struct FunctionData {
	SABOT_SQL_API virtual ~FunctionData();

	SABOT_SQL_API virtual unique_ptr<FunctionData> Copy() const = 0;
	SABOT_SQL_API virtual bool Equals(const FunctionData &other) const = 0;
	SABOT_SQL_API static bool Equals(const FunctionData *left, const FunctionData *right);
	SABOT_SQL_API virtual bool SupportStatementCache() const;

	template <class TARGET>
	TARGET &Cast() {
		DynamicCastCheck<TARGET>(this);
		return reinterpret_cast<TARGET &>(*this);
	}
	template <class TARGET>
	const TARGET &Cast() const {
		DynamicCastCheck<TARGET>(this);
		return reinterpret_cast<const TARGET &>(*this);
	}
	// FIXME: this function should be removed in the future
	template <class TARGET>
	TARGET &CastNoConst() const {
		return const_cast<TARGET &>(Cast<TARGET>()); // NOLINT: FIXME
	}
};

struct TableFunctionData : public FunctionData {
	// used to pass on projections to table functions that support them. NB, can contain COLUMN_IDENTIFIER_ROW_ID
	vector<idx_t> column_ids;

	SABOT_SQL_API ~TableFunctionData() override;

	SABOT_SQL_API unique_ptr<FunctionData> Copy() const override;
	SABOT_SQL_API bool Equals(const FunctionData &other) const override;
};

struct FunctionParameters {
	vector<Value> values;
	named_parameter_map_t named_parameters;
};

//! Function is the base class used for any type of function (scalar, aggregate or simple function)
class Function {
public:
	SABOT_SQL_API explicit Function(string name);
	SABOT_SQL_API virtual ~Function();

	//! The name of the function
	string name;
	//! Additional Information to specify function from it's name
	string extra_info;

	// Optional catalog name of the function
	string catalog_name;

	// Optional schema name of the function
	string schema_name;

public:
	//! Returns the formatted string name(arg1, arg2, ...)
	SABOT_SQL_API static string CallToString(const string &catalog_name, const string &schema_name, const string &name,
	                                      const vector<LogicalType> &arguments,
	                                      const LogicalType &varargs = LogicalType::INVALID);
	//! Returns the formatted string name(arg1, arg2..) -> return_type
	SABOT_SQL_API static string CallToString(const string &catalog_name, const string &schema_name, const string &name,
	                                      const vector<LogicalType> &arguments, const LogicalType &varargs,
	                                      const LogicalType &return_type);
	//! Returns the formatted string name(arg1, arg2.., np1=a, np2=b, ...)
	SABOT_SQL_API static string CallToString(const string &catalog_name, const string &schema_name, const string &name,
	                                      const vector<LogicalType> &arguments,
	                                      const named_parameter_type_map_t &named_parameters);

	//! Used in the bind to erase an argument from a function
	SABOT_SQL_API static void EraseArgument(SimpleFunction &bound_function, vector<unique_ptr<Expression>> &arguments,
	                                     idx_t argument_index);
};

class SimpleFunction : public Function {
public:
	SABOT_SQL_API SimpleFunction(string name, vector<LogicalType> arguments,
	                          LogicalType varargs = LogicalType(LogicalTypeId::INVALID));
	SABOT_SQL_API ~SimpleFunction() override;

	//! The set of arguments of the function
	vector<LogicalType> arguments;
	//! The set of original arguments of the function - only set if Function::EraseArgument is called
	//! Used for (de)serialization purposes
	vector<LogicalType> original_arguments;
	//! The type of varargs to support, or LogicalTypeId::INVALID if the function does not accept variable length
	//! arguments
	LogicalType varargs;

public:
	SABOT_SQL_API virtual string ToString() const;

	SABOT_SQL_API bool HasVarArgs() const;
};

class SimpleNamedParameterFunction : public SimpleFunction {
public:
	SABOT_SQL_API SimpleNamedParameterFunction(string name, vector<LogicalType> arguments,
	                                        LogicalType varargs = LogicalType(LogicalTypeId::INVALID));
	SABOT_SQL_API ~SimpleNamedParameterFunction() override;

	//! The named parameters of the function
	named_parameter_type_map_t named_parameters;

public:
	SABOT_SQL_API string ToString() const override;
	SABOT_SQL_API bool HasNamedParameters() const;
};

class BaseScalarFunction : public SimpleFunction {
public:
	SABOT_SQL_API BaseScalarFunction(string name, vector<LogicalType> arguments, LogicalType return_type,
	                              FunctionStability stability,
	                              LogicalType varargs = LogicalType(LogicalTypeId::INVALID),
	                              FunctionNullHandling null_handling = FunctionNullHandling::DEFAULT_NULL_HANDLING,
	                              FunctionErrors errors = FunctionErrors::CANNOT_ERROR);
	SABOT_SQL_API ~BaseScalarFunction() override;

	//! Return type of the function
	LogicalType return_type;
	//! The stability of the function (see FunctionStability enum for more info)
	FunctionStability stability;
	//! How this function handles NULL values
	FunctionNullHandling null_handling;
	//! Whether or not this function can throw an error
	FunctionErrors errors;
	//! Collation handling of the function
	FunctionCollationHandling collation_handling;

	static BaseScalarFunction SetReturnsError(BaseScalarFunction &function) {
		function.errors = FunctionErrors::CAN_THROW_RUNTIME_ERROR;
		return function;
	}

public:
	SABOT_SQL_API hash_t Hash() const;

	SABOT_SQL_API string ToString() const override;
};

} // namespace sabot_sql
